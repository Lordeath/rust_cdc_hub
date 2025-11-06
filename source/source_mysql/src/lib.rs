use async_trait::async_trait;
use common::{retry_3_async, CdcConfig, DataBuffer, Operation, Sink, Source, Value};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::column::json::json_binary::JsonBinary;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

pub struct MySQLSource {
    // config: SourceConfig,
    connection_url: String,
    server_id: u64,
    // start_position: StartPosition,
    table_name: String,
}

impl MySQLSource {
    pub fn new(config: CdcConfig) -> Self {
        // let config = self.config.clone();
        // 使用map 拼接成 connection_url
        let username = config.get("username");
        let password = config.get("password");
        let host = config.get("host");
        let port = config.get("port");
        let database = config.get("database");
        let connection_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            username, password, host, port, database
        );

        println!("Connecting to MySQL binlog at {}", connection_url.clone());

        let server_id: u64 = config.get_u64("server_id");
        // let binlog_filename = config.get("binlog_filename");
        // let binlog_position: u32 = config.get_u32("binlog_position");
        // let gtid_set = config.get("gtid_set");

        // let start_position = StartPosition::Latest;
        let table_name = config.get("table_name");

        Self {
            connection_url,
            server_id,
            table_name,
        }
    }
}

#[async_trait]
impl Source for MySQLSource {
    async fn start(
        &self,
        mut sink: Arc<tokio::sync::Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("Starting MySQL binlog source");
        let mut stream = BinlogClient::new(
            self.connection_url.as_str(),
            self.server_id.clone(),
            StartPosition::Latest,
        )
            // / Heartbeat interval in seconds
            // / Server will send a heartbeat event if no binlog events are received within this interval
            // / If heartbeat_interval_secs=0, server won't send heartbeat events
            // / default is 0 means not enabled
            .with_master_heartbeat(Duration::from_secs(5))
            // / Network operation timeout in seconds
            // / Maximum wait time for operations like connection establishment and data reading
            // / default is 60 secs
            .with_read_timeout(Duration::from_secs(60))
            // / TCP keepalive idle time and keepalive interval time
            // / default is (0 secs, 0 secs) means keepalive not enabled
            .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
            .connect()
            .await
            .unwrap();

        let mut columns: Vec<String> = vec![];
        // TODO 这里获取列名

        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        //
        loop {
            match stream.read().await {
                Ok((header, data)) => {
                    // let (header, data) = stream.read().await?;
                    // println!("header: {:?}", header);
                    // println!("data: {:?}", data);
                    // println!();
                    // 组装成 DataBuffer
                    // let timestamp = header.timestamp.to_string();

                    match data {
                        EventData::TableMap(event) => {
                            let table_name = event.table_name;
                            let table_id = event.table_id;
                            let database_name = event.database_name;
                            println!("TableMap: {:?} {:?}", table_name, table_id);
                            println!("TableMap.column_metas: {:?}", event.column_metas);
                            println!("TableMap.column_types: {:?}", event.column_types);
                            println!("TableMap.null_bits: {:?}", event.null_bits);
                            // let columns = event.table_metadata.unwrap().columns.into_iter().map(|m|m.column_name).collect::<Vec<_>>();
                            // println!("TableMap.table_metadata: {:?}", columns);
                            // let table_info = TableInfo{
                            //     database_name, table_name
                            // };
                            table_map.insert(table_id, table_name);
                            table_database_map.insert(table_id, database_name);
                        }

                        EventData::WriteRows(event) => {
                            let table_name = table_map.get(&event.table_id).unwrap().as_str();
                            let database_name = table_database_map.get(&event.table_id).unwrap().as_str();
                            println!("WriteRows: {}.{}", database_name, table_name);
                            if self.table_name.eq_ignore_ascii_case(table_name) {
                                for row in event.rows {
                                    let mut before: HashMap<String, Value> = HashMap::new();
                                    let mut after: HashMap<String, Value> = HashMap::new();
                                    parse_row(row, &mut after, &columns);
                                    let op = Operation::CREATE;
                                    let data_buffer = DataBuffer { before, after, op };
                                    sink.lock().await.write_record(data_buffer);
                                }
                            }
                        }
                        EventData::DeleteRows(event) => {
                            let table_name = table_map.get(&event.table_id).unwrap().as_str();
                            let database_name = table_database_map.get(&event.table_id).unwrap().as_str();
                            println!("DeleteRows: {}.{}", database_name, table_name);
                            if self.table_name.eq_ignore_ascii_case(table_name) {
                                for row in event.rows {
                                    let mut before: HashMap<String, Value> = HashMap::new();
                                    let mut after: HashMap<String, Value> = HashMap::new();
                                    parse_row(row, &mut before, &columns);
                                    let op = Operation::DELETE;
                                    let data_buffer = DataBuffer { before, after, op };
                                    sink.lock().await.write_record(data_buffer);
                                }
                            }
                        }
                        EventData::UpdateRows(event) => {
                            let table_name = table_map.get(&event.table_id).unwrap().as_str();
                            let database_name = table_database_map.get(&event.table_id).unwrap().as_str();
                            println!("UpdateRows: {}.{}", database_name, table_name);
                            if self.table_name.eq_ignore_ascii_case(table_name) {
                                for (b, a) in event.rows {
                                    let mut before: HashMap<String, Value> = HashMap::new();
                                    let mut after: HashMap<String, Value> = HashMap::new();
                                    parse_row(b, &mut before, &columns);
                                    parse_row(a, &mut after, &columns);
                                    let op = Operation::UPDATE;
                                    let data_buffer = DataBuffer { before, after, op };
                                    sink.lock().await.write_record(data_buffer);
                                    // retry_3_async(async || sink.lock().await.write_record(data_buffer));
                                }
                            }
                        }
                        _ => {}
                    }
                    sink.lock().await.flush();
                    // retry_3_async(async || sink.lock().await.flush());
                }
                Err(e) => {
                    // 打印错误信息，并且继续监听
                    println!("Error: {}", e);
                }
            }
            // tokio::time::sleep(Duration::from_secs(5)).await;

            // let data_buffer = common::DataBuffer::new(data);
        }
    }
}

fn parse_row(row: RowEvent, data: &mut HashMap<String, Value>, columns: &Vec<String>) {
    let mut index = 0;
    for column_value in row.column_values {
        // let column_name = columns[index].clone();
        if let ColumnValue::String(bytes) = column_value {
            println!("string column: {}", String::from_utf8_lossy(&bytes));
            let value: Value = Value::String(String::from_utf8_lossy(&bytes).to_string());
            println!("value: {:?}", value);
            // data.insert(column_name, value);
        } else if let ColumnValue::Json(bytes) = column_value {
            println!(
                "json column: {}",
                JsonBinary::parse_as_string(&bytes).unwrap()
            )
        }
        index = index + 1;
    }
}

struct TableInfo {
    database_name: String,
    table_name: String,
    // columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(1, 1);
    }
}
