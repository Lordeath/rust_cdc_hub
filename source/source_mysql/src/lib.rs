use async_trait::async_trait;
use common::{CdcConfig, DataBuffer, Operation, Sink, Source, Value};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use sqlx::{MySqlPool, Row};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct MySQLSource {
    // config: SourceConfig,
    connection_url: String,
    // server_id: u64,
    // start_position: StartPosition,
    database: String,
    table_name: String,
    // db:  DbConn,
    streams: Vec<BinlogStream>, // ✅ 多个流
}

impl MySQLSource {
    pub async fn new(config: CdcConfig) -> Self {
        // let config = self.config.clone();
        // 使用map 拼接成 connection_url
        let username = config.get("username");
        let password = config.get("password");
        let host = config.get("host");
        let port = config.get("port");
        let database = config.get("database");
        let connection_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            username,
            password,
            host,
            port,
            database.clone()
        );

        println!("Connecting to MySQL binlog at {}", connection_url.clone());

        let server_id: u64 = config.get_u64("server_id");
        let table_name = config.get("table_name");
        let mut streams = Vec::new();

        let client = BinlogClient::new(&connection_url, server_id, StartPosition::Latest)
            .with_master_heartbeat(Duration::from_secs(5))
            .with_read_timeout(Duration::from_secs(60))
            .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
            .connect()
            .await
            .unwrap();

        streams.push(client);

        Self {
            connection_url,
            database,
            table_name,
            streams,
        }
    }

    async fn write_record_with_retry(
        sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>,
        data_buffer: &DataBuffer,
    ) {
        let mut loop_count = 0;
        loop {
            let sink_result = sink.lock().await.write_record(&data_buffer).await;
            match sink_result {
                Ok(_) => {
                    break;
                }
                Err(_) => {}
            }
            loop_count += 1;
            if loop_count >= 3 {
                panic!("flush error");
            }
        }
    }

    async fn flush_with_retry(sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>) {
        let mut loop_count = 0;
        loop {
            let sink_result = sink.lock().await.flush().await;
            match sink_result {
                Ok(_) => {
                    break;
                }
                Err(_) => {}
            }
            loop_count += 1;
            if loop_count >= 3 {
                panic!("flush error");
            }
        }
        ()
    }

    fn is_target_database_and_table(&self, database_name: &str, table_name: &str) -> bool {
        self.database.eq_ignore_ascii_case(database_name)
            && self.table_name.eq_ignore_ascii_case(table_name)
    }

    async fn fill_table_column(&self) -> Vec<String> {
        let sql = r#"
            select COLUMN_NAME as column_name from information_schema.`COLUMNS` c
            where 1=1
            and c.TABLE_SCHEMA = ?
            and c.TABLE_NAME = ?
            order by c.ORDINAL_POSITION
        "#;

        let pool = MySqlPool::connect(&self.connection_url).await.unwrap();

        sqlx::query(sql)
            .bind(self.database.clone())
            .bind(self.table_name.clone())
            .fetch_all(&pool)
            // .execute(&mut *tx.acquire().await?)
            // .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Error executing query: {}", e))
            .into_iter()
            .map(|row| row.get("column_name"))
            .collect()
    }

    async fn parse_row(
        &self,
        row: RowEvent,
        // data: &mut HashMap<String, Value>,
        columns: &mut Mutex<Vec<String>>,
    ) -> HashMap<String, Value> {
        let mut data: HashMap<String, Value> = HashMap::new();
        let mut index = 0;
        if columns.lock().await.len() != row.column_values.len() {
            let columns_new = self.fill_table_column().await;
            columns.lock().await.clear();
            columns.lock().await.extend(columns_new);
        }
        if columns.lock().await.len() != row.column_values.len() {
            panic!("columns length not equal to column_values length");
        }

        for column_value in row.column_values {
            let column_name = columns.lock().await[index].clone();
            match column_value {
                ColumnValue::None => {
                    data.insert(column_name, Value::None);
                }
                ColumnValue::Tiny(v) => {
                    data.insert(column_name, Value::Int8(v));
                }
                ColumnValue::Short(v) => {
                    let value: Value = Value::Int16(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Long(v) => {
                    let value: Value = Value::Int32(v);
                    data.insert(column_name, value);
                }
                ColumnValue::LongLong(v) => {
                    let value: Value = Value::Int64(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Float(v) => {
                    let value: Value = Value::Float(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Double(v) => {
                    let value: Value = Value::Double(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Decimal(v) => {
                    let value: Value = Value::Decimal(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Time(v) => {
                    let value: Value = Value::Time(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Date(v) => {
                    let value: Value = Value::Date(v);
                    data.insert(column_name, value);
                }
                ColumnValue::DateTime(v) => {
                    let value: Value = Value::DateTime(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Timestamp(v) => {
                    let value: Value = Value::Timestamp(v);
                    data.insert(column_name, value);
                }
                ColumnValue::Year(v) => {
                    let value: Value = Value::Year(v);
                    data.insert(column_name, value);
                }
                ColumnValue::String(v) => {
                    let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
                    data.insert(column_name, value);
                }
                ColumnValue::Blob(v) => {
                    let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
                    data.insert(column_name, value);
                }
                ColumnValue::Bit(v) => {
                    let value: Value = Value::Bit(v);
                    data.insert(column_name, value);
                }
                // ColumnValue::Set(v) => { data.insert(column_name, Value::Int8(v))}
                // ColumnValue::Enum(v) => { data.insert(column_name, Value::Int8(v))}
                ColumnValue::Json(v) => {
                    let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
                    data.insert(column_name, value);
                }
                _ => {
                    columns
                        .lock()
                        .await
                        .iter()
                        .for_each(|column_name| println!("column: {}", column_name));
                    println!("column_name: {:?}", column_name);
                    println!("column_value: {:?}", column_value);
                    panic!("unsupported column value type")
                }
            }

            index = index + 1;
        }
        data
    }

    async fn push_data(&mut self, mut sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>, mut columns: &mut Mutex<Vec<String>>, table_map: &mut HashMap<u64, String>, table_database_map: &mut HashMap<u64, String>, data: EventData) {
        match data {
            EventData::TableMap(event) => {
                let table_name = event.table_name;
                let table_id = event.table_id;
                let database_name = event.database_name;
                table_map.insert(table_id, table_name);
                table_database_map.insert(table_id, database_name);
            }
            EventData::WriteRows(event) => {
                let table_name = table_map.get(&event.table_id).unwrap().as_str();
                let database_name =
                    table_database_map.get(&event.table_id).unwrap().as_str();
                if self.is_target_database_and_table(database_name, table_name) {
                    println!("WriteRows: {}.{}", database_name, table_name);
                    for row in event.rows {
                        let before: HashMap<String, Value> = HashMap::new();
                        let after: HashMap<String, Value> =
                            self.parse_row(row, &mut columns).await;
                        let op = Operation::CREATE;
                        let data_buffer = DataBuffer { before, after, op };
                        // sink.lock().await.write_record(&data_buffer);
                        Self::write_record_with_retry(&mut sink, &data_buffer)
                            .await;
                    }
                }
            }
            EventData::DeleteRows(event) => {
                let table_name = table_map.get(&event.table_id).unwrap().as_str();
                let database_name =
                    table_database_map.get(&event.table_id).unwrap().as_str();
                if self.is_target_database_and_table(database_name, table_name) {
                    println!("DeleteRows: {}.{}", database_name, table_name);
                    for row in event.rows {
                        let before: HashMap<String, Value> =
                            self.parse_row(row, &mut columns).await;
                        let after: HashMap<String, Value> = HashMap::new();
                        let op = Operation::DELETE;
                        let data_buffer = DataBuffer { before, after, op };
                        // sink.lock().await.write_record(&data_buffer);
                        Self::write_record_with_retry(&mut sink, &data_buffer)
                            .await;
                    }
                }
            }
            EventData::UpdateRows(event) => {
                let table_name = table_map.get(&event.table_id).unwrap().as_str();
                let database_name =
                    table_database_map.get(&event.table_id).unwrap().as_str();
                if self.is_target_database_and_table(database_name, table_name) {
                    println!("UpdateRows: {}.{}", database_name, table_name);
                    for (b, a) in event.rows {
                        let before: HashMap<String, Value> =
                            self.parse_row(b, &mut columns).await;
                        let after: HashMap<String, Value> =
                            self.parse_row(a, &mut columns).await;
                        let op = Operation::UPDATE;
                        let data_buffer = DataBuffer { before, after, op };
                        // sink.lock().await.write_record(&data_buffer);
                        Self::write_record_with_retry(&mut sink, &data_buffer)
                            .await;
                    }
                }
            }
            _ => {}
        }
        Self::flush_with_retry(&mut sink).await;
    }
}

#[async_trait]
impl Source for MySQLSource {
    async fn start(
        &mut self,
        mut sink: Arc<Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("Starting MySQL binlog source");
        let mut columns: Mutex<Vec<String>> = Mutex::new(vec![]);
        // 这里获取列名
        // columns = self.fill_table_column().await;

        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        loop {
            let max = self.streams.len();
            for i in 0..max {
                let stream = &mut self.streams[i];

                match stream.read().await {
                    Ok((_header, data)) => {
                        self.push_data(&mut sink, &mut columns, &mut table_map, &mut table_database_map, data).await;
                    }
                    Err(e) => {
                        // 打印错误信息，并且继续监听
                        println!("Error: {}", e);
                    }
                }
            }
        }
    }
}
