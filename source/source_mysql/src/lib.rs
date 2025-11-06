use async_trait::async_trait;
use common::{CdcConfig, DataBuffer, Operation, Sink, Source, Value};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::column::json::json_binary::JsonBinary;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
// use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use sqlx::{MySqlPool, Row};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct MySQLSource {
    // config: SourceConfig,
    connection_url: String,
    server_id: u64,
    // start_position: StartPosition,
    database: String,
    table_name: String,
    // db:  DbConn,
}

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, DeriveEntityModel)]
// #[serde(rename_all = "camelCase")]
// #[sea_orm(table_name = "running_event")]
// // #[sea_orm(from_query_result)]
// pub struct ModelXXX {
//     pub column_name: String,
// }

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
            username,
            password,
            host,
            port,
            database.clone()
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
            database,
            table_name,
        }
    }

    async fn write_record_with_retry(
        sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>,
        data_buffer: &DataBuffer,
    ) {
        let mut loop_count = 0;
        loop {
            let sink_result = sink.lock().await.write_record(&data_buffer);
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
            let sink_result = sink.lock().await.flush();
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

        // let params = vec![self.database.clone(), self.table_name.clone()];

        // 3. 创建 Statement，SeaORM 会根据 DbBackend::MySql 使用 `?`作为占位符
        // let stmt = Statement::from_sql_and_values(DbBackend::MySql, sql, vec![self.database.clone(), self.table_name.clone()]);
        // 4. 执行查询并将结果映射到 Vec<running_event::Model>
        // ModelXXX::find_by_statement(stmt)
        //     .all(&self.db)
        //     .await
        //     .unwrap_or_else(|e| {
        //         panic!("Error executing query: {}", e);
        //     })

        // let conn = match Database::connect(&self.connection_url).await {
        //     Ok(c) => c,
        //     Err(e) => {
        //         // error!("sea-orm {}", e);
        //         // error!("sea-orm 数据库连接失败");
        //         // process::exit(1);
        //         panic!("sea-orm 数据库连接失败 {}", e)
        //     }
        // };
        //
        //
        // conn.query_all(stmt)
        //     .await
        //     .unwrap_or_else(|e| panic!("Error executing query: {}", e))
        //     .into_iter()
        //     .map(|row| row.get("column_name"))
        //     .collect()

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
        //
        // conn.execute(Statement::from_sql_and_values(
        //     DbBackend::MySql,
        //     sql,
        //     params,
        // ))
        // .await
    }

    async fn parse_row(
        &self,
        row: RowEvent,
        data: &mut HashMap<String, Value>,
        columns: &mut Mutex<Vec<String>>,
    ) {
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
            if let ColumnValue::None = column_value {
                data.insert(column_name, Value::None);
            } else if let ColumnValue::String(bytes) = column_value {
                println!("string column: {}", String::from_utf8_lossy(&bytes));
                let value: Value = Value::String(String::from_utf8_lossy(&bytes).to_string());
                println!("value: {:?}", value);
                data.insert(column_name, value);
            } else if let ColumnValue::Tiny(v) = column_value {
                let value: Value = Value::Tiny(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Short(v) = column_value {
                let value: Value = Value::Short(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Long(v) = column_value {
                let value: Value = Value::Long(v);
                data.insert(column_name, value);
            } else if let ColumnValue::LongLong(v) = column_value {
                let value: Value = Value::LongLong(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Float(v) = column_value {
                let value: Value = Value::Float(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Double(v) = column_value {
                let value: Value = Value::Double(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Decimal(v) = column_value {
                let value: Value = Value::Decimal(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Time(v) = column_value {
                let value: Value = Value::Time(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Date(v) = column_value {
                let value: Value = Value::Date(v);
                data.insert(column_name, value);
            } else if let ColumnValue::DateTime(v) = column_value {
                let value: Value = Value::DateTime(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Timestamp(v) = column_value {
                let value: Value = Value::Timestamp(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Year(v) = column_value {
                let value: Value = Value::Year(v);
                data.insert(column_name, value);
            } else if let ColumnValue::Blob(v) = column_value {
                let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
                data.insert(column_name, value);
            } else if let ColumnValue::Json(v) = column_value {
                let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
                data.insert(column_name, value);
            } else {
                columns
                    .lock()
                    .await
                    .iter()
                    .for_each(|column_name| println!("column: {}", column_name));
                println!("column_name: {:?}", column_name);
                println!("column_value: {:?}", column_value);
                panic!("unsupported column value type")
            }
            index = index + 1;
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

        // let mut columns: Vec<String> = vec![];
        let mut columns: Mutex<Vec<String>> = Mutex::new(vec![]);
        // 这里获取列名
        // columns = self.fill_table_column().await;

        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        //
        loop {
            match stream.read().await {
                Ok((_header, data)) => {
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
                                    let mut before: HashMap<String, Value> = HashMap::new();
                                    let mut after: HashMap<String, Value> = HashMap::new();
                                    self.parse_row(row, &mut after, &mut columns).await;
                                    let op = Operation::CREATE;
                                    let data_buffer = DataBuffer { before, after, op };
                                    // sink.lock().await.write_record(&data_buffer);
                                    Self::write_record_with_retry(&mut sink, &data_buffer).await;
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
                                    let mut before: HashMap<String, Value> = HashMap::new();
                                    let mut after: HashMap<String, Value> = HashMap::new();
                                    self.parse_row(row, &mut before, &mut columns).await;
                                    let op = Operation::DELETE;
                                    let data_buffer = DataBuffer { before, after, op };
                                    // sink.lock().await.write_record(&data_buffer);
                                    Self::write_record_with_retry(&mut sink, &data_buffer).await;
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
                                    let mut before: HashMap<String, Value> = HashMap::new();
                                    let mut after: HashMap<String, Value> = HashMap::new();
                                    self.parse_row(b, &mut before, &mut columns).await;
                                    self.parse_row(a, &mut after, &mut columns).await;
                                    let op = Operation::UPDATE;
                                    let data_buffer = DataBuffer { before, after, op };
                                    // sink.lock().await.write_record(&data_buffer);
                                    Self::write_record_with_retry(&mut sink, &data_buffer).await;
                                }
                            }
                        }
                        _ => {}
                    }
                    Self::flush_with_retry(&mut sink).await;
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
