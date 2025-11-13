use async_trait::async_trait;
use common::{CdcConfig, DataBuffer, Operation, Sink, Source, Value};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use serde::Deserialize;
use serde::Serialize;
use sqlx::{MySqlPool, Row};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

pub struct MySQLSource {
    streams: Vec<BinlogStream>, // ✅ 多个流
    mysql_source: Vec<MysqlSourceConfigDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSourceConfig {
    table_name_list: Vec<String>,
    mysql_source: Vec<MysqlSourceConfigDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSourceConfigDetail {
    username: String,
    password: String,
    host: String,
    port: String,
    database: String,
    table_name: String,
    server_id: u64,
    connection_url: String,
}

impl MysqlSourceConfig {
    pub async fn new(config: CdcConfig) -> Self {
        let size = config.source_config.len();
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        let table_name = config.first_source("table_name");
        // TODO 后续要支持多张表
        let table_name_list: Vec<String> = vec![table_name.clone()];

        for i in 0..size {
            let username = config.source("username", i);
            let password = config.source("password", i);
            let host = config.source("host", i);
            let port = config.source("port", i);
            let database = config.source("database", i);
            let server_id: u64 = config.source("server_id", i).parse::<u64>().unwrap_or(0);
            let connection_url = format!(
                "mysql://{}:{}@{}:{}/{}",
                username,
                password,
                host,
                port,
                database.clone(),
            );
            mysql_source.push(MysqlSourceConfigDetail {
                username,
                password,
                host,
                port,
                database,
                table_name: table_name.clone(),
                server_id,
                connection_url,
            });
        }
        MysqlSourceConfig {
            table_name_list,
            mysql_source,
        }
    }
}

impl MysqlSourceConfigDetail {
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
}

impl MySQLSource {
    pub async fn new(config: CdcConfig) -> Self {
        let mut streams: Vec<BinlogStream> = vec![];
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        // 这里支持多个数据源配置
        let cfg: MysqlSourceConfig = MysqlSourceConfig::new(config).await;
        let size = cfg.mysql_source.len();
        for i in 0..size {
            let server_id: u64 = cfg.mysql_source[i].server_id;
            let connection_url = cfg.mysql_source[i].connection_url.clone();
            let client: BinlogStream =
                BinlogClient::new(&connection_url, server_id, StartPosition::Latest)
                    .with_master_heartbeat(Duration::from_secs(5))
                    .with_read_timeout(Duration::from_secs(60))
                    .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
                    .connect()
                    .await
                    .unwrap();

            streams.push(client);
            mysql_source.push(cfg.mysql_source[i].clone());
        }

        Self {
            streams,
            mysql_source,
        }
    }

    async fn write_record_with_retry(
        sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>,
        data_buffer: &DataBuffer,
    ) {
        let mut loop_count = 0;
        loop {
            let sink_result = sink.lock().await.write_record(&data_buffer).await;
            if sink_result.is_ok() {
                break;
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
            if sink_result.is_ok() {
                break;
            }
            loop_count += 1;
            if loop_count >= 3 {
                panic!("flush error");
            }
        }
    }
}

#[async_trait]
impl Source for MySQLSource {
    async fn start(
        &mut self,
        mut sink: Arc<Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Starting MySQL binlog source");
        let mut columns: Mutex<Vec<String>> = Mutex::new(vec![]);
        // 这里获取列名
        // columns = self.fill_table_column().await;

        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        loop {
            let max = self.streams.len();
            for i in 0..max {
                // let stream_bind: StreamBind = self.streams[i].into();
                let stream: &mut BinlogStream = &mut self.streams[i];
                let config: &MysqlSourceConfigDetail = &mut self.mysql_source[i];

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
                                if (&config).is_target_database_and_table(database_name, table_name)
                                {
                                    info!("WriteRows: {}.{}", database_name, table_name);
                                    for row in event.rows {
                                        let before: HashMap<String, Value> = HashMap::new();
                                        let after: HashMap<String, Value> =
                                            parse_row(row, &mut columns, &config).await;
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
                                if (&config).is_target_database_and_table(database_name, table_name)
                                {
                                    info!("DeleteRows: {}.{}", database_name, table_name);
                                    for row in event.rows {
                                        let before: HashMap<String, Value> =
                                            parse_row(row, &mut columns, &config).await;
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
                                if (&config).is_target_database_and_table(database_name, table_name)
                                {
                                    info!("UpdateRows: {}.{}", database_name, table_name);
                                    for (b, a) in event.rows {
                                        let before: HashMap<String, Value> =
                                            parse_row(b, &mut columns, &config).await;
                                        let after: HashMap<String, Value> =
                                            parse_row(a, &mut columns, &config).await;
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
                    Err(e) => {
                        // 打印错误信息，并且继续监听
                        error!("Error: {}", e);
                    }
                }
            }
        }
    }
}

async fn parse_row(
    row: RowEvent,
    // data: &mut HashMap<String, Value>,
    columns: &mut Mutex<Vec<String>>,
    config: &MysqlSourceConfigDetail,
) -> HashMap<String, Value> {
    let mut data: HashMap<String, Value> = HashMap::new();
    let mut index = 0;
    if columns.lock().await.len() != row.column_values.len() {
        let columns_new = config.fill_table_column().await;
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
                    .for_each(|column_name| error!("column: {}", column_name));
                error!("column_name: {:?}", column_name);
                error!("column_value: {:?}", column_value);
                panic!("unsupported column value type")
            }
        }

        index += 1;
    }
    data
}
