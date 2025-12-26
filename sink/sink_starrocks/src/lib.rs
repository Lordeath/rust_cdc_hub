mod starrocks_client;

use crate::starrocks_client::StarrocksClient;
use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value};
use meilisearch_sdk::macro_helper::async_trait;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::info;
use tracing::log::trace;

const BATCH_SIZE: usize = 1024;

pub const OP_UPSERT: u8 = 0;
pub const OP_DELETE: u8 = 1;

pub struct StarrocksSink {
    // pool: Pool<MySql>,
    starrocks_client: StarrocksClient,
    health_url: String,
    database: String,

    table_info_list: Vec<TableInfoVo>,
    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,

    // 缓存所有字段名（第一批数据会取一次）
    table_info_cache: Mutex<HashMap<String, TableInfoVo>>,
    columns_cache: Mutex<HashMap<String, Vec<String>>>,
    pks_cache: Mutex<HashMap<String, Vec<String>>>,
}

impl StarrocksSink {
    pub async fn new(config: &CdcConfig, table_info_list: Vec<TableInfoVo>) -> Self {
        let username = config.first_sink_not_blank("username");
        let password = config.first_sink_not_blank("password");
        let host = config.first_sink_not_blank("host");
        // let port = config.first_sink_not_blank("port");
        let http_port = config.first_sink_not_blank("http_port");
        let database = config.first_sink_not_blank("database");

        let base_url = format!("http://{}:{}/api/", host, http_port);
        let starrocks_client: StarrocksClient =
            StarrocksClient::new(base_url.as_str(), username.as_str(), password.as_str()).await;
        StarrocksSink {
            starrocks_client,
            health_url: format!("http://{}:{}/api/health", host, http_port),
            database,
            table_info_list,
            buffer: Mutex::new(vec![]),
            initialized: RwLock::new(false),
            table_info_cache: Mutex::new(HashMap::new()),
            columns_cache: Mutex::new(HashMap::new()),
            pks_cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn cast_to_starrocks_map(&self, data_buffer: &DataBuffer) -> HashMap<String, Value> {
        let pks_info: &HashMap<String, Vec<String>> = &*self.pks_cache.lock().await;
        let col_info: &HashMap<String, Vec<String>> = &*self.columns_cache.lock().await;
        // 把数据转换成json
        let data: HashMap<String, Value> = match data_buffer.op {
            Operation::DELETE => {
                let mut obj = HashMap::new();
                for (table_name, pks) in pks_info {
                    if !table_name.eq_ignore_ascii_case(&data_buffer.table_name) {
                        continue;
                    }
                    for pk in pks {
                        for (k, v) in &data_buffer.before {
                            if k.eq_ignore_ascii_case(pk) {
                                obj.insert(pk.clone(), v.clone());
                                break;
                            }
                        }
                    }
                    obj.insert("__op".to_string(), Value::UnsignedInt8(OP_DELETE));
                    break;
                }
                obj
            }
            _ => {
                let mut obj = HashMap::new();
                for (table_name, cols) in col_info {
                    if !table_name.eq_ignore_ascii_case(&data_buffer.table_name) {
                        continue;
                    }
                    for col in cols {
                        for (k, v) in &data_buffer.after {
                            if k.eq_ignore_ascii_case(col) {
                                obj.insert(col.clone(), v.clone());
                                break;
                            }
                        }
                    }
                    obj.insert("__op".to_string(), Value::UnsignedInt8(OP_UPSERT));
                    break;
                }
                obj
            }
        };
        data
    }
}

#[async_trait]
impl Sink for StarrocksSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 可选：ping FE
        let _ = self
            .starrocks_client
            .client
            .get(&self.health_url)
            .send()
            .await?;
        info!("Starrocks FE is healthy");
        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;
        buf.push(record.clone());

        if buf.len() >= BATCH_SIZE {
            drop(buf);
            self.flush_with_retry(&FlushByOperation::Signal).await;
        }

        Ok(())
    }

    async fn flush(
        &self,
        flush_by_operation: &FlushByOperation,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;

        match flush_by_operation {
            FlushByOperation::Timer => {
                if !buf.is_empty() {
                    info!("Flushing Mysql Sink by timer... {}", buf.len());
                }
            }
            FlushByOperation::Init => {
                if !buf.is_empty() {
                    info!("Flushing Mysql Sink by init... {}", buf.len());
                }
            }
            FlushByOperation::Signal => {
                if !buf.is_empty() {
                    info!("Flushing Mysql Sink by signal... {}", buf.len());
                }
            }
            FlushByOperation::Cdc => {
                if !buf.is_empty() {
                    info!("Flushing Mysql Sink by cdc... {}", buf.len());
                }
            }
        }
        if buf.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut *buf);
        drop(buf);

        // 初始化字段名（只做一次）
        if !*self.initialized.read().await {
            let mut col_info: HashMap<String, Vec<String>> = HashMap::new();
            // 用实际的pk来作为主键，因为异构数据库的pk可能会有不同
            let mut pks_info: HashMap<String, Vec<String>> = HashMap::new();
            // let pool = &self.pool;
            for table_info in &self.table_info_list {
                let table_name = table_info.table_name.clone();
                let cols = table_info.columns.clone();
                col_info.entry(table_name.clone()).or_default().extend(cols);
                let sql = &format!(
                    r#"
                    select COLUMN_NAME AS column_name from information_schema.`COLUMNS` c where TABLE_SCHEMA = DATABASE()
                    AND COLUMN_KEY = "PRI"
                    AND c.TABLE_NAME = "{}"
                    ORDER BY ORDINAL_POSITION
                "#,
                    table_name.clone()
                );
                let result = self.starrocks_client.execute_sql(&self.database, sql).await;
                for s in result.split("\n") {
                    if !s.starts_with("{\"data\":[\"") {
                        continue;
                    }
                    let pk = s[10..s.len() - 3].to_string();
                    info!("pk: {}", pk);
                    pks_info.entry(table_name.clone()).or_default().push(pk);
                }
            }
            *self.columns_cache.lock().await = col_info;
            *self.pks_cache.lock().await = pks_info;
            *self.initialized.write().await = true;
        }

        let mut json_list_map: HashMap<String, Vec<HashMap<String, Value>>> = HashMap::new();

        let mut cache_for_roll_back: Vec<DataBuffer> = vec![];
        for r in batch {
            let table_name = r.table_name.clone();
            if !self.table_info_cache.lock().await.contains_key(&table_name) {
                for table_info in &self.table_info_list {
                    if table_info.table_name == table_name {
                        self.table_info_cache
                            .lock()
                            .await
                            .insert(table_name.clone(), table_info.clone());
                        break;
                    }
                }
            }
            cache_for_roll_back.push(r.clone());
            json_list_map
                .entry(table_name)
                .or_default()
                .push(self.cast_to_starrocks_map(&r.clone()).await);
        }

        for (k, v) in json_list_map {
            let body = serde_json::to_string(&v).expect("serialize json failed");
            trace!("数据长度: {}", v.len());
            let c = &self
                .table_info_list
                .iter()
                .find(|x| x.table_name.eq_ignore_ascii_case(&k))
                .unwrap()
                .columns;
            let mut c2 = c.clone();
            c2.push("__op".to_string());
            let columns = &c2.join(",");
            let result = self
                .starrocks_client
                .stream_load(&self.database, &k, body, columns.to_string())
                .await;
            if result.is_err() {
                let mut buf = self.buffer.lock().await;
                for cached_data_buffer in cache_for_roll_back {
                    buf.push(cached_data_buffer);
                }
                return Err(result.err().unwrap());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_json() {
        let data: HashMap<String, Value> = HashMap::new();
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(json, "{}")
    }
    #[tokio::test]
    async fn test_json2() {
        let mut data: HashMap<String, Value> = HashMap::new();
        data.insert("a".to_string(), Value::Int8(1));
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(json, "{\"a\":1}")
    }
    #[tokio::test]
    async fn test_json3() {
        let mut data: HashMap<String, Value> = HashMap::new();
        data.insert("a".to_string(), Value::String("1".to_string()));
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(json, "{\"a\":\"1\"}")
    }
}
