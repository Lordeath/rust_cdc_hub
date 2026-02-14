mod starrocks_client;

use crate::starrocks_client::StarrocksClient;
use common::case_insensitive_hash_map::CaseInsensitiveHashMap;
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_type_token_from_column_definition,
};
use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value};
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::log::trace;
use tracing::{error, info};

pub const OP_UPSERT: u8 = 0;
pub const OP_DELETE: u8 = 1;

pub struct StarrocksSink {
    // pool: Pool<MySql>,
    starrocks_client: StarrocksClient,
    health_url: String,
    database: String,
    sink_batch_size: usize,
    auto_add_column: bool,
    auto_modify_column: bool,

    table_info_list: Vec<TableInfoVo>,
    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,

    // 缓存所有字段名（第一批数据会取一次）
    table_info_cache: Mutex<HashMap<String, TableInfoVo>>,
    columns_cache: Mutex<HashMap<String, Vec<String>>>,
    pks_cache: Mutex<HashMap<String, Vec<String>>>,

    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
}

impl StarrocksSink {
    pub async fn new(config: &CdcConfig, table_info_list: Vec<TableInfoVo>) -> Self {
        let username = config.first_sink_not_blank("username");
        let password = config.first_sink_not_blank("password");
        let host = config.first_sink_not_blank("host");
        let http_port = config.first_sink_not_blank("http_port");
        let database = config.first_sink_not_blank("database");

        let base_url = format!("http://{}:{}/api/", host, http_port);
        let starrocks_client: StarrocksClient =
            StarrocksClient::new(base_url.as_str(), username.as_str(), password.as_str()).await;
        let sink_batch_size = config.sink_batch_size.unwrap_or(1024);
        let auto_add_column = config.auto_add_column.unwrap_or(true);
        let auto_modify_column = config.auto_modify_column.unwrap_or(true);
        StarrocksSink {
            starrocks_client,
            health_url: format!("http://{}:{}/api/health", host, http_port),
            database,
            sink_batch_size,
            auto_add_column,
            auto_modify_column,
            table_info_list,
            buffer: Mutex::new(Vec::with_capacity(sink_batch_size)),
            initialized: RwLock::new(false),
            table_info_cache: Mutex::new(HashMap::new()),
            columns_cache: Mutex::new(HashMap::new()),
            pks_cache: Mutex::new(HashMap::new()),
            checkpoint: Mutex::new(HashMap::new()),
        }
    }

    async fn fetch_rows(&self, database: &str, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let text = self.starrocks_client.execute_sql(database, sql).await;
        if text.is_empty() {
            return Err("empty response".to_string());
        }
        let v: JsonValue = serde_json::from_str(text.as_str()).map_err(|e| e.to_string())?;
        let data = v
            .get("data")
            .ok_or_else(|| "missing data".to_string())?
            .as_array()
            .ok_or_else(|| "data not array".to_string())?;
        let mut out: Vec<Vec<String>> = Vec::with_capacity(data.len());
        for row in data {
            let arr = row
                .as_array()
                .ok_or_else(|| "row not array".to_string())?;
            let mut cols: Vec<String> = Vec::with_capacity(arr.len());
            for c in arr {
                if let Some(s) = c.as_str() {
                    cols.push(s.to_string());
                } else {
                    cols.push(c.to_string());
                }
            }
            out.push(cols);
        }
        Ok(out)
    }

    fn map_mysql_type_to_starrocks(mysql_type_token: &str) -> String {
        let t = mysql_type_token.to_ascii_lowercase();
        if t.starts_with("tinyint(1)") {
            return "BOOLEAN".to_string();
        }
        if t.starts_with("tinyint") {
            return "TINYINT".to_string();
        }
        if t.starts_with("smallint") {
            return "SMALLINT".to_string();
        }
        if t.starts_with("mediumint") {
            return "INT".to_string();
        }
        if t.starts_with("int") || t.starts_with("integer") {
            return "INT".to_string();
        }
        if t.starts_with("bigint") {
            return "BIGINT".to_string();
        }
        if t.starts_with("float") {
            return "FLOAT".to_string();
        }
        if t.starts_with("double") {
            return "DOUBLE".to_string();
        }
        if t.starts_with("decimal") || t.starts_with("numeric") {
            return mysql_type_token.to_ascii_uppercase();
        }
        if t.starts_with("datetime") || t.starts_with("timestamp") {
            return "DATETIME".to_string();
        }
        if t.starts_with("date") {
            return "DATE".to_string();
        }
        if t.starts_with("time") {
            return "VARCHAR(32)".to_string();
        }
        if t.starts_with("varchar") || t.starts_with("char") {
            return mysql_type_token.to_ascii_uppercase();
        }
        if t.contains("text")
            || t.contains("blob")
            || t.starts_with("json")
            || t.starts_with("enum")
            || t.starts_with("set")
        {
            return "STRING".to_string();
        }
        "STRING".to_string()
    }

    async fn ensure_schema(&self) {
        if !self.auto_add_column && !self.auto_modify_column {
            return;
        }
        for table_info in &self.table_info_list {
            let table_name = table_info.table_name.clone();
            let defs = extract_mysql_create_table_column_definitions(
                table_info.create_table_sql.as_str(),
            );
            let sql = format!(
                "select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE from information_schema.`COLUMNS` where TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
                self.database, table_name
            );
            let rows = match self.fetch_rows(self.database.as_str(), sql.as_str()).await {
                Ok(v) => v,
                Err(e) => {
                    error!("fetch starrocks columns failed: {} {}", table_name, e);
                    continue;
                }
            };
            let mut exists: HashMap<String, (String, bool)> = HashMap::new();
            for row in rows {
                if row.len() < 3 {
                    continue;
                }
                let name = row[0].to_ascii_lowercase();
                let typ = row[1].to_ascii_lowercase();
                let nullable = row[2].eq_ignore_ascii_case("YES");
                exists.insert(name, (typ, nullable));
            }

            for src_col in &table_info.columns {
                let key = src_col.to_ascii_lowercase();
                let def = match defs.get(&key) {
                    None => continue,
                    Some(v) => v,
                };
                let mysql_type = match mysql_type_token_from_column_definition(def.as_str()) {
                    None => continue,
                    Some(v) => v,
                };
                let starrocks_type = Self::map_mysql_type_to_starrocks(mysql_type.as_str());
                let src_nullable = mysql_column_allows_null_from_definition(def.as_str());
                let nullable_sql = if src_nullable { "NULL" } else { "NOT NULL" };

                match exists.get(&key) {
                    None => {
                        if !self.auto_add_column {
                            continue;
                        }
                        let alter = format!(
                            "ALTER TABLE `{}` ADD COLUMN `{}` {} {}",
                            table_name, src_col, starrocks_type, nullable_sql
                        );
                        let resp = self
                            .starrocks_client
                            .execute_sql(self.database.as_str(), alter.as_str())
                            .await;
                        if resp.is_empty() {
                            error!(
                                "auto add column failed: {} {} empty response",
                                table_name, src_col
                            );
                        } else {
                            info!("auto add column attempted: {} {}", table_name, src_col);
                        }
                    }
                    Some((sink_type, sink_nullable)) => {
                        if !self.auto_modify_column {
                            continue;
                        }
                        let need_modify_type =
                            !sink_type.eq_ignore_ascii_case(&starrocks_type);
                        let need_relax_nullable = src_nullable && !*sink_nullable;
                        if !need_modify_type && !need_relax_nullable {
                            continue;
                        }
                        let alter = format!(
                            "ALTER TABLE `{}` MODIFY COLUMN `{}` {} {}",
                            table_name, src_col, starrocks_type, nullable_sql
                        );
                        let resp = self
                            .starrocks_client
                            .execute_sql(self.database.as_str(), alter.as_str())
                            .await;
                        if resp.is_empty() {
                            error!(
                                "auto modify column failed: {} {} empty response",
                                table_name, src_col
                            );
                        } else {
                            info!("auto modify column attempted: {} {}", table_name, src_col);
                        }
                    }
                }
            }
        }
    }

    pub async fn cast_to_starrocks_map(&self, data_buffer: &DataBuffer) -> HashMap<String, Value> {
        let pks_info: &HashMap<String, Vec<String>> = &*self.pks_cache.lock().await;
        let col_info: &HashMap<String, Vec<String>> = &*self.columns_cache.lock().await;
        // 把数据转换成json
        let data: CaseInsensitiveHashMap = match data_buffer.op {
            Operation::DELETE => {
                let mut obj = CaseInsensitiveHashMap::new_with_no_arg();
                for (table_name, pks) in pks_info {
                    if !table_name.eq_ignore_ascii_case(&data_buffer.table_name) {
                        continue;
                    }
                    for pk in pks {
                        let v = data_buffer.before.get(pk);
                        if !v.is_none() {
                            obj.insert(pk.clone(), v.clone());
                        }
                    }
                    obj.insert("__op".to_string(), Value::UnsignedInt8(OP_DELETE));
                    break;
                }
                obj
            }
            _ => {
                let mut obj = CaseInsensitiveHashMap::new_with_no_arg();
                for (table_name, cols) in col_info {
                    if !table_name.eq_ignore_ascii_case(&data_buffer.table_name) {
                        continue;
                    }
                    for col in cols {
                        let v = data_buffer.after.get(col);
                        if !v.is_none() {
                            obj.insert(col.clone(), v.clone());
                        }
                    }
                    obj.insert("__op".to_string(), Value::UnsignedInt8(OP_UPSERT));
                    break;
                }
                obj
            }
        };
        data.get_raw_map()
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
        self.ensure_schema().await;
        Ok(())
    }

    async fn write_record(
        &mut self,
        record: &DataBuffer,
        mysql_check_point_detail_entity: &Option<MysqlCheckPointDetailEntity>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;
        buf.push(record.clone());
        if let Some(s) = mysql_check_point_detail_entity {
            self.checkpoint
                .lock()
                .await
                .insert(s.checkpoint_filepath.to_string(), s.clone());
        }

        if buf.len() >= self.sink_batch_size {
            drop(buf);
            self.flush_with_retry(&FlushByOperation::Signal).await;
        }

        Ok(())
    }

    async fn flush(&self, flush_by_operation: &FlushByOperation) -> Result<(), String> {
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
                return Err(result.err().unwrap().to_string());
            }
        }

        Ok(())
    }

    async fn alter_flush(&mut self) -> Result<(), String> {
        let err_messages: Vec<String> = self
            .checkpoint
            .lock()
            .await
            .values()
            .map(|s| match s.save() {
                Ok(_) => "".to_string(),
                Err(msg) => {
                    error!("{}", msg);
                    msg
                }
            })
            .find(|x| !x.is_empty())
            .into_iter()
            .collect();
        if !err_messages.is_empty() {
            return Err(err_messages.join("\n").to_string());
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
