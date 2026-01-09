pub mod mysql_checkpoint;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::HashMap;
use std::collections::hash_map::Keys;
use std::error::Error;
use std::{fmt, process};
use std::sync::Arc;
use std::time::Duration;
use strum_macros::Display;
use tokio::sync::Mutex;
use tracing::{error, info, trace};

use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row, ValueRef};
use sqlx::{MySql, Pool, TypeInfo};

use crate::mysql_checkpoint::MysqlCheckPointDetailEntity;
use serde_json::Value as JsonValue;

#[async_trait]
pub trait Source: Send + Sync {
    async fn start(
        &mut self,
        sink: Arc<Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    async fn add_plugins(&mut self, plugin: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>);

    async fn get_table_info(&mut self) -> Vec<TableInfoVo>;
}

/// Sink trait: 所有目标端都要实现它
#[async_trait]
pub trait Sink: Send + Sync {
    /// 连接目标端（如 Kafka、文件、数据库）
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// 写入一条数据（可以是 json 或结构化 map）
    async fn write_record(
        &mut self,
        record: &DataBuffer,
        mysql_check_point_detail_entity: &Option<MysqlCheckPointDetailEntity>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    async fn flush_with_retry(&mut self, from_timer: &FlushByOperation) {
        let mut loop_count = 0;
        loop {
            loop_count += 1;
            let sink_result = self.flush(from_timer).await;
            if sink_result.is_ok() {
                break;
            }
            error!(
                "error occurred {} loop_count: {}",
                sink_result.expect_err("error not found").to_string(),
                loop_count
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            if loop_count > 30 {
                error!("flush error");
                process::exit(1);
            }
        }
        trace!("flush success");
        match self.alter_flush().await {
            Ok(_) => {}
            Err(e) => {
                error!("alter flush error: {}", e.to_string())
            }
        }
    }

    /// 刷新缓冲区（可选）
    async fn flush(&self, _from_timer: &FlushByOperation) -> Result<(), String>;

    async fn alter_flush(&mut self) -> Result<(), String>;
}

#[async_trait]
pub trait Plugin: Send + Sync {
    async fn collect(&mut self, data_buffer: DataBuffer) -> Result<DataBuffer, ()>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum SourceType {
    MySQL,
    // 未来可扩展：Postgres, Kafka, Mongo, 等
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum SinkType {
    Print,
    MeiliSearch,
    MySQL,
    Starrocks,
    // 未来可扩展：Postgres, Kafka, Mongo, 等
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum PluginType {
    #[strum(serialize = "column_in")]
    ColumnIn,
    #[strum(serialize = "plus")]
    Plus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    pub source_type: SourceType,
    pub sink_type: SinkType,
    pub source_config: Vec<HashMap<String, String>>,
    pub sink_config: Vec<HashMap<String, String>>,
    pub auto_create_database: Option<bool>,
    pub auto_create_table: Option<bool>,
    pub plugins: Option<Vec<PluginConfig>>,
    pub source_batch_size: Option<usize>,
    pub sink_batch_size: Option<usize>,
    pub checkpoint_file_path: Option<String>,
    pub log_level: Option<String>,
}

impl CdcConfig {
    pub fn source(&self, key: &str, index: usize) -> String {
        match self.source_config[index].get(key) {
            None => "".to_string(),
            Some(v) => v.clone(),
        }
    }

    pub fn sink(&self, key: &str, index: usize) -> String {
        match self.sink_config[index].get(key) {
            None => "".to_string(),
            Some(v) => v.clone(),
        }
    }

    pub fn first_source(&self, key: &str) -> String {
        self.source(key, 0)
    }
    pub fn first_sink(&self, key: &str) -> String {
        self.sink(key, 0)
    }
    pub fn first_sink_not_blank(&self, key: &str) -> String {
        let x = self.sink(key, 0);
        if x.is_empty() {
            panic!("sink config is blank: {}", key)
        }
        x
    }
    pub fn first_u64_source(&self, key: &str) -> u64 {
        let value = self.first_source(key);
        value.parse::<u64>().unwrap_or(0)
    }
    pub fn first_u32_source(&self, key: &str) -> u32 {
        let value = self.first_source(key);
        value.parse::<u32>().unwrap_or(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    pub plugin_type: PluginType,
    pub config: HashMap<String, String>,
}
impl PluginConfig {
    pub fn get_config(&self, key: &str) -> String {
        match self.config.get(key) {
            None => "".to_string(),
            Some(v) => v.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataBuffer {
    pub table_name: String,
    pub before: CaseInsensitiveHashMap,
    pub after: CaseInsensitiveHashMap,
    pub op: Operation,
    pub binlog_filename: String,
    pub timestamp: u32,
    pub next_event_position: u32,
}

impl DataBuffer {
    pub fn new(
        table_name: String,
        before: CaseInsensitiveHashMap,
        after: CaseInsensitiveHashMap,
        op: Operation,
        binlog_filename: String,
        timestamp: u32,
        next_event_position: u32,
    ) -> DataBuffer {
        let mut before_key_to_lower: HashMap<String, String> = HashMap::new();
        for key in before.keys() {
            before_key_to_lower.insert(key.to_lowercase(), key.clone());
        }

        let mut after_key_to_lower: HashMap<String, String> = HashMap::new();
        for key in after.keys() {
            after_key_to_lower.insert(key.to_lowercase(), key.clone());
        }

        DataBuffer {
            table_name,
            before,
            after,
            op,
            binlog_filename,
            timestamp,
            next_event_position,
        }
    }

    pub fn get_pk(&self, pk_name: &str) -> &Value {
        let mut result = self.after.get(pk_name);
        if result.is_none() {
            result = self.before.get(pk_name);
        }
        result
    }

    pub fn new_before(&self, data: CaseInsensitiveHashMap) -> DataBuffer {
        DataBuffer::new(
            self.table_name.clone(),
            data,
            self.after.clone(),
            self.op.clone(),
            self.binlog_filename.clone(),
            self.timestamp,
            self.next_event_position,
        )
    }
    pub fn new_after(&self, data: CaseInsensitiveHashMap) -> DataBuffer {
        DataBuffer::new(
            self.table_name.clone(),
            self.before.clone(),
            data,
            self.op.clone(),
            self.binlog_filename.clone(),
            self.timestamp,
            self.next_event_position,
        )
    }
}

pub enum FlushByOperation {
    Timer,
    Init,
    Signal,
    Cdc,
}

#[derive(Debug, Clone, Display)]
pub enum Value {
    // Str(String),
    // Num(i64),
    // Bool(bool),
    None,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UnsignedInt8(u8),
    UnsignedInt32(u32),
    UnsignedInt64(u64),
    // A 32 bit floating point number
    Float(f32),
    // A 64 bit floating point number
    Double(f64),
    // A decimal value
    Decimal(String),
    // A datatype to store a time value
    Time(String),
    // A datatype to store a date value
    Date(String),
    // A datatype containing timestamp values ranging from
    // '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.
    DateTime(String),
    // A datatype containing timestamp values ranging from
    // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
    // MySQL converts TIMESTAMP values from the current time zone to UTC for storage,
    // and back from UTC to the current time zone for retrieval.
    // (This does not occur for other types such as DATETIME.)
    // refer: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    Timestamp(i64),
    // A datatype to store year with a range of 1901 to 2155,
    // refer: https://dev.mysql.com/doc/refman/8.0/en/year.html
    Year(u16),
    // A datatype for string values
    String(String),
    // A datatype containing binary large objects
    Blob(String),
    Json(String),
    // A datatype containing a set of bit
    Bit(u64),
}

impl Value {
    pub fn resolve_string(&self) -> String {
        match self {
            Value::String(s) => s.to_string(),
            Value::Int8(s) => s.to_string(),
            Value::Int16(s) => s.to_string(),
            Value::Int32(s) => s.to_string(),
            Value::Int64(s) => s.to_string(),
            Value::UnsignedInt8(s) => s.to_string(),
            Value::UnsignedInt32(s) => s.to_string(),
            Value::UnsignedInt64(s) => s.to_string(),
            Value::Float(s) => s.to_string(),
            Value::Double(s) => s.to_string(),
            Value::Decimal(s) => s.to_string(),
            Value::Time(s) => s.to_string(),
            Value::Date(s) => s.to_string(),
            Value::DateTime(s) => s.to_string(),
            // === ⭐ Timestamp 格式化（微秒 → "YYYY-MM-DD HH:MM:SS"）===
            Value::Timestamp(micros) => {
                let secs = micros / 1_000_000;
                let sub_us = (micros % 1_000_000) as u32;

                let utc_dt = DateTime::<Utc>::from_timestamp(secs, sub_us * 1000)
                    .expect("invalid timestamp");

                // 转成东八区
                let offset = FixedOffset::east_opt(Local::now().offset().local_minus_utc())
                    .unwrap_or_else(|| panic!("invalid offset"));
                let dt_utc8 = utc_dt.with_timezone(&offset);

                dt_utc8.format("%Y-%m-%d %H:%M:%S").to_string()
            }
            Value::Year(s) => s.to_string(),
            Value::Blob(s) => s.to_string(),
            Value::Json(s) => s.to_string(),
            Value::Bit(s) => s.to_string(),
            Value::None => "null".to_string(),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }
    pub fn is_json(&self) -> bool {
        matches!(self, Value::Json(_))
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::None => serializer.serialize_none(),
            Value::String(s)
            | Value::Decimal(s)
            | Value::Time(s)
            | Value::Date(s)
            | Value::DateTime(s)
            | Value::Json(s)
            | Value::Blob(s) => serializer.serialize_str(s.as_str()),
            Value::Int8(v) => serializer.serialize_i8(*v),
            Value::Int16(v) => serializer.serialize_i16(*v),
            Value::Int32(v) => serializer.serialize_i32(*v),
            Value::Int64(v) => serializer.serialize_i64(*v),
            Value::Float(v) => serializer.serialize_f32(*v),
            Value::Double(v) => serializer.serialize_f64(*v),
            Value::Timestamp(v) => serializer.serialize_i64(*v),
            Value::Year(v) => serializer.serialize_u16(*v),
            Value::Bit(v) => serializer.serialize_u64(*v),
            Value::UnsignedInt8(v) => serializer.serialize_u8(*v),
            Value::UnsignedInt32(v) => serializer.serialize_u32(*v),
            Value::UnsignedInt64(v) => serializer.serialize_u64(*v),
        }
    }
}

// 如果你需要从 JSON 反序列化回来，也可以加上这个：
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON primitive or null")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Int64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Bit(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Double(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::String(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::String(v))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::None)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Operation {
    // READ,
    CREATE(bool),
    UPDATE,
    DELETE,
    TRUNCATE,
    MESSAGE,
    OTHER,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfoVo {
    pub table_name: String,
    pub pk_column: String,
    pub create_table_sql: String,
    pub columns: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfoVo {
    pub column_name: String,
    pub value_for_type: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseInsensitiveHashMap {
    map: HashMap<String, Value>,
    key_map_to_lowercase: HashMap<String, String>,
}

impl CaseInsensitiveHashMap {
    pub fn new_with_no_arg() -> Self {
        CaseInsensitiveHashMap::new(HashMap::new())
    }
    pub fn new(map: HashMap<String, Value>) -> Self {
        let mut key_map_to_lowercase: HashMap<String, String> = HashMap::new();
        for key in map.keys() {
            key_map_to_lowercase.insert(key.to_lowercase(), key.to_string());
        }
        CaseInsensitiveHashMap {
            map,
            key_map_to_lowercase,
        }
    }

    pub fn get(&self, key: &str) -> &Value {
        match self.key_map_to_lowercase.get(&key.to_lowercase()) {
            None => &Value::None,
            Some(real_key) => self.map.get(real_key).unwrap_or(&Value::None),
        }
    }

    pub fn keys(&self) -> Keys<'_, String, Value> {
        self.map.keys()
    }

    pub fn insert(&mut self, k: String, v: Value) -> Option<Value> {
        self.key_map_to_lowercase
            .insert(k.to_lowercase(), k.clone());
        self.map.insert(k, v)
    }

    pub fn get_raw_map(&self) -> HashMap<String, Value> {
        self.map.clone()
    }
}

#[inline]
pub fn mysql_row_to_hashmap(row: &MySqlRow) -> CaseInsensitiveHashMap {
    let mut result = HashMap::new();
    for row_column in row.columns().iter().enumerate() {
        let column = row_column.1;
        let name = column.name().to_string();
        let is_null = row
            .try_get_raw(name.as_str())
            .expect("mysql_row_to_hashmap")
            .is_null();

        let value = if is_null {
            Value::None
        } else {
            match column.type_info().name() {
                "INT" => match row.try_get::<i32, _>(name.as_str()) {
                    Ok(v) => Value::Int32(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "INT UNSIGNED" => match row.try_get::<u32, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt32(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BIGINT UNSIGNED" => match row.try_get::<u64, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt64(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BIGINT" => match row.try_get::<i64, _>(name.as_str()) {
                    Ok(v) => Value::Int64(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BOOLEAN" => match row.try_get::<bool, _>(name.as_str()) {
                    Ok(v) => {
                        if v {
                            Value::Int8(0)
                        } else {
                            Value::Int8(1)
                        }
                    }
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "TINYINT" => match row.try_get::<i8, _>(name.as_str()) {
                    Ok(v) => Value::Int8(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "TINYINT UNSIGNED" => match row.try_get::<u8, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt8(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DATE" => match row.try_get::<NaiveDate, _>(name.as_str()) {
                    Ok(v) => Value::String(v.to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "TIME" => match row.try_get::<NaiveTime, _>(name.as_str()) {
                    Ok(v) => Value::String(v.format("%H:%M:%S").to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DATETIME" => match row.try_get::<NaiveDateTime, _>(name.as_str()) {
                    Ok(v) => Value::String(v.to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "TIMESTAMP" => match row.try_get::<DateTime<Utc>, _>(name.as_str()) {
                    Ok(v) => {
                        // 推荐：将带时区的 DateTime<Utc> 转换为 Unix 时间戳（i64 秒数）
                        let ts_ms = v.timestamp_millis();
                        let secs = ts_ms / 1000;
                        let nanos = ((ts_ms % 1000) * 1_000_000) as u32;

                        let dt = DateTime::from_timestamp(secs, nanos)
                            .unwrap_or_else(|| {
                                DateTime::from_timestamp(0, 0)
                                    .unwrap_or_else(|| panic!("无法创建 DateTime"))
                            })
                            .naive_utc();

                        let x = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
                        // 使用 DateTime
                        Value::DateTime(x)
                    }
                    Err(e) => {
                        // ... 错误处理保持不变
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "VARCHAR" | "TEXT" => match row.try_get::<String, _>(name.as_str()) {
                    Ok(v) => Value::String(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "JSON" => match row.try_get::<JsonValue, &str>(name.as_str()) {
                    Ok(v) => Value::Json(v.to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "FLOAT" | "DOUBLE" => match row.try_get::<f64, _>(name.as_str()) {
                    Ok(v) => Value::Double(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DECIMAL" => match row.try_get::<BigDecimal, _>(name.as_str()) {
                    Ok(v) => Value::Decimal(v.to_string()), // 假设您有 Value::Decimal 变体
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BLOB" => match row.try_get::<Vec<u8>, _>(name.as_str()) {
                    Ok(v) => Value::Blob(String::from_utf8(v).expect("Invalid UTF-8").to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                _ => {
                    error!("Unsupported column type: {}", column.type_info().name());
                    panic!("Unsupported column type: {}", column.type_info().name())
                }
            }
        };

        result.insert(column.name().to_string(), value);
    }
    CaseInsensitiveHashMap::new(result)
}

pub async fn get_mysql_pool_by_url(
    connection_url: &str,
    from: &str,
) -> Result<Pool<MySql>, String> {
    info!("Connecting to MySQL: {} from: {}", connection_url, from);
    match MySqlPoolOptions::new()
        .max_connections(10)
        // 连接空闲超过 20 分钟直接丢弃
        .idle_timeout(Duration::from_secs(20 * 60))
        // 不管用不用，活超过 2 小时就重建
        .max_lifetime(Duration::from_secs(2 * 60 * 60))
        .connect(connection_url)
        .await
    {
        Ok(x) => {
            info!("Connected to MySQL.");
            Ok(x)
        }
        Err(e) => {
            error!("Failed to connect to MySQL: {}", e);
            Err(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_string_timestamp() {
        // 2025-12-02 11:38:28 UTC → 微秒级：1764646708000000
        let ts = Value::Timestamp(1764646708000000);
        let s = ts.resolve_string();

        assert_eq!(s, "2025-12-02 11:38:28");
    }

    #[test]
    fn test_resolve_string_string() {
        let v = Value::String("hello".to_string());
        assert_eq!(v.resolve_string(), "hello");
    }

    #[test]
    fn test_resolve_string_int() {
        let v = Value::Int32(123);
        assert_eq!(v.resolve_string(), "123");
    }

    #[test]
    fn test_resolve_string_none() {
        let v = Value::None;
        assert_eq!(v.resolve_string(), "null");
    }
}
