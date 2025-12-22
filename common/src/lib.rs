use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::error;

#[async_trait]
pub trait Source: Send + Sync {
    async fn start(
        &mut self,
        sink: Arc<Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    async fn get_table_info(&mut self) -> Vec<TableInfoVo>;
}

/// Sink trait: 所有目标端都要实现它
#[async_trait]
pub trait Sink: Send + Sync {
    /// 连接目标端（如 Kafka、文件、数据库）
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// 写入一条数据（可以是 json 或结构化 map）
    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>>;

    async fn flush_with_retry(&self, from_timer: &FlushByOperation) {
        let mut loop_count = 0;
        loop {
            let sink_result = self.flush(from_timer).await;
            if sink_result.is_ok() {
                break;
            }
            error!(
                "error occurred {} loop_count: {}",
                sink_result.err().unwrap().to_string(),
                loop_count
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            loop_count += 1;
            if loop_count >= 30 {
                panic!("flush error");
            }
        }
    }

    /// 刷新缓冲区（可选）
    async fn flush(
        &self,
        _from_timer: &FlushByOperation,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceType {
    MySQL,
    // 未来可扩展：Postgres, Kafka, Mongo, 等
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SinkType {
    Print,
    MeiliSearch,
    MySQL,
    // 未来可扩展：Postgres, Kafka, Mongo, 等
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    pub source_type: SourceType,
    pub sink_type: SinkType,
    pub source_config: Vec<HashMap<String, String>>,
    pub sink_config: Vec<HashMap<String, String>>,
    pub auto_create_database: Option<bool>,
    pub auto_create_table: Option<bool>,
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
pub struct DataBuffer {
    pub table_name: String,
    pub before: HashMap<String, Value>,
    pub after: HashMap<String, Value>,
    pub op: Operation,
}

// pub const VALUE_NONE: Value = Value::None;

impl DataBuffer {
    pub fn get_pk_before(&self, pk_name: &str) -> &Value {
        self.before.get(pk_name).unwrap_or(&Value::None)
    }
    pub fn get_pk_after(&self, pk_name: &str) -> &Value {
        self.after.get(pk_name).unwrap_or(&Value::None)
    }
    pub fn get_pk(&self, pk_name: &str) -> &Value {
        let mut result = self.get_pk_after(pk_name);
        if result.is_none() {
            result = self.get_pk_before(pk_name);
        }
        result
    }
}

pub enum FlushByOperation {
    Timer,
    Init,
    Signal,
    Cdc,
}

#[derive(Debug, Clone)]
pub enum Value {
    // Str(String),
    // Num(i64),
    // Bool(bool),
    None,
    // A 8 bit signed integer
    Int8(i8),
    // A 16 bit signed integer
    Int16(i16),
    // A 32 bit signed integer
    Int32(i32),
    // A 64 bit signed integer
    Int64(i64),
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
                let offset = FixedOffset::east_opt(8 * 3600).unwrap();
                let dt_utc8 = utc_dt.with_timezone(&offset);

                dt_utc8.format("%Y-%m-%d %H:%M:%S").to_string()
            }
            Value::Year(s) => s.to_string(),
            Value::Blob(s) => s.to_string(),
            Value::Bit(s) => s.to_string(),
            Value::None => "null".to_string(),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    // READ,
    CREATE,
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
    // pub column_info: Vec<ColumnInfoVo>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfoVo {
    pub column_name: String,
    pub value_for_type: Value,
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
