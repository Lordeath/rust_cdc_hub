use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

#[async_trait]
pub trait Source: Send + Sync {
    async fn start(
        &self,
        sink: Arc<tokio::sync::Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// Sink trait: 所有目标端都要实现它
#[async_trait]
pub trait Sink: Send + Sync {
    /// 连接目标端（如 Kafka、文件、数据库）
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// 写入一条数据（可以是 json 或结构化 map）
    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// 刷新缓冲区（可选）
    async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    // 未来可扩展：Postgres, Kafka, Mongo, 等
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    pub source_type: SourceType,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}

impl CdcConfig {
    pub fn get(&self, key: &str) -> String {
        match self.config.get(key) {
            None => "".to_string(),
            Some(v) => v.clone(),
        }
    }
    pub fn get_u64(&self, key: &str) -> u64 {
        let value = self.get(key);
        value.parse::<u64>().unwrap_or_else(|_| 0)
    }
    pub fn get_u32(&self, key: &str) -> u32 {
        let value = self.get(key);
        value.parse::<u32>().unwrap_or_else(|_| 0)
    }
}

#[derive(Debug, Clone)]
pub struct DataBuffer {
    // pub db: String,
    // pub table: String,
    pub before: HashMap<String, Value>,
    pub after: HashMap<String, Value>,
    pub op: Operation,
    // pub binlog_file: String,
    // pub binlog_position: u64,
    // pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum Value {
    // Str(String),
    // Num(i64),
    // Bool(bool),
    None,
    // A 8 bit signed integer
    Tiny(i8),
    // A 16 bit signed integer
    Short(i16),
    // A 32 bit signed integer
    Long(i32),
    // A 64 bit signed integer
    LongLong(i64),
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

#[derive(Debug, Clone)]
pub enum Operation {
    READ,
    CREATE,
    UPDATE,
    DELETE,
    TRUNCATE,
    MESSAGE,
    OTHER,
}
