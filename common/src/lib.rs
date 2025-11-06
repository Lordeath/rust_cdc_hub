use async_trait::async_trait;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use serde::de::Visitor;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Value {
    pub fn to_string(&self) -> String {
        match self {
            Value::String(s) => s.to_string(),
            Value::Tiny(s) => s.to_string(),
            Value::Short(s) => s.to_string(),
            Value::Long(s) => s.to_string(),
            Value::LongLong(s) => s.to_string(),
            Value::Float(s) => s.to_string(),
            Value::Double(s) => s.to_string(),
            Value::Decimal(s) => s.to_string(),
            Value::Time(s) => s.to_string(),
            Value::Date(s) => s.to_string(),
            Value::DateTime(s) => s.to_string(),
            Value::Timestamp(s) => s.to_string(),
            Value::Year(s) => s.to_string(),
            Value::Blob(s) => s.to_string(),
            Value::Bit(s) => s.to_string(),
            _ => "null".to_string(),
        }
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
            | Value::Blob(s) => serializer.serialize_str(s),
            Value::Tiny(v) => serializer.serialize_i8(*v),
            Value::Short(v) => serializer.serialize_i16(*v),
            Value::Long(v) => serializer.serialize_i32(*v),
            Value::LongLong(v) => serializer.serialize_i64(*v),
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

            fn visit_none<E>(self) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::None)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::String(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::String(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::LongLong(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::Bit(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> where E: de::Error {
                Ok(Value::Double(v))
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
