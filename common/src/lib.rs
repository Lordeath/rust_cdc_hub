pub mod case_insensitive_hash_map;
pub mod checkpoint_manager;
pub mod custom_error;
pub mod metrics;
pub mod mysql_checkpoint;
pub mod runtime_progress;
pub mod schema;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, process};
use strum_macros::Display;
use tokio::sync::Mutex;
use tracing::{error, info, trace};

use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::types::BigDecimal;
use sqlx::{Column, Row, ValueRef};
use sqlx::{MySql, Pool, TypeInfo};

use crate::case_insensitive_hash_map::CaseInsensitiveHashMap;
use crate::custom_error::CustomError;
use crate::mysql_checkpoint::MysqlCheckPointDetailEntity;
use serde_json::Value as JsonValue;

#[async_trait]
pub trait Source: Send + Sync {
    async fn start(&mut self, sink: Arc<Mutex<dyn Sink + Send + Sync>>) -> Result<(), CustomError>;

    async fn add_plugins(&mut self, plugin: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>);

    async fn get_table_info(&mut self) -> Vec<TableInfoVo>;

    /// 关闭source并释放资源（如连接池）
    async fn close(&mut self) {}
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

    async fn after_initialization(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// 关闭sink并释放资源（如连接池）
    async fn close(&mut self) {}
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
    Dameng,
    // 未来可扩展：Postgres, Kafka, Mongo, 等
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum PluginType {
    #[strum(serialize = "column_in")]
    ColumnIn,
    #[strum(serialize = "plus")]
    Plus,
    // Modified By Codex 20260508 标识数据库拆分控制插件，不参与事件转换链路
    DatabaseSplit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    pub source_type: SourceType,
    pub sink_type: SinkType,
    pub source_config: Vec<HashMap<String, String>>,
    pub sink_config: Vec<HashMap<String, String>>,
    pub multi_mode: Option<MultiModeConfig>,
    pub auto_create_database: Option<bool>,
    pub auto_create_table: Option<bool>,
    pub auto_add_column: Option<bool>,
    pub auto_modify_column: Option<bool>,
    pub sync_foreign_key_tables: Option<bool>,
    pub sync_no_pk_table_schema: Option<bool>,
    #[serde(alias = "sync_stored_procedures")]
    pub sync_stored_procedure: Option<bool>,
    #[serde(alias = "overwrite_stored_procedures")]
    pub overwrite_stored_procedure: Option<bool>,
    pub random_check_data_after_init: Option<bool>,
    pub random_check_data_after_init_batch_size_min: Option<usize>,
    pub plugins: Option<Vec<PluginConfig>>,
    pub source_batch_size: Option<usize>,
    pub sink_batch_size: Option<usize>,
    pub checkpoint_file_path: Option<String>,
    pub log_level: Option<String>,
    pub enable_ui: Option<bool>,
    pub ui_bind: Option<String>,
    pub ui_port: Option<u16>,
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

    pub fn multi_mode_open(&self) -> bool {
        self.multi_mode
            .as_ref()
            .map(|multi_mode| multi_mode.open.unwrap_or(false))
            .unwrap_or(false)
    }

    pub fn multi_mode_init_parallelism(&self) -> usize {
        self.multi_mode
            .as_ref()
            .and_then(|multi_mode| multi_mode.init_parallelism)
            .unwrap_or(4)
            .max(1)
    }

    pub fn multi_mode_route_map(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        if let Some(multi_mode) = &self.multi_mode {
            for route in &multi_mode.database_route {
                result.insert(
                    route.source.to_ascii_lowercase(),
                    route.sink.trim().to_string(),
                );
            }
        }
        result
    }

    pub fn sync_stored_procedure_enabled(&self) -> bool {
        self.sync_stored_procedure.unwrap_or(false)
    }

    pub fn sync_no_pk_table_schema_enabled(&self) -> bool {
        self.sync_no_pk_table_schema.unwrap_or(true)
    }

    pub fn sync_foreign_key_tables_enabled(&self) -> bool {
        self.sync_foreign_key_tables.unwrap_or(true)
    }

    pub fn overwrite_stored_procedure_enabled(&self) -> bool {
        self.overwrite_stored_procedure.unwrap_or(false)
    }

    pub fn random_check_data_after_init_enabled(&self) -> bool {
        self.random_check_data_after_init.unwrap_or(false)
    }

    pub fn random_check_data_after_init_batch_size_min(&self) -> usize {
        self.random_check_data_after_init_batch_size_min
            .unwrap_or(10)
            .max(1)
    }

    pub fn target_database_for_source(&self, source_database: &str) -> String {
        if self.multi_mode_open() {
            return self
                .multi_mode_route_map()
                .get(&source_database.to_ascii_lowercase())
                .cloned()
                .unwrap_or_else(|| {
                    panic!(
                        "multi_mode database_route missing source database: {}",
                        source_database
                    )
                });
        }
        self.sink_databases()
            .first()
            .cloned()
            .unwrap_or_else(|| self.first_sink("database"))
    }

    pub fn split_csv_value(value: &str) -> Vec<String> {
        value
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn source_databases(&self) -> Vec<String> {
        if self.multi_mode_open() {
            return Self::split_csv_value(self.first_source("database").as_str());
        }
        self.source_config
            .iter()
            .map(|item| item.get("database").cloned().unwrap_or_default())
            .collect()
    }

    pub fn sink_databases(&self) -> Vec<String> {
        let sink_database = if matches!(self.sink_type, SinkType::Dameng) {
            let schema = self.first_sink("schema");
            if schema.trim().is_empty() {
                self.first_sink("database")
            } else {
                schema
            }
        } else {
            self.first_sink("database")
        };
        if self.multi_mode_open() {
            return Self::split_csv_value(sink_database.as_str());
        }
        vec![sink_database]
    }

    pub fn validate_multi_mode(&self) -> Result<(), String> {
        if !self.multi_mode_open() {
            return Ok(());
        }
        if !matches!(self.source_type, SourceType::MySQL) {
            return Err("multi_mode 当前只支持 MySQL source".to_string());
        }
        if !matches!(self.sink_type, SinkType::MySQL | SinkType::Dameng) {
            return Err("multi_mode 当前只支持 MySQL -> MySQL/Dameng".to_string());
        }
        if self.source_config.len() != 1 {
            return Err(
                "multi_mode 第一版只支持单个 source_config，通过 database 逗号分隔多个源库"
                    .to_string(),
            );
        }
        if self.sink_config.len() != 1 {
            return Err(
                "multi_mode 只支持单个 sink_config，通过 database/schema 逗号分隔多个目标库"
                    .to_string(),
            );
        }
        let source_databases = self.source_databases();
        if source_databases.is_empty() {
            return Err("multi_mode.source_config[0].database 不能为空".to_string());
        }
        ensure_no_duplicate_values("multi_mode source database", &source_databases)?;
        let sink_databases = self.sink_databases();
        if sink_databases.is_empty() {
            return Err("multi_mode.sink_config[0].database 不能为空".to_string());
        }
        ensure_no_duplicate_values("multi_mode sink database", &sink_databases)?;
        let source_set = lower_set(&source_databases);
        let sink_set = lower_set(&sink_databases);
        let multi_mode = self
            .multi_mode
            .as_ref()
            .ok_or_else(|| "multi_mode 配置缺失".to_string())?;
        if multi_mode.database_route.is_empty() {
            return Err("multi_mode.database_route 必须覆盖所有源库".to_string());
        }
        let mut route_sources = HashSet::new();
        for route in &multi_mode.database_route {
            let source = route.source.trim();
            let sink = route.sink.trim();
            if source.is_empty() || sink.is_empty() {
                return Err("multi_mode.database_route source/sink 不能为空".to_string());
            }
            let source_key = source.to_ascii_lowercase();
            if !source_set.contains(&source_key) {
                return Err(format!(
                    "multi_mode.database_route source 不在 source_config.database 中: {}",
                    source
                ));
            }
            if !route_sources.insert(source_key.clone()) {
                return Err(format!("multi_mode.database_route source 重复: {}", source));
            }
            if !sink_set.contains(&sink.to_ascii_lowercase()) {
                return Err(format!(
                    "multi_mode.database_route sink 不在 sink_config.database 清单中: {}",
                    sink
                ));
            }
        }
        for source in source_databases {
            if !route_sources.contains(&source.to_ascii_lowercase()) {
                return Err(format!("multi_mode.database_route 未覆盖源库: {}", source));
            }
        }
        if multi_mode.init_parallelism == Some(0) {
            return Err("multi_mode.init_parallelism 必须大于 0".to_string());
        }
        Ok(())
    }
}

fn lower_set(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect()
}

fn ensure_no_duplicate_values(label: &str, values: &[String]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for value in values {
        let key = value.to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(format!("{} 重复: {}", label, value));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MultiModeConfig {
    pub open: Option<bool>,
    pub init_parallelism: Option<usize>,
    #[serde(default)]
    pub database_route: Vec<DatabaseRoute>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct DatabaseRoute {
    pub source: String,
    pub sink: String,
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
    pub source_database: String,
    pub target_database: String,
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
        DataBuffer::new_with_route(
            "".to_string(),
            "".to_string(),
            table_name,
            before,
            after,
            op,
            binlog_filename,
            timestamp,
            next_event_position,
        )
    }

    pub fn new_with_route(
        source_database: String,
        target_database: String,
        table_name: String,
        before: CaseInsensitiveHashMap,
        after: CaseInsensitiveHashMap,
        op: Operation,
        binlog_filename: String,
        timestamp: u32,
        next_event_position: u32,
    ) -> DataBuffer {
        DataBuffer {
            source_database,
            target_database,
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
        DataBuffer::new_with_route(
            self.source_database.clone(),
            self.target_database.clone(),
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
        DataBuffer::new_with_route(
            self.source_database.clone(),
            self.target_database.clone(),
            self.table_name.clone(),
            self.before.clone(),
            data,
            self.op.clone(),
            self.binlog_filename.clone(),
            self.timestamp,
            self.next_event_position,
        )
    }

    pub fn source_table_key(&self) -> String {
        database_table_key(self.source_database.as_str(), self.table_name.as_str())
    }

    pub fn target_table_key(&self) -> String {
        database_table_key(self.target_database.as_str(), self.table_name.as_str())
    }
}

pub fn database_table_key(database: &str, table_name: &str) -> String {
    if database.trim().is_empty() {
        table_name.to_string()
    } else {
        format!("{}.{}", database, table_name)
    }
}

#[derive(Debug)]
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
    UnsignedInt16(u16),
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
    Blob(Vec<u8>),
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
            Value::UnsignedInt16(s) => s.to_string(),
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
            Value::Blob(v) => Self::bytes_to_hex(v),
            Value::Json(s) => s.to_string(),
            Value::Bit(s) => s.to_string(),
            Value::None => "null".to_string(),
        }
    }

    fn bytes_to_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut result = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            result.push(HEX[(b >> 4) as usize] as char);
            result.push(HEX[(b & 0x0f) as usize] as char);
        }
        result
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
            | Value::Json(s) => serializer.serialize_str(s.as_str()),
            Value::Blob(v) => serializer.serialize_str(Self::bytes_to_hex(v).as_str()),
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
            Value::UnsignedInt16(v) => serializer.serialize_u16(*v),
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
    #[serde(default)]
    pub source_database: String,
    #[serde(default)]
    pub target_database: String,
    pub table_name: String,
    pub pk_column: String,
    pub create_table_sql: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForeignKeyInfo {
    pub constraint_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub referenced_table_name: String,
    pub referenced_columns: Vec<String>,
    pub update_rule: Option<String>,
    pub delete_rule: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfoVo {
    pub column_name: String,
    pub value_for_type: Value,
}

#[inline]
fn is_mysql_string_type(type_name: &str) -> bool {
    type_name.eq_ignore_ascii_case("CHAR")
        || type_name.eq_ignore_ascii_case("VARCHAR")
        || type_name.eq_ignore_ascii_case("TEXT")
}

#[inline]
fn is_mysql_binary_type(type_name: &str) -> bool {
    type_name.eq_ignore_ascii_case("BINARY")
        || type_name.eq_ignore_ascii_case("VARBINARY")
        || type_name.eq_ignore_ascii_case("BLOB")
        || type_name.eq_ignore_ascii_case("TINYBLOB")
        || type_name.eq_ignore_ascii_case("MEDIUMBLOB")
        || type_name.eq_ignore_ascii_case("LONGBLOB")
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
                "BOOLEAN" => match row
                    .try_get::<i8, _>(name.as_str())
                    .or_else(|_| row.try_get::<bool, _>(name.as_str()).map(i8::from))
                {
                    Ok(v) => Value::Int8(v),
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
                "SMALLINT" => match row.try_get::<i16, _>(name.as_str()) {
                    Ok(v) => Value::Int16(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "SMALLINT UNSIGNED" => match row.try_get::<u16, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt16(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "MEDIUMINT" => match row.try_get::<i32, _>(name.as_str()) {
                    Ok(v) => Value::Int32(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "MEDIUMINT UNSIGNED" => match row.try_get::<u32, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt32(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "YEAR" => match row.try_get::<u16, _>(name.as_str()) {
                    Ok(v) => Value::Year(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BIT" => match row.try_get::<u64, _>(name.as_str()) {
                    Ok(v) => Value::Bit(v),
                    Err(e) => match row.try_get::<bool, _>(name.as_str()) {
                        Ok(v) => Value::Bit(u64::from(v)),
                        Err(bool_err) => {
                            error!("类型转换失败: {}", column.type_info().name());
                            error!("{}", e);
                            error!("{}", bool_err);
                            panic!("类型转换失败: {}", column.type_info().name());
                        }
                    },
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
                type_name if is_mysql_string_type(type_name) => {
                    match row.try_get::<String, _>(name.as_str()) {
                        Ok(v) => Value::String(v),
                        Err(e) => {
                            error!("类型转换失败: {}", column.type_info().name());
                            error!("{}", e);
                            panic!("类型转换失败: {}", column.type_info().name());
                        }
                    }
                }
                "JSON" => match row.try_get::<JsonValue, &str>(name.as_str()) {
                    Ok(v) => Value::Json(v.to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "FLOAT" => match row.try_get::<f32, _>(name.as_str()) {
                    Ok(v) => Value::Float(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DOUBLE" => match row.try_get::<f64, _>(name.as_str()) {
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
                type_name if is_mysql_binary_type(type_name) => {
                    match row.try_get::<Vec<u8>, _>(name.as_str()) {
                        Ok(v) => Value::Blob(v),
                        Err(e) => {
                            error!("类型转换失败: {}", column.type_info().name());
                            error!("{}", e);
                            panic!("类型转换失败: {}", column.type_info().name());
                        }
                    }
                }
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

pub fn mysql_row_text_value(row: &MySqlRow, column_name: &str) -> String {
    row.try_get::<String, _>(column_name)
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>(column_name)
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        })
        .unwrap_or_else(|e| panic!("读取MySQL文本列失败: {} {}", column_name, e))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlRoutineKind {
    Procedure,
    Function,
}

impl MySqlRoutineKind {
    pub fn routine_type(self) -> &'static str {
        match self {
            MySqlRoutineKind::Procedure => "PROCEDURE",
            MySqlRoutineKind::Function => "FUNCTION",
        }
    }

    pub fn create_row_name_column(self) -> &'static str {
        match self {
            MySqlRoutineKind::Procedure => "Procedure",
            MySqlRoutineKind::Function => "Function",
        }
    }

    pub fn create_row_sql_column(self) -> &'static str {
        match self {
            MySqlRoutineKind::Procedure => "Create Procedure",
            MySqlRoutineKind::Function => "Create Function",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MySqlRoutineDefinition {
    pub kind: MySqlRoutineKind,
    pub name: String,
    pub create_sql: String,
    pub sql_mode: String,
}

pub fn quote_mysql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

pub fn qualified_mysql_name(database: &str, name: &str) -> String {
    format!(
        "{}.{}",
        quote_mysql_identifier(database),
        quote_mysql_identifier(name)
    )
}

pub fn show_create_mysql_routine_sql(
    source_database: &str,
    routine_kind: MySqlRoutineKind,
    routine_name: &str,
) -> String {
    format!(
        "SHOW CREATE {} {}",
        routine_kind.routine_type(),
        qualified_mysql_name(source_database, routine_name)
    )
}

pub fn mysql_utf8mb4_string_expr(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut sql = String::with_capacity("CONVERT(0x USING utf8mb4)".len() + value.len() * 2);
    sql.push_str("CONVERT(0x");
    for byte in value.as_bytes() {
        sql.push(HEX[(byte >> 4) as usize] as char);
        sql.push(HEX[(byte & 0x0F) as usize] as char);
    }
    sql.push_str(" USING utf8mb4)");
    sql
}

pub async fn fetch_mysql_routines(
    source_pool: &Pool<MySql>,
    source_database: &str,
    routine_kinds: &[MySqlRoutineKind],
) -> Result<Vec<MySqlRoutineDefinition>, String> {
    if routine_kinds.is_empty() {
        return Ok(Vec::new());
    }
    let routine_types = routine_kinds
        .iter()
        .map(|kind| format!("'{}'", kind.routine_type()))
        .collect::<Vec<_>>()
        .join(", ");
    let routine_sql = format!(
        r#"
        SELECT ROUTINE_NAME, ROUTINE_TYPE
        FROM information_schema.ROUTINES
        WHERE ROUTINE_SCHEMA = {}
          AND ROUTINE_TYPE IN ({})
        ORDER BY FIELD(ROUTINE_TYPE, 'FUNCTION', 'PROCEDURE'), ROUTINE_NAME
        "#,
        mysql_utf8mb4_string_expr(source_database),
        routine_types
    );
    let routine_rows = sqlx::raw_sql(routine_sql.as_str())
        .fetch_all(source_pool)
        .await
        .map_err(|e| format!("{}: {}", routine_sql, e))?;

    let mut routines = Vec::with_capacity(routine_rows.len());
    for row in routine_rows {
        let name = mysql_row_text_value(&row, "ROUTINE_NAME");
        let routine_type = mysql_row_text_value(&row, "ROUTINE_TYPE");
        let kind = parse_mysql_routine_kind(routine_type.as_str())?;
        let show_sql = show_create_mysql_routine_sql(source_database, kind, name.as_str());
        let create_row = sqlx::raw_sql(show_sql.as_str())
            .fetch_one(source_pool)
            .await
            .map_err(|e| format!("{}: {}", show_sql, e))?;
        routines.push(parse_show_create_mysql_routine_row(
            kind,
            name.as_str(),
            &create_row,
        )?);
    }
    Ok(routines)
}

fn parse_mysql_routine_kind(value: &str) -> Result<MySqlRoutineKind, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "PROCEDURE" => Ok(MySqlRoutineKind::Procedure),
        "FUNCTION" => Ok(MySqlRoutineKind::Function),
        _ => Err(format!("unsupported MySQL routine type: {}", value)),
    }
}

fn parse_show_create_mysql_routine_row(
    kind: MySqlRoutineKind,
    fallback_name: &str,
    row: &MySqlRow,
) -> Result<MySqlRoutineDefinition, String> {
    let name = row
        .try_get::<String, _>(kind.create_row_name_column())
        .or_else(|_| row.try_get::<String, _>(0))
        .unwrap_or_else(|_| fallback_name.to_string());
    let sql_mode = row
        .try_get::<String, _>("sql_mode")
        .or_else(|_| row.try_get::<String, _>(1))
        .unwrap_or_default();
    let create_sql = row
        .try_get::<String, _>(kind.create_row_sql_column())
        .or_else(|_| row.try_get::<String, _>(2))
        .map_err(|e| {
            format!(
                "SHOW CREATE {} 读取 {} 失败: {}",
                kind.routine_type(),
                kind.create_row_sql_column(),
                e
            )
        })?;
    Ok(MySqlRoutineDefinition {
        kind,
        name,
        create_sql: strip_create_routine_definer(create_sql.as_str(), kind),
        sql_mode,
    })
}

pub fn strip_create_routine_definer(create_sql: &str, routine_kind: MySqlRoutineKind) -> String {
    let sql = create_sql.trim_start();
    if !starts_with_keyword(sql, 0, "CREATE") {
        return create_sql.to_string();
    }
    let mut pos = skip_ascii_whitespace(sql, "CREATE".len());
    if !starts_with_keyword(sql, pos, "DEFINER") {
        return create_sql.to_string();
    }
    pos = skip_ascii_whitespace(sql, pos + "DEFINER".len());
    if sql.as_bytes().get(pos) != Some(&b'=') {
        return create_sql.to_string();
    }
    let Some(routine_keyword_pos) =
        find_keyword_outside_quotes(sql, pos + 1, routine_kind.routine_type())
    else {
        return create_sql.to_string();
    };
    format!("CREATE {}", sql[routine_keyword_pos..].trim_start())
}

fn skip_ascii_whitespace(value: &str, mut pos: usize) -> usize {
    while value
        .as_bytes()
        .get(pos)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        pos += 1;
    }
    pos
}

fn starts_with_keyword(value: &str, pos: usize, keyword: &str) -> bool {
    let bytes = value.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    if pos + keyword_bytes.len() > bytes.len() {
        return false;
    }
    if !bytes[pos..pos + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes) {
        return false;
    }
    let before_ok = pos == 0 || !is_ascii_identifier_byte(bytes[pos - 1]);
    let after_pos = pos + keyword_bytes.len();
    let after_ok = after_pos >= bytes.len() || !is_ascii_identifier_byte(bytes[after_pos]);
    before_ok && after_ok
}

fn find_keyword_outside_quotes(value: &str, start: usize, keyword: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut pos = start;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if let Some(quote_byte) = quote {
            if byte == quote_byte {
                if bytes.get(pos + 1) == Some(&quote_byte) {
                    pos += 2;
                } else {
                    quote = None;
                    pos += 1;
                }
            } else {
                pos += 1;
            }
            continue;
        }
        if matches!(byte, b'`' | b'\'' | b'"') {
            quote = Some(byte);
            pos += 1;
            continue;
        }
        if starts_with_keyword(value, pos, keyword) {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

fn is_ascii_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub async fn get_mysql_pool_by_url(
    connection_url: &str,
    from: &str,
) -> Result<Pool<MySql>, String> {
    get_mysql_pool_by_url_with_max_connections(connection_url, from, 10).await
}

pub async fn get_mysql_pool_by_url_with_max_connections(
    connection_url: &str,
    from: &str,
    max_connections: u32,
) -> Result<Pool<MySql>, String> {
    let redacted_connection_url = redact_connection_url_password(connection_url);
    info!(
        "Connecting to MySQL: {} from: {} max_connections: {}",
        redacted_connection_url, from, max_connections
    );
    match MySqlPoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(Duration::from_secs(10))
        .test_before_acquire(true)
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
            let error_detail = error_chain_message(&e);
            error!(
                "Failed to connect to MySQL from: {} url: {} error: {}",
                from, redacted_connection_url, error_detail
            );
            if is_likely_mysql_tls_error(&error_detail) {
                error!(
                    "MySQL TLS握手失败 from: {}。如果该连接不需要SSL，可在对应的source_config/sink_config中添加 ssl_mode: disabled；如果必须使用SSL，请检查MySQL TLS版本、证书和ssl_ca配置。",
                    from
                );
            }
            Err(format!(
                "MySQL连接失败 from: {} url: {} error: {}",
                from, redacted_connection_url, error_detail
            ))
        }
    }
}

const MYSQL_URL_OPTION_KEYS: &[(&str, &str)] = &[
    ("ssl_mode", "ssl-mode"),
    ("ssl-mode", "ssl-mode"),
    ("sslmode", "ssl-mode"),
    ("ssl_ca", "ssl-ca"),
    ("ssl-ca", "ssl-ca"),
    ("sslca", "ssl-ca"),
    ("ssl_cert", "ssl-cert"),
    ("ssl-cert", "ssl-cert"),
    ("sslcert", "ssl-cert"),
    ("ssl_key", "ssl-key"),
    ("ssl-key", "ssl-key"),
    ("sslkey", "ssl-key"),
    ("charset", "charset"),
    ("collation", "collation"),
    ("statement_cache_capacity", "statement-cache-capacity"),
    ("statement-cache-capacity", "statement-cache-capacity"),
];
const DEFAULT_MYSQL_SSL_MODE: &str = "disabled";

pub fn mysql_connection_url_from_config(
    config: &HashMap<String, String>,
    database: Option<&str>,
) -> String {
    let username = config.get("username").map(String::as_str).unwrap_or("");
    let password = config.get("password").map(String::as_str).unwrap_or("");
    let host = config.get("host").map(String::as_str).unwrap_or("");
    let port = config.get("port").map(String::as_str).unwrap_or("");
    let mut connection_url = format!("mysql://{}:{}@{}:{}", username, password, host, port);
    if let Some(database) = database.filter(|database| !database.is_empty()) {
        connection_url.push('/');
        connection_url.push_str(database);
    }

    let mysql_options = mysql_url_options_from_config(config);
    if !mysql_options.is_empty() {
        connection_url.push('?');
        connection_url.push_str(
            &mysql_options
                .into_iter()
                .map(|(key, value)| format!("{}={}", key, percent_encode_query_component(value)))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    connection_url
}

fn mysql_url_options_from_config(config: &HashMap<String, String>) -> Vec<(&'static str, &str)> {
    let mut result = Vec::new();
    let mut seen_url_keys: Vec<&'static str> = Vec::new();
    for (config_key, url_key) in MYSQL_URL_OPTION_KEYS {
        let Some(value) = config.get(*config_key).map(String::as_str) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() || seen_url_keys.contains(url_key) {
            continue;
        }
        seen_url_keys.push(*url_key);
        result.push((*url_key, value));
    }
    if !seen_url_keys.contains(&"ssl-mode") {
        result.insert(0, ("ssl-mode", DEFAULT_MYSQL_SSL_MODE));
    }
    result
}

fn percent_encode_query_component(value: &str) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            result.push(byte as char);
        } else {
            result.push_str(&format!("%{:02X}", byte));
        }
    }
    result
}

fn error_chain_message(error: &(dyn Error + 'static)) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        let message = error.to_string();
        if !messages.iter().any(|item| item == &message) {
            messages.push(message);
        }
        source = error.source();
    }
    messages.join(": ")
}

fn is_likely_mysql_tls_error(error_detail: &str) -> bool {
    let lower_error = error_detail.to_ascii_lowercase();
    lower_error.contains("handshakefailure")
        || lower_error.contains("handshake failure")
        || lower_error.contains("tls")
        || lower_error.contains("ssl")
        || lower_error.contains("certificate")
}

pub fn redact_connection_url_password(connection_url: &str) -> String {
    let Some(scheme_end) = connection_url.find("://") else {
        return connection_url.to_string();
    };
    let authority_start = scheme_end + 3;
    let rest = &connection_url[authority_start..];
    let authority_end = rest
        .find(|c| matches!(c, '/' | '?' | '#'))
        .map(|idx| authority_start + idx)
        .unwrap_or(connection_url.len());
    let authority = &connection_url[authority_start..authority_end];
    let Some(at_pos_rel) = authority.rfind('@') else {
        return connection_url.to_string();
    };
    let userinfo = &authority[..at_pos_rel];
    let Some(colon_pos_rel) = userinfo.find(':') else {
        return connection_url.to_string();
    };

    let password_start = authority_start + colon_pos_rel + 1;
    let password_end = authority_start + at_pos_rel;
    format!(
        "{}******{}",
        &connection_url[..password_start],
        &connection_url[password_end..]
    )
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
    fn test_resolve_string_unsigned_int16() {
        let v = Value::UnsignedInt16(65535);
        assert_eq!(v.resolve_string(), "65535");
        assert_eq!(serde_json::to_string(&v).unwrap(), "65535");
    }

    #[test]
    fn test_resolve_string_none() {
        let v = Value::None;
        assert_eq!(v.resolve_string(), "null");
    }

    #[test]
    fn test_resolve_string_blob_as_hex() {
        let v = Value::Blob(vec![0x1f, 0x8b, 0x08, 0xff]);
        assert_eq!(v.resolve_string(), "1f8b08ff");
    }

    #[test]
    fn test_mysql_char_type_is_string() {
        assert!(is_mysql_string_type("CHAR"));
        assert!(is_mysql_string_type("VARCHAR"));
        assert!(is_mysql_string_type("TEXT"));
        assert!(is_mysql_string_type("char"));
        assert!(!is_mysql_string_type("INT"));
    }

    #[test]
    fn test_mysql_blob_type_is_binary() {
        assert!(is_mysql_binary_type("BLOB"));
        assert!(is_mysql_binary_type("LONGBLOB"));
        assert!(is_mysql_binary_type("VARBINARY"));
        assert!(is_mysql_binary_type("binary"));
        assert!(!is_mysql_binary_type("VARCHAR"));
    }

    #[test]
    fn mysql_routine_sql_quotes_database_and_name() {
        assert_eq!(
            show_create_mysql_routine_sql("source-db", MySqlRoutineKind::Function, "sync`demo"),
            "SHOW CREATE FUNCTION `source-db`.`sync``demo`"
        );
        assert_eq!(
            mysql_utf8mb4_string_expr("newsee-backlog"),
            "CONVERT(0x6E65777365652D6261636B6C6F67 USING utf8mb4)"
        );
        assert_eq!(mysql_utf8mb4_string_expr(""), "''");
    }

    #[test]
    fn strips_create_function_definer() {
        let sql = "CREATE DEFINER=`source_user`@`%` FUNCTION `sync_demo`() RETURNS int DETERMINISTIC BEGIN RETURN 1; END";

        assert_eq!(
            strip_create_routine_definer(sql, MySqlRoutineKind::Function),
            "CREATE FUNCTION `sync_demo`() RETURNS int DETERMINISTIC BEGIN RETURN 1; END"
        );
    }

    #[test]
    fn test_redact_connection_url_password() {
        assert_eq!(
            redact_connection_url_password("mysql://user:secret@127.0.0.1:3306/demo"),
            "mysql://user:******@127.0.0.1:3306/demo"
        );
    }

    #[test]
    fn test_redact_connection_url_password_preserves_suffix() {
        assert_eq!(
            redact_connection_url_password(
                "mysql://user:secret@127.0.0.1:3306/demo?ssl-mode=required"
            ),
            "mysql://user:******@127.0.0.1:3306/demo?ssl-mode=required"
        );
    }

    #[test]
    fn test_redact_connection_url_password_without_password() {
        assert_eq!(
            redact_connection_url_password("mysql://user@127.0.0.1:3306/demo"),
            "mysql://user@127.0.0.1:3306/demo"
        );
        assert_eq!(
            redact_connection_url_password("mysql://127.0.0.1:3306/demo"),
            "mysql://127.0.0.1:3306/demo"
        );
    }

    #[test]
    fn test_mysql_connection_url_from_config_appends_ssl_mode() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), "127.0.0.1".to_string());
        config.insert("port".to_string(), "3306".to_string());
        config.insert("username".to_string(), "user".to_string());
        config.insert("password".to_string(), "secret".to_string());
        config.insert("ssl_mode".to_string(), "disabled".to_string());

        assert_eq!(
            mysql_connection_url_from_config(&config, Some("demo")),
            "mysql://user:secret@127.0.0.1:3306/demo?ssl-mode=disabled"
        );
    }

    #[test]
    fn test_mysql_connection_url_from_config_defaults_ssl_mode_disabled() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), "127.0.0.1".to_string());
        config.insert("port".to_string(), "3306".to_string());
        config.insert("username".to_string(), "user".to_string());
        config.insert("password".to_string(), "secret".to_string());

        assert_eq!(
            mysql_connection_url_from_config(&config, Some("demo")),
            "mysql://user:secret@127.0.0.1:3306/demo?ssl-mode=disabled"
        );
    }

    #[test]
    fn test_mysql_connection_url_from_config_without_database() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), "127.0.0.1".to_string());
        config.insert("port".to_string(), "3306".to_string());
        config.insert("username".to_string(), "user".to_string());
        config.insert("password".to_string(), "secret".to_string());
        config.insert("ssl-mode".to_string(), "required".to_string());

        assert_eq!(
            mysql_connection_url_from_config(&config, None),
            "mysql://user:secret@127.0.0.1:3306?ssl-mode=required"
        );
    }

    #[test]
    fn test_mysql_connection_url_from_config_encodes_option_values() {
        let mut config = HashMap::new();
        config.insert("host".to_string(), "127.0.0.1".to_string());
        config.insert("port".to_string(), "3306".to_string());
        config.insert("username".to_string(), "user".to_string());
        config.insert("password".to_string(), "secret".to_string());
        config.insert("ssl_ca".to_string(), "/tmp/mysql ca.pem".to_string());

        assert_eq!(
            mysql_connection_url_from_config(&config, Some("demo")),
            "mysql://user:secret@127.0.0.1:3306/demo?ssl-mode=disabled&ssl-ca=%2Ftmp%2Fmysql%20ca.pem"
        );
    }

    fn multi_mode_config() -> CdcConfig {
        let mut source = HashMap::new();
        source.insert("database".to_string(), "src_a,src_b".to_string());
        let mut sink = HashMap::new();
        sink.insert("database".to_string(), "dst_a,dst_b".to_string());
        CdcConfig {
            source_type: SourceType::MySQL,
            sink_type: SinkType::MySQL,
            source_config: vec![source],
            sink_config: vec![sink],
            multi_mode: Some(MultiModeConfig {
                open: Some(true),
                init_parallelism: Some(4),
                database_route: vec![
                    DatabaseRoute {
                        source: "src_a".to_string(),
                        sink: "dst_a".to_string(),
                    },
                    DatabaseRoute {
                        source: "src_b".to_string(),
                        sink: "dst_b".to_string(),
                    },
                ],
            }),
            auto_create_database: None,
            auto_create_table: None,
            auto_add_column: None,
            auto_modify_column: None,
            sync_foreign_key_tables: None,
            sync_no_pk_table_schema: None,
            sync_stored_procedure: None,
            overwrite_stored_procedure: None,
            random_check_data_after_init: None,
            random_check_data_after_init_batch_size_min: None,
            plugins: None,
            source_batch_size: None,
            sink_batch_size: None,
            checkpoint_file_path: None,
            log_level: None,
            enable_ui: None,
            ui_bind: None,
            ui_port: None,
        }
    }

    #[test]
    fn test_multi_mode_validate_success() {
        let config = multi_mode_config();

        assert!(config.validate_multi_mode().is_ok());
        assert_eq!(
            config.source_databases(),
            vec!["src_a".to_string(), "src_b".to_string()]
        );
        assert_eq!(config.target_database_for_source("src_b"), "dst_b");
    }

    #[test]
    fn random_check_after_init_defaults_are_disabled() {
        let config: CdcConfig = serde_json::from_str(
            r#"{
                "source_type": "MySQL",
                "sink_type": "Dameng",
                "source_config": [{}],
                "sink_config": [{}]
            }"#,
        )
        .unwrap();

        assert!(!config.random_check_data_after_init_enabled());
        assert_eq!(config.random_check_data_after_init_batch_size_min(), 10);
    }

    #[test]
    fn random_check_after_init_batch_size_is_at_least_one() {
        let config: CdcConfig = serde_json::from_str(
            r#"{
                "source_type": "MySQL",
                "sink_type": "Dameng",
                "source_config": [{}],
                "sink_config": [{}],
                "random_check_data_after_init": false,
                "random_check_data_after_init_batch_size_min": 0
            }"#,
        )
        .unwrap();

        assert!(!config.random_check_data_after_init_enabled());
        assert_eq!(config.random_check_data_after_init_batch_size_min(), 1);
    }

    #[test]
    fn test_multi_mode_validate_allows_dameng_sink_schema_list() {
        let mut config = multi_mode_config();
        config.sink_type = SinkType::Dameng;
        config.sink_config[0].insert("database".to_string(), "physical_db".to_string());
        config.sink_config[0].insert("schema".to_string(), "dst_a,dst_b".to_string());

        assert!(config.validate_multi_mode().is_ok());
        assert_eq!(
            config.sink_databases(),
            vec!["dst_a".to_string(), "dst_b".to_string()]
        );
    }

    #[test]
    fn test_dameng_target_database_prefers_schema_when_not_multi_mode() {
        let mut config = multi_mode_config();
        config.sink_type = SinkType::Dameng;
        config.multi_mode = None;
        config.source_config[0].insert("database".to_string(), "src".to_string());
        config.sink_config[0].insert("database".to_string(), "physical_db".to_string());
        config.sink_config[0].insert("schema".to_string(), "target_schema".to_string());

        assert_eq!(config.sink_databases(), vec!["target_schema".to_string()]);
        assert_eq!(config.target_database_for_source("src"), "target_schema");
    }

    #[test]
    fn test_stored_procedure_flags_default_false_and_parse_aliases() {
        let default_config = multi_mode_config();
        assert!(!default_config.sync_stored_procedure_enabled());
        assert!(!default_config.overwrite_stored_procedure_enabled());

        let config: CdcConfig = serde_json::from_str(
            r#"{
                "source_type": "MySQL",
                "sink_type": "MySQL",
                "source_config": [{}],
                "sink_config": [{}],
                "sync_stored_procedures": true,
                "overwrite_stored_procedures": true
            }"#,
        )
        .unwrap();

        assert!(config.sync_stored_procedure_enabled());
        assert!(config.overwrite_stored_procedure_enabled());
    }

    #[test]
    fn test_sync_no_pk_table_schema_default_true_and_parse_false() {
        let default_config = multi_mode_config();
        assert!(default_config.sync_no_pk_table_schema_enabled());

        let config: CdcConfig = serde_json::from_str(
            r#"{
                "source_type": "MySQL",
                "sink_type": "Dameng",
                "source_config": [{}],
                "sink_config": [{}],
                "sync_no_pk_table_schema": false
            }"#,
        )
        .unwrap();

        assert!(!config.sync_no_pk_table_schema_enabled());
    }

    #[test]
    fn test_sync_foreign_key_tables_default_true_and_parse_false() {
        let default_config = multi_mode_config();
        assert!(default_config.sync_foreign_key_tables_enabled());

        let config: CdcConfig = serde_json::from_str(
            r#"{
                "source_type": "MySQL",
                "sink_type": "MySQL",
                "source_config": [{}],
                "sink_config": [{}],
                "sync_foreign_key_tables": false
            }"#,
        )
        .unwrap();

        assert!(!config.sync_foreign_key_tables_enabled());
    }

    #[test]
    fn test_multi_mode_validate_rejects_unsupported_sink() {
        let mut config = multi_mode_config();
        config.sink_type = SinkType::Print;

        let err = config.validate_multi_mode().unwrap_err();

        assert!(err.contains("MySQL/Dameng"));
    }

    #[test]
    fn test_multi_mode_route_must_cover_all_sources() {
        let mut config = multi_mode_config();
        config.multi_mode.as_mut().unwrap().database_route.pop();

        let err = config.validate_multi_mode().unwrap_err();

        assert!(err.contains("未覆盖源库"));
    }

    #[test]
    fn test_multi_mode_route_sink_must_be_declared() {
        let mut config = multi_mode_config();
        config.multi_mode.as_mut().unwrap().database_route[1].sink = "dst_x".to_string();

        let err = config.validate_multi_mode().unwrap_err();

        assert!(err.contains("sink 不在 sink_config.database"));
    }
}
