use async_trait::async_trait;
use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecString,
};
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_type_token_from_column_definition,
};
use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value};
use dameng::Client;
use dameng::ToDmValue;
use dameng_types::DmValue;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace};

#[derive(Debug, Clone)]
enum DamengParam {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
}

impl ToDmValue for DamengParam {
    fn to_dm_value(&self) -> DmValue {
        match self {
            DamengParam::Null => DmValue::Null,
            DamengParam::Bool(v) => DmValue::Boolean(*v),
            DamengParam::I8(v) => DmValue::TinyInt(*v),
            DamengParam::I16(v) => DmValue::SmallInt(*v),
            DamengParam::I32(v) => DmValue::Int(*v),
            DamengParam::I64(v) => DmValue::BigInt(*v),
            DamengParam::F32(v) => DmValue::Float(*v),
            DamengParam::F64(v) => DmValue::Double(*v),
            DamengParam::Text(v) => DmValue::Text(v.clone()),
        }
    }
}

#[derive(Debug, Clone)]
struct DamengColumnInfo {
    data_type: String,
    data_length: u32,
    char_length: u32,
}

pub struct DamengSink {
    client: Mutex<Client>,
    host: String,
    port: u16,
    username: String,
    password: String,
    schema: String,
    table_info_list: Vec<TableInfoVo>,
    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,
    sink_batch_size: usize,
    auto_create_database: bool,
    auto_create_table: bool,
    auto_add_column: bool,
    auto_modify_column: bool,
    table_info_cache: Mutex<CaseInsensitiveHashMapTableInfoVo>,
    columns_cache: Mutex<CaseInsensitiveHashMapVecString>,
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
}

impl DamengSink {
    pub async fn new(config: &CdcConfig, table_info_list: Vec<TableInfoVo>) -> Self {
        let host = config.first_sink_not_blank("host");
        let port = config.first_sink("port").parse::<u16>().unwrap_or(5236);
        let username = config.first_sink_not_blank("username");
        let password = config.first_sink_not_blank("password");
        let schema = {
            let schema = config.first_sink("schema");
            if schema.is_empty() {
                config.first_sink("database")
            } else {
                schema
            }
        };

        let client = Self::connect_client(&host, port, &username, &password)
            .unwrap_or_else(|e| panic!("Failed to connect to Dameng: {}", e));

        let sink_batch_size = config.sink_batch_size.unwrap_or(256);
        let sink = DamengSink {
            client: Mutex::new(client),
            host,
            port,
            username,
            password,
            schema,
            table_info_list,
            buffer: Mutex::new(Vec::with_capacity(sink_batch_size)),
            initialized: RwLock::new(false),
            sink_batch_size,
            auto_create_database: config.auto_create_database.unwrap_or(true),
            auto_create_table: config.auto_create_table.unwrap_or(true),
            auto_add_column: config.auto_add_column.unwrap_or(true),
            auto_modify_column: config.auto_modify_column.unwrap_or(true),
            table_info_cache: Mutex::new(CaseInsensitiveHashMapTableInfoVo::new_with_no_arg()),
            columns_cache: Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg()),
            checkpoint: Mutex::new(HashMap::new()),
        };
        sink.ensure_database()
            .await
            .unwrap_or_else(|e| panic!("Dameng ensure database failed: {}", e));
        sink.ensure_schema().await;
        sink
    }

    fn connect_client(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
    ) -> Result<Client, Box<dyn Error + Send + Sync>> {
        let mut client = Client::new(host, port);
        client.connect(username, password)?;
        Ok(client)
    }

    async fn reconnect(&self) -> Result<(), String> {
        let mut client =
            Self::connect_client(&self.host, self.port, &self.username, &self.password)
                .map_err(|e| e.to_string())?;
        if !self.schema.is_empty() {
            let _ =
                client.execute(format!("SET SCHEMA {}", Self::quote_ident(&self.schema)).as_str());
        }
        *self.client.lock().await = client;
        Ok(())
    }

    async fn ensure_schema(&self) {
        for table_info in &self.table_info_list {
            if let Err(e) = self.ensure_table(table_info).await {
                error!(
                    "Dameng ensure table failed: {} {}",
                    table_info.table_name, e
                );
            }
            if let Err(e) = self.ensure_columns(table_info).await {
                error!(
                    "Dameng ensure columns failed: {} {}",
                    table_info.table_name, e
                );
            }
        }
    }

    async fn ensure_database(&self) -> Result<(), String> {
        if self.schema.is_empty() {
            return Ok(());
        }

        let set_schema_sql = Self::set_schema_sql(self.schema.as_str());
        match self.execute(set_schema_sql.as_str()).await {
            Ok(_) => return Ok(()),
            Err(e) if !self.auto_create_database => {
                return Err(format!(
                    "target schema {} is unavailable and auto_create_database is false: {}",
                    self.schema, e
                ));
            }
            Err(set_schema_error) => {
                let create_schema_sql = Self::create_schema_sql(self.schema.as_str());
                match self.execute(create_schema_sql.as_str()).await {
                    Ok(_) => {
                        info!("Dameng auto create schema success: {}", self.schema);
                    }
                    Err(create_schema_error) => match self.execute(set_schema_sql.as_str()).await {
                        Ok(_) => return Ok(()),
                        Err(retry_set_schema_error) => {
                            return Err(format!(
                                "set schema failed: {}; create schema failed: {}; retry set schema failed: {}",
                                set_schema_error, create_schema_error, retry_set_schema_error
                            ));
                        }
                    },
                }
                self.execute(set_schema_sql.as_str()).await?;
                Ok(())
            }
        }
    }

    async fn ensure_table(&self, table_info: &TableInfoVo) -> Result<(), String> {
        if !self.auto_create_table {
            return Ok(());
        }
        if self.table_exists(table_info.table_name.as_str()).await? {
            return Ok(());
        }

        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let mut cols_sql: Vec<String> = Vec::with_capacity(table_info.columns.len());
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
            let mut nullable = mysql_column_allows_null_from_definition(def.as_str());
            if src_col.eq_ignore_ascii_case(table_info.pk_column.as_str()) {
                nullable = false;
            }
            let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
            cols_sql.push(format!(
                "{} {} {}",
                Self::quote_ident(src_col),
                Self::map_mysql_type_to_dameng(mysql_type.as_str()),
                nullable_sql
            ));
        }
        if cols_sql.is_empty() {
            return Err("no columns parsed from source create table sql".to_string());
        }
        cols_sql.push(format!(
            "PRIMARY KEY ({})",
            Self::quote_ident(table_info.pk_column.as_str())
        ));
        let sql = format!(
            "CREATE TABLE {} ({})",
            self.qualified_table(table_info.table_name.as_str()),
            cols_sql.join(", ")
        );
        self.execute(sql.as_str()).await?;
        info!(
            "Dameng auto create table success: {}",
            table_info.table_name
        );
        Ok(())
    }

    async fn ensure_columns(&self, table_info: &TableInfoVo) -> Result<(), String> {
        if !self.auto_add_column && !self.auto_modify_column {
            return Ok(());
        }
        let existing_cols = self
            .existing_columns(table_info.table_name.as_str())
            .await?;
        if existing_cols.is_empty() {
            return Ok(());
        }
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
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
            let nullable = mysql_column_allows_null_from_definition(def.as_str());
            let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
            if let Some(existing_col) = existing_cols.get(key.as_str()) {
                if self.auto_modify_column
                    && Self::should_modify_existing_column(&mysql_type, existing_col)
                {
                    let sql = format!(
                        "ALTER TABLE {} MODIFY {} {} {}",
                        self.qualified_table(table_info.table_name.as_str()),
                        Self::quote_ident(src_col),
                        Self::map_mysql_type_to_dameng(mysql_type.as_str()),
                        nullable_sql
                    );
                    match self.execute(sql.as_str()).await {
                        Ok(_) => info!(
                            "Dameng auto modify column success: {} {}",
                            table_info.table_name, src_col
                        ),
                        Err(e) => error!(
                            "Dameng auto modify column failed: {} {} {}",
                            table_info.table_name, src_col, e
                        ),
                    }
                }
                continue;
            }
            if !self.auto_add_column {
                continue;
            }
            let sql = format!(
                "ALTER TABLE {} ADD {} {} {}",
                self.qualified_table(table_info.table_name.as_str()),
                Self::quote_ident(src_col),
                Self::map_mysql_type_to_dameng(mysql_type.as_str()),
                nullable_sql
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto add column success: {} {}",
                    table_info.table_name, src_col
                ),
                Err(e) => error!(
                    "Dameng auto add column failed: {} {} {}",
                    table_info.table_name, src_col, e
                ),
            }
        }
        Ok(())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, String> {
        let table_upper = table_name.to_ascii_uppercase();
        let sql = if self.schema.is_empty() {
            "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = ? OR TABLE_NAME = ?"
        } else {
            "SELECT COUNT(*) FROM ALL_TABLES WHERE (OWNER = ? OR OWNER = ?) AND (TABLE_NAME = ? OR TABLE_NAME = ?)"
        };
        let params = if self.schema.is_empty() {
            vec![
                DamengParam::Text(table_name.to_string()),
                DamengParam::Text(table_upper),
            ]
        } else {
            vec![
                DamengParam::Text(self.schema.to_string()),
                DamengParam::Text(self.schema.to_ascii_uppercase()),
                DamengParam::Text(table_name.to_string()),
                DamengParam::Text(table_upper),
            ]
        };
        let rows = self.query(sql, &params).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn existing_columns(
        &self,
        table_name: &str,
    ) -> Result<HashMap<String, DamengColumnInfo>, String> {
        let table_upper = table_name.to_ascii_uppercase();
        let sql = if self.schema.is_empty() {
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, CHAR_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? OR TABLE_NAME = ?"
        } else {
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, CHAR_LENGTH FROM ALL_TAB_COLUMNS WHERE (OWNER = ? OR OWNER = ?) AND (TABLE_NAME = ? OR TABLE_NAME = ?)"
        };
        let params = if self.schema.is_empty() {
            vec![
                DamengParam::Text(table_name.to_string()),
                DamengParam::Text(table_upper),
            ]
        } else {
            vec![
                DamengParam::Text(self.schema.to_string()),
                DamengParam::Text(self.schema.to_ascii_uppercase()),
                DamengParam::Text(table_name.to_string()),
                DamengParam::Text(table_upper),
            ]
        };
        let rows = self.query(sql, &params).await?;
        let mut cols = HashMap::new();
        for row in rows.iter() {
            if let (Ok(name), Ok(data_type)) = (row.get::<String>(0), row.get::<String>(1)) {
                cols.insert(
                    name.to_ascii_lowercase(),
                    DamengColumnInfo {
                        data_type,
                        data_length: row.get::<i32>(2).unwrap_or(0).max(0) as u32,
                        char_length: row.get::<i32>(3).unwrap_or(0).max(0) as u32,
                    },
                );
            }
        }
        Ok(cols)
    }

    async fn execute(&self, sql: &str) -> Result<u64, String> {
        let first_result = {
            let mut client = self.client.lock().await;
            client.execute(sql)
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("Dameng execute failed, retrying: {} sql: {}", e, sql);
                self.reconnect().await?;
                self.client
                    .lock()
                    .await
                    .execute(sql)
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn execute_with_params(&self, sql: &str, params: &[DamengParam]) -> Result<u64, String> {
        let first_result = {
            let mut client = self.client.lock().await;
            let refs = Self::param_refs(params);
            client.execute_with_params(sql, &refs)
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("Dameng execute failed, retrying: {} sql: {}", e, sql);
                self.reconnect().await?;
                let mut client = self.client.lock().await;
                let refs = Self::param_refs(params);
                client
                    .execute_with_params(sql, &refs)
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn query(&self, sql: &str, params: &[DamengParam]) -> Result<dameng::ResultSet, String> {
        let mut client = self.client.lock().await;
        let refs = Self::param_refs(params);
        client
            .query_with_params(sql, &refs)
            .map_err(|e| e.to_string())
    }

    fn param_refs(params: &[DamengParam]) -> Vec<&dyn ToDmValue> {
        params.iter().map(|p| p as &dyn ToDmValue).collect()
    }

    async fn get_pk_name_from_cache(&self, table_name: &str) -> String {
        self.table_info_cache
            .lock()
            .await
            .get(table_name)
            .pk_column
            .to_string()
    }

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn create_schema_sql(schema: &str) -> String {
        format!("CREATE SCHEMA {}", Self::quote_ident(schema))
    }

    fn set_schema_sql(schema: &str) -> String {
        format!("SET SCHEMA {}", Self::quote_ident(schema))
    }

    fn qualified_table(&self, table_name: &str) -> String {
        if self.schema.is_empty() {
            Self::quote_ident(table_name)
        } else {
            format!(
                "{}.{}",
                Self::quote_ident(&self.schema),
                Self::quote_ident(table_name)
            )
        }
    }

    fn value_to_param(value: &Value) -> DamengParam {
        match value {
            Value::None => DamengParam::Null,
            Value::Int8(v) => DamengParam::I8(*v),
            Value::Int16(v) => DamengParam::I16(*v),
            Value::Int32(v) => DamengParam::I32(*v),
            Value::Int64(v) => DamengParam::I64(*v),
            Value::UnsignedInt8(v) => DamengParam::I16(*v as i16),
            Value::UnsignedInt32(v) => DamengParam::I64(*v as i64),
            Value::UnsignedInt64(v) => {
                if *v <= i64::MAX as u64 {
                    DamengParam::I64(*v as i64)
                } else {
                    DamengParam::Text(v.to_string())
                }
            }
            Value::Float(v) => DamengParam::F32(*v),
            Value::Double(v) => DamengParam::F64(*v),
            Value::Bit(v) => DamengParam::Bool(*v != 0),
            Value::Decimal(v)
            | Value::Time(v)
            | Value::Date(v)
            | Value::DateTime(v)
            | Value::String(v)
            | Value::Blob(v)
            | Value::Json(v) => DamengParam::Text(v.clone()),
            Value::Timestamp(_) | Value::Year(_) => DamengParam::Text(value.resolve_string()),
        }
    }

    fn map_mysql_type_to_dameng(mysql_type_token: &str) -> String {
        let t = mysql_type_token.to_ascii_lowercase();
        if t.starts_with("tinyint(1)") || t.starts_with("boolean") || t.starts_with("bool") {
            return "BIT".to_string();
        }
        if t.starts_with("tinyint") {
            if t.contains("unsigned") {
                return "SMALLINT".to_string();
            }
            return "TINYINT".to_string();
        }
        if t.starts_with("smallint") {
            if t.contains("unsigned") {
                return "INT".to_string();
            }
            return "SMALLINT".to_string();
        }
        if t.starts_with("mediumint") || t.starts_with("int") || t.starts_with("integer") {
            if t.contains("unsigned") {
                return "BIGINT".to_string();
            }
            return "INT".to_string();
        }
        if t.starts_with("bigint") {
            if t.contains("unsigned") {
                return "DECIMAL(20,0)".to_string();
            }
            return "BIGINT".to_string();
        }
        if t.starts_with("float") {
            return "FLOAT".to_string();
        }
        if t.starts_with("double") || t.starts_with("real") {
            return "DOUBLE".to_string();
        }
        if t.starts_with("decimal") || t.starts_with("numeric") {
            return mysql_type_token.to_ascii_uppercase();
        }
        if t.starts_with("datetime") || t.starts_with("timestamp") {
            return "TIMESTAMP".to_string();
        }
        if t.starts_with("date") {
            return "DATE".to_string();
        }
        if t.starts_with("time") {
            return "TIME".to_string();
        }
        if t.starts_with("varchar") {
            return Self::map_mysql_char_type_to_dameng("VARCHAR", mysql_type_token, 8000);
        }
        if t.starts_with("char") {
            return Self::map_mysql_char_type_to_dameng("CHAR", mysql_type_token, 2000);
        }
        if t.contains("text")
            || t.starts_with("json")
            || t.starts_with("enum")
            || t.starts_with("set")
        {
            return "CLOB".to_string();
        }
        if t.contains("blob") || t.contains("binary") {
            return "BLOB".to_string();
        }
        "VARCHAR(255 CHAR)".to_string()
    }

    fn map_mysql_char_type_to_dameng(
        dameng_type: &str,
        mysql_type_token: &str,
        char_limit: u32,
    ) -> String {
        match Self::mysql_type_length(mysql_type_token) {
            Some(len) if len > char_limit => "CLOB".to_string(),
            Some(len) => format!("{}({} CHAR)", dameng_type, len),
            None => format!("{}(255 CHAR)", dameng_type),
        }
    }

    fn mysql_type_length(mysql_type_token: &str) -> Option<u32> {
        let start = mysql_type_token.find('(')? + 1;
        let end = mysql_type_token[start..].find(')')? + start;
        mysql_type_token[start..end]
            .split(',')
            .next()?
            .trim()
            .parse::<u32>()
            .ok()
    }

    fn should_modify_existing_column(
        mysql_type_token: &str,
        existing_col: &DamengColumnInfo,
    ) -> bool {
        let t = mysql_type_token.to_ascii_lowercase();
        if !t.starts_with("varchar") && !t.starts_with("char") {
            return false;
        }

        let expected_type = Self::map_mysql_type_to_dameng(mysql_type_token);
        if expected_type == "CLOB" {
            return !existing_col.data_type.eq_ignore_ascii_case("CLOB");
        }

        let expected_prefix = if t.starts_with("varchar") {
            "VARCHAR"
        } else {
            "CHAR"
        };
        if !existing_col
            .data_type
            .to_ascii_uppercase()
            .starts_with(expected_prefix)
        {
            return true;
        }

        match Self::mysql_type_length(mysql_type_token) {
            Some(expected_chars) => {
                existing_col.char_length < expected_chars
                    || existing_col.data_length <= existing_col.char_length
            }
            None => existing_col.data_length <= existing_col.char_length,
        }
    }
}

#[async_trait]
impl Sink for DamengSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.client.lock().await.query("SELECT 1 FROM DUAL")?;
        info!("Connected to Dameng");
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
        let trigger = format!("{:?}", flush_by_operation);
        let _timer = SINK_FLUSH_DURATION_SECONDS
            .with_label_values(&["dameng", &trigger])
            .start_timer();

        if !*self.initialized.read().await {
            for table_info in &self.table_info_list {
                self.table_info_cache
                    .lock()
                    .await
                    .insert(table_info.table_name.clone(), table_info.clone());
            }
            let mut col_info = CaseInsensitiveHashMapVecString::new_with_no_arg();
            for table_info in &self.table_info_list {
                for col in &table_info.columns {
                    col_info.entry_insert(table_info.table_name.as_str(), col.clone());
                }
            }
            *self.columns_cache.lock().await = col_info;
            *self.initialized.write().await = true;
        }

        let mut buf = self.buffer.lock().await;
        if buf.is_empty() {
            return Ok(());
        }
        match flush_by_operation {
            FlushByOperation::Timer => info!("Flushing Dameng Sink by timer... {}", buf.len()),
            FlushByOperation::Init => info!("Flushing Dameng Sink by init... {}", buf.len()),
            FlushByOperation::Signal => info!("Flushing Dameng Sink by signal... {}", buf.len()),
            FlushByOperation::Cdc => info!("Flushing Dameng Sink by cdc... {}", buf.len()),
        }

        let batch = std::mem::take(&mut *buf);
        drop(buf);

        let columns_cache = self.columns_cache.lock().await;
        let mut cache_for_roll_back = Vec::with_capacity(batch.len());
        for record in batch {
            cache_for_roll_back.push(record.clone());
            let table_name = record.table_name.clone();
            let pk_name = self.get_pk_name_from_cache(table_name.as_str()).await;
            let op_str = match record.op {
                Operation::CREATE(_) => "create",
                Operation::UPDATE => "update",
                Operation::DELETE => "delete",
                _ => "other",
            };
            SINK_EVENTS_TOTAL
                .with_label_values(&["dameng", &table_name, op_str])
                .inc();

            let result = match record.op {
                Operation::CREATE(_) | Operation::UPDATE => {
                    let columns = columns_cache.get(table_name.as_str());
                    self.upsert_record(table_name.as_str(), pk_name.as_str(), &columns, &record)
                        .await
                }
                Operation::DELETE => {
                    self.delete_record(table_name.as_str(), pk_name.as_str(), &record)
                        .await
                }
                _ => Err(format!("unexpected operation {:?}", record.op)),
            };

            if let Err(e) = result {
                error!("Dameng flush error: {}", e);
                SINK_FLUSH_ERRORS_TOTAL
                    .with_label_values(&["dameng", "write"])
                    .inc();
                let mut buf = self.buffer.lock().await;
                for cached_data_buffer in cache_for_roll_back {
                    buf.push(cached_data_buffer);
                }
                return Err(e);
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
        trace!("Dameng alter flush done");
        Ok(())
    }
}

impl DamengSink {
    async fn upsert_record(
        &self,
        table_name: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_name));
        }
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Err(format!("pk is empty: {}.{}", table_name, pk_name));
        }

        let set_sql = columns
            .iter()
            .map(|c| format!("{} = ?", Self::quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let update_sql = format!(
            "UPDATE {} SET {} WHERE {} = ?",
            self.qualified_table(table_name),
            set_sql,
            Self::quote_ident(pk_name)
        );
        let mut update_params = Vec::with_capacity(columns.len() + 1);
        for col in columns {
            update_params.push(Self::value_to_param(record.after.get(col)));
        }
        update_params.push(Self::value_to_param(pk));
        debug!("Dameng UPDATE: {}", update_sql);
        let affected = self
            .execute_with_params(update_sql.as_str(), &update_params)
            .await?;
        if affected > 0 {
            return Ok(());
        }

        let cols_sql = columns
            .iter()
            .map(|c| Self::quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = vec!["?"; columns.len()].join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.qualified_table(table_name),
            cols_sql,
            placeholders
        );
        let insert_params = columns
            .iter()
            .map(|col| Self::value_to_param(record.after.get(col)))
            .collect::<Vec<_>>();
        debug!("Dameng INSERT: {}", insert_sql);
        self.execute_with_params(insert_sql.as_str(), &insert_params)
            .await?;
        Ok(())
    }

    async fn delete_record(
        &self,
        table_name: &str,
        pk_name: &str,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Ok(());
        }
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            self.qualified_table(table_name),
            Self::quote_ident(pk_name)
        );
        let params = vec![Self::value_to_param(pk)];
        debug!("Dameng DELETE: {}", sql);
        self.execute_with_params(sql.as_str(), &params).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::DamengSink;

    #[test]
    fn schema_sql_quotes_identifiers() {
        assert_eq!(
            DamengSink::create_schema_sql("TARGET_SCHEMA"),
            "CREATE SCHEMA \"TARGET_SCHEMA\""
        );
        assert_eq!(
            DamengSink::set_schema_sql("TARGET_SCHEMA"),
            "SET SCHEMA \"TARGET_SCHEMA\""
        );
        assert_eq!(
            DamengSink::create_schema_sql("target\"schema"),
            "CREATE SCHEMA \"target\"\"schema\""
        );
    }

    #[test]
    fn mysql_char_types_use_character_semantics() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(50)"),
            "VARCHAR(50 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("char(20)"),
            "CHAR(20 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(9000)"),
            "CLOB"
        );
    }

    #[test]
    fn existing_char_columns_only_modify_when_needed() {
        let byte_semantics = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 50,
            char_length: 50,
        };
        let char_semantics = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 200,
            char_length: 50,
        };
        assert!(DamengSink::should_modify_existing_column(
            "varchar(50)",
            &byte_semantics
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "varchar(50)",
            &char_semantics
        ));
    }
}
