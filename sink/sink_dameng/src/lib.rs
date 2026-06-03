use async_trait::async_trait;
use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecString,
};
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_column_is_auto_increment_from_definition, mysql_type_token_from_column_definition,
};
use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value};
use dameng::Client;
use dameng::ToDmValue;
use dameng_types::DmValue;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

const DAMENG_INLINE_STRING_CHAR_LIMIT: u32 = 512;

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
    Bytes(Vec<u8>),
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
            DamengParam::Bytes(v) => DmValue::Bytea(v.clone()),
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
        sink.ensure_schema()
            .await
            .unwrap_or_else(|e| panic!("Dameng ensure schema failed: {}", e));
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

    async fn ensure_schema(&self) -> Result<(), String> {
        for table_info in &self.table_info_list {
            self.ensure_table(table_info).await.map_err(|e| {
                format!(
                    "Dameng ensure table failed: {} {}",
                    table_info.table_name, e
                )
            })?;
            self.ensure_columns(table_info).await.map_err(|e| {
                format!(
                    "Dameng ensure columns failed: {} {}",
                    table_info.table_name, e
                )
            })?;
        }
        Ok(())
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
            let auto_increment = src_col.eq_ignore_ascii_case(table_info.pk_column.as_str())
                && mysql_column_is_auto_increment_from_definition(def.as_str());
            cols_sql.push(format!(
                "{} {} {}",
                Self::quote_ident(src_col),
                Self::map_mysql_type_to_dameng_for_column(mysql_type.as_str(), auto_increment),
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
            warn!(
                "Dameng target table metadata is empty, skip column check: {} auto_create_table={} auto_add_column={} auto_modify_column={}",
                table_info.table_name,
                self.auto_create_table,
                self.auto_add_column,
                self.auto_modify_column
            );
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
            let dameng_type = Self::map_mysql_type_to_dameng(mysql_type.as_str());
            if let Some(existing_col) = existing_cols.get(key.as_str()) {
                let should_modify = Self::should_modify_existing_column(&mysql_type, existing_col);
                if dameng_type.eq_ignore_ascii_case("CLOB") || should_modify {
                    info!(
                        "Dameng column check: {}.{} existing={} data_length={} char_length={} expected={} auto_modify={}",
                        table_info.table_name,
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        dameng_type,
                        self.auto_modify_column
                    );
                }
                if should_modify && !self.auto_modify_column {
                    return Err(format!(
                        "Dameng column type mismatch and auto_modify_column is false: {}.{} existing={} data_length={} char_length={} expected={}",
                        table_info.table_name,
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        dameng_type
                    ));
                }
                if should_modify {
                    let sql = format!(
                        "ALTER TABLE {} MODIFY {} {} {}",
                        self.qualified_table(table_info.table_name.as_str()),
                        Self::quote_ident(src_col),
                        dameng_type,
                        nullable_sql
                    );
                    match self.execute(sql.as_str()).await {
                        Ok(_) => info!(
                            "Dameng auto modify column success: {} {}",
                            table_info.table_name, src_col
                        ),
                        Err(e) => {
                            let msg = format!(
                                "Dameng auto modify column failed: {} {} {}",
                                table_info.table_name, src_col, e
                            );
                            error!("{}", msg);
                            return Err(msg);
                        }
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
                dameng_type,
                nullable_sql
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto add column success: {} {}",
                    table_info.table_name, src_col
                ),
                Err(e) => {
                    let msg = format!(
                        "Dameng auto add column failed: {} {} {}",
                        table_info.table_name, src_col, e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
        }
        Ok(())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, String> {
        let sql = if self.schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_TABLES WHERE {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_TABLES WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", &self.schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        };
        let rows = self.query(sql.as_str(), &[]).await?;
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
        let sql = if self.schema.is_empty() {
            format!(
                "SELECT LISTAGG(COLUMN_NAME || ':' || DATA_TYPE || ':' || DATA_LENGTH || ':' || CHAR_LENGTH, '|') WITHIN GROUP (ORDER BY COLUMN_ID) FROM USER_TAB_COLUMNS WHERE {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT LISTAGG(COLUMN_NAME || ':' || DATA_TYPE || ':' || DATA_LENGTH || ':' || CHAR_LENGTH, '|') WITHIN GROUP (ORDER BY COLUMN_ID) FROM ALL_TAB_COLUMNS WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", &self.schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        };
        let rows = self.query(sql.as_str(), &[]).await?;
        let metadata = rows
            .first()
            .and_then(|row| row.get::<String>(0).ok())
            .unwrap_or_default();
        Ok(Self::parse_columns_metadata(metadata.as_str()))
    }

    fn parse_columns_metadata(metadata: &str) -> HashMap<String, DamengColumnInfo> {
        let mut cols = HashMap::new();
        for column in metadata.split('|').filter(|s| !s.is_empty()) {
            let mut parts = column.splitn(4, ':');
            let name = match parts.next() {
                Some(v) if !v.is_empty() => v,
                _ => continue,
            };
            let data_type = match parts.next() {
                Some(v) if !v.is_empty() => v,
                _ => continue,
            };
            let data_length = parts
                .next()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(0);
            let char_length = parts
                .next()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(0);
            cols.insert(
                name.to_ascii_lowercase(),
                DamengColumnInfo {
                    data_type: data_type.to_string(),
                    data_length,
                    char_length,
                },
            );
        }
        cols
    }

    fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn eq_original_or_upper_sql(column_name: &str, value: &str) -> String {
        format!(
            "({} = {} OR {} = {})",
            column_name,
            Self::quote_literal(value),
            column_name,
            Self::quote_literal(value.to_ascii_uppercase().as_str())
        )
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
            | Value::Json(v) => DamengParam::Text(v.clone()),
            Value::Blob(v) => DamengParam::Bytes(v.clone()),
            Value::Timestamp(_) | Value::Year(_) => DamengParam::Text(value.resolve_string()),
        }
    }

    fn map_mysql_type_to_dameng(mysql_type_token: &str) -> String {
        Self::map_mysql_type_to_dameng_for_column(mysql_type_token, false)
    }

    fn map_mysql_type_to_dameng_for_column(mysql_type_token: &str, auto_increment: bool) -> String {
        let t = mysql_type_token.to_ascii_lowercase();
        if !auto_increment
            && (t.starts_with("tinyint(1)") || t.starts_with("boolean") || t.starts_with("bool"))
        {
            return "BIT".to_string();
        }
        let dameng_type = if t.starts_with("tinyint") {
            if t.contains("unsigned") {
                "SMALLINT".to_string()
            } else {
                "TINYINT".to_string()
            }
        } else if t.starts_with("smallint") {
            if t.contains("unsigned") {
                "INT".to_string()
            } else {
                "SMALLINT".to_string()
            }
        } else if t.starts_with("mediumint") || t.starts_with("int") || t.starts_with("integer") {
            if t.contains("unsigned") {
                "BIGINT".to_string()
            } else {
                "INT".to_string()
            }
        } else if t.starts_with("bigint") {
            if t.contains("unsigned") {
                "DECIMAL(20,0)".to_string()
            } else {
                "BIGINT".to_string()
            }
        } else if t.starts_with("float") {
            "FLOAT".to_string()
        } else if t.starts_with("double") || t.starts_with("real") {
            "DOUBLE".to_string()
        } else if t.starts_with("decimal") || t.starts_with("numeric") {
            mysql_type_token.to_ascii_uppercase()
        } else if t.starts_with("datetime") || t.starts_with("timestamp") {
            "TIMESTAMP".to_string()
        } else if t.starts_with("date") {
            "DATE".to_string()
        } else if t.starts_with("time") {
            "TIME".to_string()
        } else if t.starts_with("varchar") {
            Self::map_mysql_char_type_to_dameng(
                "VARCHAR",
                mysql_type_token,
                DAMENG_INLINE_STRING_CHAR_LIMIT,
            )
        } else if t.starts_with("char") {
            Self::map_mysql_char_type_to_dameng(
                "CHAR",
                mysql_type_token,
                DAMENG_INLINE_STRING_CHAR_LIMIT,
            )
        } else if t.contains("text")
            || t.starts_with("json")
            || t.starts_with("enum")
            || t.starts_with("set")
        {
            "CLOB".to_string()
        } else if t.contains("blob") || t.contains("binary") {
            "BLOB".to_string()
        } else {
            "VARCHAR(255 CHAR)".to_string()
        };

        if auto_increment && Self::dameng_type_supports_identity(dameng_type.as_str()) {
            format!("{} IDENTITY(1,1)", dameng_type)
        } else {
            dameng_type
        }
    }

    fn dameng_type_supports_identity(dameng_type: &str) -> bool {
        let t = dameng_type.to_ascii_uppercase();
        t == "TINYINT"
            || t == "SMALLINT"
            || t == "INT"
            || t == "BIGINT"
            || t.starts_with("DECIMAL")
            || t.starts_with("NUMERIC")
    }

    fn source_column_dameng_type(table_info: &TableInfoVo, column_name: &str) -> Option<String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let def = defs.get(column_name.to_ascii_lowercase().as_str())?;
        let mysql_type = mysql_type_token_from_column_definition(def.as_str())?;
        Some(Self::map_mysql_type_to_dameng(mysql_type.as_str()))
    }

    fn is_source_clob_column(table_info: &TableInfoVo, column_name: &str) -> bool {
        Self::source_column_dameng_type(table_info, column_name)
            .is_some_and(|dameng_type| dameng_type.eq_ignore_ascii_case("CLOB"))
    }

    fn value_placeholder(table_info: &TableInfoVo, column_name: &str) -> &'static str {
        if Self::is_source_clob_column(table_info, column_name) {
            "CAST(? AS CLOB)"
        } else {
            "?"
        }
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
        let table_info = self.table_info_cache.lock().await.get(table_name);

        let set_sql = columns
            .iter()
            .map(|c| {
                format!(
                    "{} = {}",
                    Self::quote_ident(c),
                    Self::value_placeholder(&table_info, c)
                )
            })
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
        let placeholders = columns
            .iter()
            .map(|c| Self::value_placeholder(&table_info, c))
            .collect::<Vec<_>>()
            .join(", ");
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
    use common::{TableInfoVo, Value};
    use dameng::ToDmValue;
    use dameng_types::DmValue;

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
    fn metadata_sql_quotes_literals() {
        assert_eq!(
            DamengSink::quote_literal("newsee'system"),
            "'newsee''system'"
        );
        assert_eq!(
            DamengSink::eq_original_or_upper_sql("OWNER", "newsee-system"),
            "(OWNER = 'newsee-system' OR OWNER = 'NEWSEE-SYSTEM')"
        );
    }

    #[test]
    fn parses_dameng_columns_metadata() {
        let cols = DamengSink::parse_columns_metadata(
            "id:BIGINT:8:0|param:VARCHAR:16000:4000|opration:VARCHAR:4000:1000",
        );

        let param = cols.get("param").unwrap();
        assert_eq!(param.data_type, "VARCHAR");
        assert_eq!(param.data_length, 16000);
        assert_eq!(param.char_length, 4000);

        let opration = cols.get("opration").unwrap();
        assert_eq!(opration.data_type, "VARCHAR");
        assert_eq!(opration.data_length, 4000);
        assert_eq!(opration.char_length, 1000);
    }

    #[test]
    fn blob_values_bind_as_dameng_bytes() {
        let value = Value::Blob(vec![0x1f, 0x8b, 0x08]);
        assert_eq!(
            DamengSink::value_to_param(&value).to_dm_value(),
            DmValue::Bytea(vec![0x1f, 0x8b, 0x08])
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
            DamengSink::map_mysql_type_to_dameng("varchar(1000)"),
            "CLOB"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(4000)"),
            "CLOB"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(9000)"),
            "CLOB"
        );
    }

    #[test]
    fn auto_increment_integer_columns_use_dameng_identity() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("bigint", false),
            "BIGINT"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("bigint", true),
            "BIGINT IDENTITY(1,1)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("int(11) unsigned", true),
            "BIGINT IDENTITY(1,1)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("bigint unsigned", true),
            "DECIMAL(20,0) IDENTITY(1,1)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("tinyint(1)", true),
            "TINYINT IDENTITY(1,1)"
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

    #[test]
    fn large_varchar_columns_use_clob_placeholder() {
        let table_info = TableInfoVo {
            table_name: "ns_system_log".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_system_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `param` varchar(4000) DEFAULT NULL,
  `modul` varchar(100) DEFAULT NULL,
  `opration` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "param".to_string(),
                "modul".to_string(),
                "opration".to_string(),
            ],
        };

        assert_eq!(
            DamengSink::value_placeholder(&table_info, "param"),
            "CAST(? AS CLOB)"
        );
        assert_eq!(
            DamengSink::value_placeholder(&table_info, "opration"),
            "CAST(? AS CLOB)"
        );
        assert_eq!(DamengSink::value_placeholder(&table_info, "modul"), "?");
    }
}
