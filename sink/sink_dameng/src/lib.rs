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
use common::{
    CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value,
    database_table_key,
};
use dameng::Client;
use dameng::ToDmValue;
use dameng_types::DmValue;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

const DAMENG_INLINE_STRING_CHAR_LIMIT: u32 = 512;
const DAMENG_DECIMAL_MAX_PRECISION: u32 = 38;

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
    identity_insert_tables: Mutex<HashSet<String>>,
    active_identity_insert_table: Mutex<Option<(String, String)>>,
}

impl DamengSink {
    pub async fn new(config: &CdcConfig, table_info_list: Vec<TableInfoVo>) -> Self {
        let host = config.first_sink_not_blank("host");
        let port = config.first_sink("port").parse::<u16>().unwrap_or(5236);
        let username = config.first_sink_not_blank("username");
        let password = config.first_sink_not_blank("password");
        let configured_schema = {
            let schema = config.first_sink("schema");
            if schema.is_empty() {
                config.first_sink("database")
            } else {
                schema
            }
        };
        let schema = config
            .sink_databases()
            .into_iter()
            .find(|schema| !schema.trim().is_empty())
            .unwrap_or(configured_schema);
        if let Err(e) = Self::validate_merged_target_schema(&table_info_list, schema.as_str()) {
            panic!("{}", e);
        }
        if let Err(e) = Self::validate_target_column_names(&table_info_list, schema.as_str()) {
            panic!("{}", e);
        }

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
            identity_insert_tables: Mutex::new(HashSet::new()),
            active_identity_insert_table: Mutex::new(None),
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
        let client = Self::connect_client(&self.host, self.port, &self.username, &self.password)
            .map_err(|e| e.to_string())?;
        *self.client.lock().await = client;
        Ok(())
    }

    async fn ensure_schema(&self) -> Result<(), String> {
        for table_info in &self.table_info_list {
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            self.ensure_table(schema.as_str(), table_info)
                .await
                .map_err(|e| {
                    format!(
                        "Dameng ensure table failed: {} {}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        e
                    )
                })?;
            self.ensure_columns(schema.as_str(), table_info)
                .await
                .map_err(|e| {
                    format!(
                        "Dameng ensure columns failed: {} {}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        e
                    )
                })?;
        }
        Ok(())
    }

    async fn ensure_database(&self) -> Result<(), String> {
        for schema in self.target_schemas() {
            self.ensure_database_schema(schema.as_str()).await?;
        }
        Ok(())
    }

    async fn ensure_database_schema(&self, schema: &str) -> Result<(), String> {
        if schema.is_empty() {
            return Ok(());
        }

        if self.schema_exists(schema).await? {
            return Ok(());
        }
        if !self.auto_create_database {
            return Err(format!(
                "target schema {} is unavailable and auto_create_database is false",
                schema
            ));
        }

        let create_schema_sql = Self::create_schema_sql(schema);
        match self.execute(create_schema_sql.as_str()).await {
            Ok(_) => {}
            Err(create_schema_error) => {
                if self.schema_exists(schema).await.unwrap_or(false) {
                    return Ok(());
                }
                return Err(format!(
                    "create schema failed: {} {}",
                    schema, create_schema_error
                ));
            }
        }
        if !self.schema_exists(schema).await? {
            return Err(format!(
                "create schema succeeded but schema is not visible: {}",
                schema
            ));
        }
        info!("Dameng auto create schema success: {}", schema);
        Ok(())
    }

    async fn ensure_table(&self, schema: &str, table_info: &TableInfoVo) -> Result<(), String> {
        if !self.auto_create_table {
            return Ok(());
        }
        if self
            .table_exists(schema, table_info.table_name.as_str())
            .await?
        {
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
                Self::quote_column_ident(src_col),
                Self::map_mysql_type_to_dameng_for_column(mysql_type.as_str(), auto_increment),
                nullable_sql
            ));
        }
        if cols_sql.is_empty() {
            return Err("no columns parsed from source create table sql".to_string());
        }
        cols_sql.push(format!(
            "PRIMARY KEY ({})",
            Self::quote_column_ident(table_info.pk_column.as_str())
        ));
        let sql = format!(
            "CREATE TABLE {} ({})",
            Self::qualified_table(schema, table_info.table_name.as_str()),
            cols_sql.join(", ")
        );
        self.execute(sql.as_str()).await?;
        info!(
            "Dameng auto create table success: {}",
            Self::target_table_key(schema, table_info.table_name.as_str())
        );
        Ok(())
    }

    async fn ensure_columns(&self, schema: &str, table_info: &TableInfoVo) -> Result<(), String> {
        if !self.auto_add_column && !self.auto_modify_column {
            return Ok(());
        }
        let existing_cols = self
            .existing_columns(schema, table_info.table_name.as_str())
            .await?;
        if existing_cols.is_empty() {
            warn!(
                "Dameng target table metadata is empty, skip column check: {} auto_create_table={} auto_add_column={} auto_modify_column={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
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
            let target_col = Self::target_column_name(src_col);
            let target_key = target_col.to_ascii_lowercase();
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
            if let Some(existing_col) = existing_cols.get(target_key.as_str()) {
                let should_modify = Self::should_modify_existing_column(&mysql_type, existing_col);
                if dameng_type.eq_ignore_ascii_case("CLOB") || should_modify {
                    info!(
                        "Dameng column check: {}.{} existing={} data_length={} char_length={} expected={} auto_modify={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
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
                        Self::target_table_key(schema, table_info.table_name.as_str()),
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
                        Self::qualified_table(schema, table_info.table_name.as_str()),
                        Self::quote_ident(target_col.as_str()),
                        dameng_type,
                        nullable_sql
                    );
                    match self.execute(sql.as_str()).await {
                        Ok(_) => info!(
                            "Dameng auto modify column success: {} {}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            src_col
                        ),
                        Err(e) => {
                            let msg = format!(
                                "Dameng auto modify column failed: {} {} {}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                src_col,
                                e
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
                Self::qualified_table(schema, table_info.table_name.as_str()),
                Self::quote_ident(target_col.as_str()),
                dameng_type,
                nullable_sql
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto add column success: {} {}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    src_col
                ),
                Err(e) => {
                    let msg = format!(
                        "Dameng auto add column failed: {} {} {}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
        }
        Ok(())
    }

    async fn table_exists(&self, schema: &str, table_name: &str) -> Result<bool, String> {
        let sql = if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_TABLES WHERE {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_TABLES WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
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

    async fn schema_exists(&self, schema: &str) -> Result<bool, String> {
        let rows = self
            .query(Self::schema_exists_sql(schema).as_str(), &[])
            .await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn existing_columns(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<HashMap<String, DamengColumnInfo>, String> {
        let sql = if schema.is_empty() {
            format!(
                "SELECT LISTAGG(COLUMN_NAME || ':' || DATA_TYPE || ':' || DATA_LENGTH || ':' || CHAR_LENGTH, '|') WITHIN GROUP (ORDER BY COLUMN_ID) FROM USER_TAB_COLUMNS WHERE {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT LISTAGG(COLUMN_NAME || ':' || DATA_TYPE || ':' || DATA_LENGTH || ':' || CHAR_LENGTH, '|') WITHIN GROUP (ORDER BY COLUMN_ID) FROM ALL_TAB_COLUMNS WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
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

    async fn execute_insert_with_params(
        &self,
        sql: &str,
        params: &[DamengParam],
    ) -> Result<u64, String> {
        let first_result = {
            let mut client = self.client.lock().await;
            let refs = Self::param_refs(params);
            client.execute_with_params(sql, &refs)
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if Self::is_insert_control_flow_error(msg.as_str()) {
                    return Err(msg);
                }
                error!("Dameng execute failed, retrying: {} sql: {}", msg, sql);
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

    fn is_identity_insert_required_error(error: &str) -> bool {
        error.contains("-2723") || error.to_ascii_uppercase().contains("IDENTITY_INSERT")
    }

    fn is_duplicate_key_error(error: &str) -> bool {
        error.contains("-6602") || error.to_ascii_lowercase().contains("unique constraint")
    }

    fn is_insert_control_flow_error(error: &str) -> bool {
        Self::is_identity_insert_required_error(error) || Self::is_duplicate_key_error(error)
    }

    fn identity_insert_cache_key(table_key: &str) -> String {
        table_key.to_ascii_lowercase()
    }

    fn identity_insert_sql(qualified_table: &str, enabled: bool) -> String {
        format!(
            "SET IDENTITY_INSERT {} {}",
            qualified_table,
            if enabled { "ON" } else { "OFF" }
        )
    }

    async fn set_identity_insert(
        &self,
        qualified_table: &str,
        enabled: bool,
    ) -> Result<(), String> {
        self.execute(Self::identity_insert_sql(qualified_table, enabled).as_str())
            .await
            .map(|_| ())
    }

    async fn ensure_identity_insert_enabled(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
    ) -> Result<(), String> {
        let key = Self::identity_insert_cache_key(table_key);
        let qualified_table = Self::qualified_table(schema, table_name);
        let mut active = self.active_identity_insert_table.lock().await;
        if active
            .as_ref()
            .is_some_and(|(active_key, _)| active_key == &key)
        {
            return Ok(());
        }

        if let Some((_, active_qualified_table)) = active.as_ref() {
            self.set_identity_insert(active_qualified_table.as_str(), false)
                .await?;
        }
        self.set_identity_insert(qualified_table.as_str(), true)
            .await?;
        *active = Some((key, qualified_table));
        Ok(())
    }

    async fn disable_active_identity_insert(&self) -> Result<(), String> {
        let mut active = self.active_identity_insert_table.lock().await;
        if let Some((_, active_qualified_table)) = active.as_ref() {
            self.set_identity_insert(active_qualified_table.as_str(), false)
                .await?;
            *active = None;
        }
        Ok(())
    }

    async fn clear_active_identity_insert_state(&self) {
        *self.active_identity_insert_table.lock().await = None;
    }

    async fn get_pk_name_from_cache(&self, table_name: &str) -> String {
        self.table_info_cache
            .lock()
            .await
            .get(table_name)
            .pk_column
            .to_string()
    }

    fn create_schema_sql(schema: &str) -> String {
        format!("CREATE SCHEMA {}", Self::quote_ident(schema))
    }

    fn schema_exists_sql(schema: &str) -> String {
        format!(
            "SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE$ = 'SCH' AND {}",
            Self::eq_original_or_upper_sql("NAME", schema)
        )
    }

    fn table_info_target_schema(table_info: &TableInfoVo, fallback: &str) -> String {
        if table_info.target_database.trim().is_empty() {
            fallback.to_string()
        } else {
            table_info.target_database.clone()
        }
    }

    fn record_target_schema(&self, record: &DataBuffer) -> String {
        if record.target_database.trim().is_empty() {
            self.schema.clone()
        } else {
            record.target_database.clone()
        }
    }

    fn target_table_key(schema: &str, table_name: &str) -> String {
        database_table_key(schema, table_name)
    }

    fn target_schemas(&self) -> Vec<String> {
        let mut schemas = Vec::new();
        let mut seen = HashSet::new();
        if !self.schema.trim().is_empty() && seen.insert(self.schema.to_ascii_lowercase()) {
            schemas.push(self.schema.clone());
        }
        for table_info in &self.table_info_list {
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            if schema.trim().is_empty() {
                continue;
            }
            if seen.insert(schema.to_ascii_lowercase()) {
                schemas.push(schema);
            }
        }
        schemas
    }

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn quote_column_ident(column_name: &str) -> String {
        Self::quote_ident(Self::target_column_name(column_name).as_str())
    }

    fn target_column_name(column_name: &str) -> String {
        if Self::is_disallowed_dameng_column_name(column_name) {
            format!("{}_", column_name)
        } else {
            column_name.to_string()
        }
    }

    fn is_disallowed_dameng_column_name(column_name: &str) -> bool {
        matches!(
            column_name.to_ascii_uppercase().as_str(),
            "ROWID"
                | "ROWNUM"
                | "TRXID"
                | "VERSIONS_STARTTIME"
                | "VERSIONS_ENDTIME"
                | "VERSIONS_STARTTRXID"
                | "VERSIONS_ENDTRXID"
                | "VERSIONS_OPERATION"
        )
    }

    fn qualified_table(schema: &str, table_name: &str) -> String {
        if schema.is_empty() {
            Self::quote_ident(table_name)
        } else {
            format!(
                "{}.{}",
                Self::quote_ident(schema),
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
            Value::UnsignedInt16(v) => DamengParam::I32(*v as i32),
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
            Self::map_mysql_decimal_type_to_dameng(mysql_type_token)
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

    fn is_source_blob_column(table_info: &TableInfoVo, column_name: &str) -> bool {
        Self::source_column_dameng_type(table_info, column_name)
            .is_some_and(|dameng_type| dameng_type.eq_ignore_ascii_case("BLOB"))
    }

    fn value_placeholder(table_info: &TableInfoVo, column_name: &str) -> &'static str {
        if Self::is_source_blob_column(table_info, column_name) {
            "HEXTORAW(?)"
        } else {
            "?"
        }
    }

    fn value_to_param_for_column(
        table_info: &TableInfoVo,
        column_name: &str,
        value: &Value,
    ) -> DamengParam {
        if Self::is_source_blob_column(table_info, column_name) {
            match value {
                Value::None => DamengParam::Null,
                Value::Blob(v) => DamengParam::Text(Self::bytes_to_hex(v)),
                _ => Self::value_to_param(value),
            }
        } else {
            Self::value_to_param(value)
        }
    }

    fn bytes_to_hex(value: &[u8]) -> String {
        value.iter().map(|b| format!("{:02X}", b)).collect()
    }

    fn validate_merged_target_schema(
        table_info_list: &[TableInfoVo],
        fallback_schema: &str,
    ) -> Result<(), String> {
        let mut signatures: HashMap<String, (String, Vec<(String, String)>)> = HashMap::new();
        for table_info in table_info_list {
            let schema = Self::table_info_target_schema(table_info, fallback_schema);
            let target_key =
                Self::target_table_key(schema.as_str(), table_info.table_name.as_str())
                    .to_ascii_lowercase();
            let signature = Self::table_schema_signature(table_info);
            match signatures.get(&target_key) {
                None => {
                    signatures.insert(
                        target_key,
                        (
                            Self::target_column_name(table_info.pk_column.as_str())
                                .to_ascii_lowercase(),
                            signature,
                        ),
                    );
                }
                Some((pk, existing_signature)) => {
                    if !pk.eq_ignore_ascii_case(
                        Self::target_column_name(table_info.pk_column.as_str()).as_str(),
                    ) || existing_signature != &signature
                    {
                        return Err(format!(
                            "Dameng multi_mode target table schema mismatch: {}",
                            Self::target_table_key(schema.as_str(), table_info.table_name.as_str())
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_target_column_names(
        table_info_list: &[TableInfoVo],
        fallback_schema: &str,
    ) -> Result<(), String> {
        for table_info in table_info_list {
            let schema = Self::table_info_target_schema(table_info, fallback_schema);
            let table_key = Self::target_table_key(schema.as_str(), table_info.table_name.as_str());
            let mut seen: HashMap<String, (String, String)> = HashMap::new();
            for source_column in &table_info.columns {
                let target_column = Self::target_column_name(source_column);
                let target_key = target_column.to_ascii_lowercase();
                if let Some((existing_source, existing_target)) =
                    seen.insert(target_key, (source_column.clone(), target_column.clone()))
                {
                    return Err(format!(
                        "Dameng target column name conflict: {} source columns {} and {} both map to target column {}",
                        table_key, existing_source, source_column, existing_target
                    ));
                }
            }
        }
        Ok(())
    }

    fn table_schema_signature(table_info: &TableInfoVo) -> Vec<(String, String)> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        table_info
            .columns
            .iter()
            .map(|column| {
                let key = column.to_ascii_lowercase();
                let type_token = defs
                    .get(&key)
                    .and_then(|def| {
                        let mysql_type = mysql_type_token_from_column_definition(def.as_str())?;
                        let auto_increment = column
                            .eq_ignore_ascii_case(table_info.pk_column.as_str())
                            && mysql_column_is_auto_increment_from_definition(def.as_str());
                        Some(Self::map_mysql_type_to_dameng_for_column(
                            mysql_type.as_str(),
                            auto_increment,
                        ))
                    })
                    .unwrap_or_else(|| "__missing_definition__".to_string())
                    .to_ascii_lowercase();
                (
                    Self::target_column_name(column).to_ascii_lowercase(),
                    type_token,
                )
            })
            .collect()
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

    fn map_mysql_decimal_type_to_dameng(mysql_type_token: &str) -> String {
        let Some((precision, scale)) = Self::mysql_decimal_precision_scale(mysql_type_token) else {
            return "DECIMAL".to_string();
        };
        let precision = precision.max(1);
        let scale = scale.min(precision);
        if precision <= DAMENG_DECIMAL_MAX_PRECISION {
            return format!("DECIMAL({},{})", precision, scale);
        }

        let scale = scale.min(DAMENG_DECIMAL_MAX_PRECISION);
        format!("DECIMAL({},{})", DAMENG_DECIMAL_MAX_PRECISION, scale)
    }

    fn mysql_decimal_precision_scale(mysql_type_token: &str) -> Option<(u32, u32)> {
        let start = mysql_type_token.find('(')? + 1;
        let end = mysql_type_token[start..].find(')')? + start;
        let mut parts = mysql_type_token[start..end].split(',');
        let precision = parts.next()?.trim().parse::<u32>().ok()?;
        let scale = parts
            .next()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0);
        Some((precision, scale))
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
            let mut inserted_tables = HashSet::new();
            for table_info in &self.table_info_list {
                let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
                let table_key =
                    Self::target_table_key(schema.as_str(), table_info.table_name.as_str());
                if !inserted_tables.insert(table_key.to_ascii_lowercase()) {
                    continue;
                }
                self.table_info_cache
                    .lock()
                    .await
                    .insert(table_key, table_info.clone());
            }
            let mut col_info = CaseInsensitiveHashMapVecString::new_with_no_arg();
            let mut inserted_columns = HashSet::new();
            for table_info in &self.table_info_list {
                let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
                let table_key =
                    Self::target_table_key(schema.as_str(), table_info.table_name.as_str());
                if !inserted_columns.insert(table_key.to_ascii_lowercase()) {
                    continue;
                }
                for col in &table_info.columns {
                    col_info.entry_insert(table_key.as_str(), col.clone());
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
        let mut flush_result = Ok(());
        for record in batch {
            cache_for_roll_back.push(record.clone());
            let schema = self.record_target_schema(&record);
            let table_name = record.table_name.clone();
            let table_key = Self::target_table_key(schema.as_str(), table_name.as_str());
            let pk_name = self.get_pk_name_from_cache(table_key.as_str()).await;
            let op_str = match record.op {
                Operation::CREATE(_) => "create",
                Operation::UPDATE => "update",
                Operation::DELETE => "delete",
                _ => "other",
            };
            SINK_EVENTS_TOTAL
                .with_label_values(&["dameng", table_key.as_str(), op_str])
                .inc();

            let result = match record.op {
                Operation::CREATE(true) => {
                    let columns = columns_cache.get(table_key.as_str());
                    self.insert_or_update_record(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        pk_name.as_str(),
                        &columns,
                        &record,
                    )
                    .await
                }
                Operation::CREATE(false) | Operation::UPDATE => {
                    let columns = columns_cache.get(table_key.as_str());
                    self.upsert_record(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        pk_name.as_str(),
                        &columns,
                        &record,
                    )
                    .await
                }
                Operation::DELETE => {
                    self.delete_record(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        pk_name.as_str(),
                        &record,
                    )
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
                flush_result = Err(e);
                break;
            }
        }
        drop(columns_cache);

        match (flush_result, self.disable_active_identity_insert().await) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), Ok(_)) => Err(e),
            (Ok(_), Err(identity_error)) => Err(identity_error),
            (Err(e), Err(identity_error)) => Err(format!(
                "{}; Dameng disable identity_insert failed: {}",
                e, identity_error
            )),
        }
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
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_key));
        }
        let affected = self
            .update_record(schema, table_name, table_key, pk_name, columns, record)
            .await?;
        if affected > 0 {
            return Ok(());
        }

        self.insert_record(schema, table_name, table_key, columns, record)
            .await
    }

    async fn insert_or_update_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_key));
        }
        match self
            .insert_record(schema, table_name, table_key, columns, record)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_duplicate_key_error(e.as_str()) => {
                let affected = self
                    .update_record(schema, table_name, table_key, pk_name, columns, record)
                    .await?;
                if affected > 0 {
                    Ok(())
                } else {
                    Err(format!(
                        "Dameng insert duplicate fallback update affected 0 rows: {}",
                        table_key
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn update_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<u64, String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Err(format!("pk is empty: {}.{}", table_key, pk_name));
        }
        let table_info = self.table_info_cache.lock().await.get(table_key);

        let update_columns = columns
            .iter()
            .filter(|c| !c.eq_ignore_ascii_case(pk_name))
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            return Ok(0);
        }

        let set_sql = update_columns
            .iter()
            .map(|c| {
                format!(
                    "{} = {}",
                    Self::quote_column_ident(c),
                    Self::value_placeholder(&table_info, c)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let update_sql = format!(
            "UPDATE {} SET {} WHERE {} = {}",
            Self::qualified_table(schema, table_name),
            set_sql,
            Self::quote_column_ident(pk_name),
            Self::value_placeholder(&table_info, pk_name)
        );
        let mut update_params = Vec::with_capacity(update_columns.len() + 1);
        for col in update_columns {
            update_params.push(Self::value_to_param_for_column(
                &table_info,
                col,
                record.after.get(col),
            ));
        }
        update_params.push(Self::value_to_param_for_column(&table_info, pk_name, pk));
        debug!("Dameng UPDATE: {}", update_sql);
        self.execute_with_params(update_sql.as_str(), &update_params)
            .await
    }

    async fn insert_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        let table_info = self.table_info_cache.lock().await.get(table_key);
        let cols_sql = columns
            .iter()
            .map(|c| Self::quote_column_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = columns
            .iter()
            .map(|c| Self::value_placeholder(&table_info, c))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::qualified_table(schema, table_name),
            cols_sql,
            placeholders
        );
        let insert_params = columns
            .iter()
            .map(|col| Self::value_to_param_for_column(&table_info, col, record.after.get(col)))
            .collect::<Vec<_>>();
        debug!("Dameng INSERT: {}", insert_sql);

        let identity_insert_cache_key = Self::identity_insert_cache_key(table_key);
        let use_identity_insert = self
            .identity_insert_tables
            .lock()
            .await
            .contains(identity_insert_cache_key.as_str());
        if use_identity_insert {
            self.insert_with_identity_insert(
                schema,
                table_name,
                table_key,
                insert_sql.as_str(),
                &insert_params,
            )
            .await?;
            return Ok(());
        }

        match self
            .execute_insert_with_params(insert_sql.as_str(), &insert_params)
            .await
        {
            Ok(_) => {}
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.identity_insert_tables
                    .lock()
                    .await
                    .insert(identity_insert_cache_key);
                self.insert_with_identity_insert(
                    schema,
                    table_name,
                    table_key,
                    insert_sql.as_str(),
                    &insert_params,
                )
                .await?;
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    async fn insert_with_identity_insert(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        insert_sql: &str,
        insert_params: &[DamengParam],
    ) -> Result<(), String> {
        self.ensure_identity_insert_enabled(schema, table_name, table_key)
            .await?;
        match self
            .execute_insert_with_params(insert_sql, insert_params)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.clear_active_identity_insert_state().await;
                self.ensure_identity_insert_enabled(schema, table_name, table_key)
                    .await?;
                self.execute_insert_with_params(insert_sql, insert_params)
                    .await
                    .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    async fn delete_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Ok(());
        }
        let table_info = self.table_info_cache.lock().await.get(table_key);
        let sql = format!(
            "DELETE FROM {} WHERE {} = {}",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name),
            Self::value_placeholder(&table_info, pk_name)
        );
        let params = vec![Self::value_to_param_for_column(&table_info, pk_name, pk)];
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

    fn table_info(target_schema: &str, column_type: &str) -> TableInfoVo {
        TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: target_schema.to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: format!(
                "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `name` {} DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
                column_type
            ),
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    #[test]
    fn schema_sql_quotes_identifiers_and_literals() {
        assert_eq!(
            DamengSink::create_schema_sql("TARGET_SCHEMA"),
            "CREATE SCHEMA \"TARGET_SCHEMA\""
        );
        assert_eq!(
            DamengSink::schema_exists_sql("TARGET_SCHEMA"),
            "SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE$ = 'SCH' AND (NAME = 'TARGET_SCHEMA' OR NAME = 'TARGET_SCHEMA')"
        );
        assert_eq!(
            DamengSink::create_schema_sql("target\"schema"),
            "CREATE SCHEMA \"target\"\"schema\""
        );
        assert_eq!(
            DamengSink::schema_exists_sql("newsee-bpm"),
            "SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE$ = 'SCH' AND (NAME = 'newsee-bpm' OR NAME = 'NEWSEE-BPM')"
        );
    }

    #[test]
    fn qualified_table_uses_target_schema() {
        assert_eq!(
            DamengSink::qualified_table("TARGET_SCHEMA", "orders"),
            "\"TARGET_SCHEMA\".\"orders\""
        );
        assert_eq!(DamengSink::qualified_table("", "orders"), "\"orders\"");
        assert_eq!(
            DamengSink::target_table_key("TARGET_SCHEMA", "orders"),
            "TARGET_SCHEMA.orders"
        );
    }

    #[test]
    fn disallowed_dameng_column_names_are_remapped() {
        assert_eq!(DamengSink::target_column_name("id"), "id");
        assert_eq!(DamengSink::target_column_name("rOWID"), "rOWID_");
        assert_eq!(DamengSink::target_column_name("ROWID"), "ROWID_");
        assert_eq!(DamengSink::target_column_name("trxid"), "trxid_");
        assert_eq!(DamengSink::target_column_name("rownum"), "rownum_");
        assert_eq!(
            DamengSink::target_column_name("VERSIONS_STARTTIME"),
            "VERSIONS_STARTTIME_"
        );
        assert_eq!(
            DamengSink::target_column_name("versions_endtime"),
            "versions_endtime_"
        );
        assert_eq!(
            DamengSink::target_column_name("VERSIONS_STARTTRXID"),
            "VERSIONS_STARTTRXID_"
        );
        assert_eq!(
            DamengSink::target_column_name("versions_endtrxid"),
            "versions_endtrxid_"
        );
        assert_eq!(
            DamengSink::target_column_name("VERSIONS_OPERATION"),
            "VERSIONS_OPERATION_"
        );
        assert_eq!(DamengSink::quote_column_ident("rOWID"), "\"rOWID_\"");
        assert_eq!(DamengSink::quote_column_ident("trxid"), "\"trxid_\"");
    }

    #[test]
    fn table_schema_signature_uses_target_column_name() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "line_item".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: "CREATE TABLE `line_item` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `rOWID` varchar(50) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB".to_string(),
            columns: vec!["id".to_string(), "rOWID".to_string()],
        };

        let signature = DamengSink::table_schema_signature(&table_info);

        assert!(signature.contains(&("rowid_".to_string(), "varchar(50 char)".to_string())));
    }

    #[test]
    fn target_column_name_validation_rejects_remap_collision() {
        let mut table_info = table_info("target_schema", "varchar(64)");
        table_info.columns = vec!["id".to_string(), "rOWID".to_string(), "rOWID_".to_string()];

        let err = DamengSink::validate_target_column_names(&[table_info], "").unwrap_err();

        assert!(err.contains("target column name conflict"));
        assert!(err.contains("rOWID"));
        assert!(err.contains("rOWID_"));
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
    fn detects_identity_insert_required_error() {
        assert!(DamengSink::is_identity_insert_required_error(
            "query failed: -2723: Only if specified in the column list and SET IDENTITY_INSERT is ON"
        ));
        assert!(DamengSink::is_identity_insert_required_error(
            "SET identity_insert is ON"
        ));
        assert!(!DamengSink::is_identity_insert_required_error(
            "query failed: -6609: Violate not null constraint"
        ));
    }

    #[test]
    fn detects_duplicate_key_error() {
        assert!(DamengSink::is_duplicate_key_error(
            "query failed: -6602: Violate unique constraint"
        ));
        assert!(DamengSink::is_duplicate_key_error(
            "Violate UNIQUE CONSTRAINT on primary key"
        ));
        assert!(!DamengSink::is_duplicate_key_error(
            "query failed: -2723: SET IDENTITY_INSERT is ON"
        ));
    }

    #[test]
    fn insert_control_flow_errors_are_not_retried() {
        assert!(DamengSink::is_insert_control_flow_error(
            "query failed: -6602: Violate unique constraint"
        ));
        assert!(DamengSink::is_insert_control_flow_error(
            "query failed: -2723: SET IDENTITY_INSERT is ON"
        ));
        assert!(!DamengSink::is_insert_control_flow_error(
            "query failed: -2665: record too long"
        ));
    }

    #[test]
    fn identity_insert_cache_key_is_case_insensitive() {
        assert_eq!(
            DamengSink::identity_insert_cache_key("Ns_Core_Funcinfo"),
            "ns_core_funcinfo"
        );
    }

    #[test]
    fn identity_insert_cache_key_includes_schema_when_present() {
        assert_eq!(
            DamengSink::identity_insert_cache_key("TARGET_SCHEMA.Ns_Core_Funcinfo"),
            "target_schema.ns_core_funcinfo"
        );
    }

    #[test]
    fn identity_insert_sql_uses_qualified_table() {
        assert_eq!(
            DamengSink::identity_insert_sql("\"S\".\"T\"", true),
            "SET IDENTITY_INSERT \"S\".\"T\" ON"
        );
        assert_eq!(
            DamengSink::identity_insert_sql("\"S\".\"T\"", false),
            "SET IDENTITY_INSERT \"S\".\"T\" OFF"
        );
    }

    #[test]
    fn merged_target_schema_allows_identical_tables() {
        let tables = vec![
            table_info("DST_SCHEMA", "varchar(64)"),
            table_info("DST_SCHEMA", "varchar(64)"),
        ];

        assert!(DamengSink::validate_merged_target_schema(&tables, "").is_ok());
    }

    #[test]
    fn merged_target_schema_rejects_type_mismatch_in_same_schema() {
        let tables = vec![
            table_info("DST_SCHEMA", "varchar(64)"),
            table_info("DST_SCHEMA", "varchar(128)"),
        ];

        let err = DamengSink::validate_merged_target_schema(&tables, "").unwrap_err();

        assert!(err.contains("schema mismatch"));
    }

    #[test]
    fn merged_target_schema_allows_same_table_in_different_schemas() {
        let tables = vec![
            table_info("DST_A", "varchar(64)"),
            table_info("DST_B", "varchar(128)"),
        ];

        assert!(DamengSink::validate_merged_target_schema(&tables, "").is_ok());
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
    fn mysql_decimal_precision_is_limited_for_dameng() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(20,2)"),
            "DECIMAL(20,2)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("numeric(10)"),
            "DECIMAL(10,0)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(50,2)"),
            "DECIMAL(38,2)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(65,30)"),
            "DECIMAL(38,30)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(65,30) unsigned"),
            "DECIMAL(38,30)"
        );
        assert_eq!(DamengSink::map_mysql_type_to_dameng("decimal"), "DECIMAL");
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
    fn clob_columns_use_plain_placeholder() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_system_log".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_system_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `param` varchar(4000) DEFAULT NULL,
  `modul` varchar(100) DEFAULT NULL,
  `opration` varchar(1000) DEFAULT NULL,
  `operatorLogo` blob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "param".to_string(),
                "modul".to_string(),
                "opration".to_string(),
                "operatorLogo".to_string(),
            ],
        };

        assert_eq!(DamengSink::value_placeholder(&table_info, "param"), "?");
        assert_eq!(DamengSink::value_placeholder(&table_info, "opration"), "?");
        assert_eq!(DamengSink::value_placeholder(&table_info, "modul"), "?");
        assert_eq!(
            DamengSink::value_placeholder(&table_info, "operatorLogo"),
            "HEXTORAW(?)"
        );
    }

    #[test]
    fn blob_columns_use_hex_text_for_hextoraw() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_soss_operator".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_soss_operator` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `operatorLogo` blob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec!["id".to_string(), "operatorLogo".to_string()],
        };

        let value = Value::Blob(vec![0x1f, 0x8b, 0x08]);
        assert_eq!(
            DamengSink::value_to_param_for_column(&table_info, "operatorLogo", &value)
                .to_dm_value(),
            DmValue::Text("1F8B08".to_string())
        );
        assert_eq!(
            DamengSink::value_to_param_for_column(&table_info, "operatorLogo", &Value::None)
                .to_dm_value(),
            DmValue::Null
        );
    }
}
