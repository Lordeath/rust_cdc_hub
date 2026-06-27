use async_trait::async_trait;
use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecString,
};
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_column_is_auto_increment_from_definition,
    mysql_primary_key_columns_from_create_table_sql, mysql_type_token_from_column_definition,
};
use common::{
    CdcConfig, DataBuffer, FlushByOperation, ForeignKeyInfo, MySqlRoutineDefinition,
    MySqlRoutineKind, Operation, Sink, TableInfoVo, Value, database_table_key,
    fetch_mysql_routines, get_mysql_pool_by_url, mysql_connection_url_from_config,
};
use dameng::Client;
use dameng::ToDmValue;
use dameng_types::DmValue;
use mysql_to_dameng::convert_mysql_routine_to_dameng_with_name;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

const DAMENG_INLINE_STRING_CHAR_LIMIT: u32 = 512;
const DAMENG_DECIMAL_MAX_PRECISION: u32 = 38;
const DAMENG_BLOB_WRITE_CHUNK_SIZE: usize = 1000;
const DAMENG_IDENTIFIER_CHAR_LIMIT: usize = 128;

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
    default_value: Option<String>,
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
    sync_foreign_key_tables: bool,
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
            sync_foreign_key_tables: config.sync_foreign_key_tables_enabled(),
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
        if config.sync_stored_procedure_enabled() {
            sink.sync_stored_routines(config)
                .await
                .unwrap_or_else(|e| panic!("Dameng sync stored routines failed: {}", e));
        }
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
            self.ensure_column_comments(schema.as_str(), table_info)
                .await
                .map_err(|e| {
                    format!(
                        "Dameng ensure column comments failed: {} {}",
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

    async fn ensure_column_comments(
        &self,
        schema: &str,
        table_info: &TableInfoVo,
    ) -> Result<(), String> {
        let expected_comments = Self::mysql_column_comments(table_info);
        if expected_comments.is_empty() {
            return Ok(());
        }

        let existing_comments = self
            .existing_column_comments(schema, table_info.table_name.as_str())
            .await?;
        if existing_comments.is_empty() {
            warn!(
                "Dameng target table comment metadata is empty, skip comment check: {}",
                Self::target_table_key(schema, table_info.table_name.as_str())
            );
            return Ok(());
        }

        for src_col in &table_info.columns {
            let target_col = Self::target_column_name(src_col);
            let target_key = target_col.to_ascii_lowercase();
            let Some(expected_comment) = expected_comments.get(target_key.as_str()) else {
                continue;
            };
            match Self::column_comment_needs_update(
                &existing_comments,
                target_key.as_str(),
                expected_comment.as_str(),
            ) {
                None => {
                    warn!(
                        "Dameng skip comment for missing target column: {} {}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col
                    );
                    continue;
                }
                Some(false) => continue,
                Some(true) => {}
            }

            let sql = Self::comment_on_column_sql(
                schema,
                table_info.table_name.as_str(),
                target_col.as_str(),
                expected_comment.as_str(),
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto comment column success: {} {}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    src_col
                ),
                Err(e) => {
                    let msg = format!(
                        "Dameng auto comment column failed: {} {} {}",
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

    fn column_comment_needs_update(
        existing_comments: &HashMap<String, String>,
        target_key: &str,
        expected_comment: &str,
    ) -> Option<bool> {
        existing_comments
            .get(target_key)
            .map(|existing_comment| existing_comment != expected_comment)
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

    async fn sync_stored_routines(&self, config: &CdcConfig) -> Result<(), String> {
        let overwrite = config.overwrite_stored_procedure_enabled();
        let source_databases = config.source_databases();
        for (source_index, source_database) in source_databases.iter().enumerate() {
            if source_database.trim().is_empty() {
                continue;
            }
            let source_config_index = if config.multi_mode_open() {
                0
            } else {
                source_index
            };
            let mut target_schema = config.target_database_for_source(source_database);
            if target_schema.trim().is_empty() {
                target_schema = self.schema.clone();
            }
            let source_url = mysql_connection_url_from_config(
                &config.source_config[source_config_index],
                Some(source_database),
            );
            let source_pool =
                get_mysql_pool_by_url(&source_url, "dameng sink 同步存储程序-读取源库存储程序")
                    .await?;
            let routines = fetch_mysql_routines(
                &source_pool,
                source_database,
                &[MySqlRoutineKind::Function, MySqlRoutineKind::Procedure],
            )
            .await
            .map_err(|e| {
                format!(
                    "fetch source stored routines failed: {} -> {} {}",
                    source_database, target_schema, e
                )
            })?;
            if routines.is_empty() {
                info!("MySQL source stored routine not found: {}", source_database);
                continue;
            }

            for routine in routines {
                self.sync_one_stored_routine(
                    source_database,
                    target_schema.as_str(),
                    &routine,
                    overwrite,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn sync_one_stored_routine(
        &self,
        source_database: &str,
        target_schema: &str,
        routine: &MySqlRoutineDefinition,
        overwrite: bool,
    ) -> Result<(), String> {
        let mut target_routine_name = routine.name.clone();
        let exists = self
            .dameng_routine_exists(target_schema, routine.kind, routine.name.as_str())
            .await?;
        if exists && !overwrite {
            info!(
                "Dameng stored routine exists, skip: {} {}.{} -> {}.{}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                routine.name
            );
            return Ok(());
        }
        if !exists
            && self
                .dameng_object_name_exists(target_schema, routine.name.as_str())
                .await?
        {
            let (renamed_routine_name, renamed_exists) = self
                .dameng_conflict_routine_name(target_schema, routine.kind, routine.name.as_str())
                .await?;
            if renamed_exists && !overwrite {
                info!(
                    "Dameng renamed stored routine exists, skip: {} {}.{} -> {}.{} original_target={}.{}",
                    routine.kind.routine_type(),
                    source_database,
                    routine.name,
                    target_schema,
                    renamed_routine_name,
                    target_schema,
                    routine.name
                );
                return Ok(());
            }
            warn!(
                "Dameng object name exists, rename stored routine: {} {}.{} -> {}.{} original_target={}.{}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                renamed_routine_name,
                target_schema,
                routine.name
            );
            target_routine_name = renamed_routine_name;
        }

        let create_sql = convert_mysql_routine_to_dameng_with_name(
            target_schema,
            Some(target_routine_name.as_str()),
            routine,
        )
        .map_err(|e| {
            format!(
                "convert stored routine failed: {} {}.{} -> {}.{} {}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                target_routine_name,
                e
            )
        })?;
        self.execute(create_sql.as_str()).await.map_err(|e| {
            format!(
                "create Dameng stored routine failed: {} {}.{} -> {}.{} overwrite={} sql={} error={}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                target_routine_name,
                overwrite,
                Self::sql_preview(create_sql.as_str()),
                e
            )
        })?;
        info!(
            "Dameng sync stored routine success: {} {}.{} -> {}.{} overwrite={}",
            routine.kind.routine_type(),
            source_database,
            routine.name,
            target_schema,
            target_routine_name,
            overwrite
        );
        Ok(())
    }

    async fn dameng_conflict_routine_name(
        &self,
        schema: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> Result<(String, bool), String> {
        for index in 0..100 {
            let candidate = Self::conflict_routine_name(routine_name, routine_kind, index);
            if self
                .dameng_routine_exists(schema, routine_kind, candidate.as_str())
                .await?
            {
                return Ok((candidate, true));
            }
            if !self
                .dameng_object_name_exists(schema, candidate.as_str())
                .await?
            {
                return Ok((candidate, false));
            }
        }
        Err(format!(
            "no available Dameng routine name for object conflict: {}.{} {}",
            schema,
            routine_name,
            routine_kind.routine_type()
        ))
    }

    fn conflict_routine_name(
        routine_name: &str,
        routine_kind: MySqlRoutineKind,
        index: usize,
    ) -> String {
        let suffix = match routine_kind {
            MySqlRoutineKind::Procedure => "_procedure",
            MySqlRoutineKind::Function => "_function",
        };
        let suffix = if index == 0 {
            suffix.to_string()
        } else {
            format!("{}_{}", suffix, index + 1)
        };
        Self::identifier_with_suffix(routine_name, suffix.as_str())
    }

    fn identifier_with_suffix(identifier: &str, suffix: &str) -> String {
        let suffix_len = suffix.chars().count();
        let prefix_len = DAMENG_IDENTIFIER_CHAR_LIMIT.saturating_sub(suffix_len);
        let prefix = identifier.chars().take(prefix_len).collect::<String>();
        format!("{}{}", prefix, suffix)
    }

    async fn dameng_routine_exists(
        &self,
        schema: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> Result<bool, String> {
        let sql = Self::dameng_routine_exists_sql(schema, routine_kind, routine_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn dameng_object_name_exists(
        &self,
        schema: &str,
        object_name: &str,
    ) -> Result<bool, String> {
        let sql = Self::dameng_object_name_exists_sql(schema, object_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    fn dameng_routine_exists_sql(
        schema: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> String {
        let object_type = routine_kind.routine_type();
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = {} AND {}",
                Self::quote_literal(object_type),
                Self::eq_original_or_upper_sql("OBJECT_NAME", routine_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_OBJECTS WHERE {} AND OBJECT_TYPE = {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::quote_literal(object_type),
                Self::eq_original_or_upper_sql("OBJECT_NAME", routine_name)
            )
        }
    }

    fn dameng_object_name_exists_sql(schema: &str, object_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE {}",
                Self::eq_original_or_upper_sql("OBJECT_NAME", object_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_OBJECTS WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("OBJECT_NAME", object_name)
            )
        }
    }

    fn sql_preview(sql: &str) -> String {
        let mut compact = sql
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if compact.len() > 500 {
            compact = compact.chars().take(500).collect();
            compact.push_str("...");
        }
        compact
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

        let sql = Self::create_table_sql(schema, table_info)?;
        self.execute(sql.as_str()).await?;
        info!(
            "Dameng auto create table success: {}",
            Self::target_table_key(schema, table_info.table_name.as_str())
        );
        Ok(())
    }

    fn create_table_sql(schema: &str, table_info: &TableInfoVo) -> Result<String, String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let primary_key_columns = Self::source_primary_key_columns(table_info);
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
            if Self::contains_source_column(&primary_key_columns, src_col) {
                nullable = false;
            }
            let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
            let auto_increment = mysql_column_is_auto_increment_from_definition(def.as_str());
            cols_sql.push(format!(
                "{} {}",
                Self::quote_column_ident(src_col),
                Self::dameng_column_definition(
                    mysql_type.as_str(),
                    def.as_str(),
                    nullable_sql,
                    auto_increment
                ),
            ));
        }
        if cols_sql.is_empty() {
            return Err("no columns parsed from source create table sql".to_string());
        }
        if !primary_key_columns.is_empty() {
            let primary_key_sql = primary_key_columns
                .iter()
                .map(|column| Self::quote_column_ident(column))
                .collect::<Vec<_>>()
                .join(", ");
            cols_sql.push(format!("PRIMARY KEY ({})", primary_key_sql));
        }
        let sql = format!(
            "CREATE TABLE {} ({})",
            Self::qualified_table(schema, table_info.table_name.as_str()),
            cols_sql.join(", ")
        );
        Ok(sql)
    }

    fn source_primary_key_columns(table_info: &TableInfoVo) -> Vec<String> {
        mysql_primary_key_columns_from_create_table_sql(table_info.create_table_sql.as_str())
    }

    fn contains_source_column(columns: &[String], column_name: &str) -> bool {
        columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case(column_name))
    }

    fn is_protected_source_column_for_auto_modify(
        primary_key_columns: &[String],
        column_name: &str,
        column_definition: &str,
    ) -> bool {
        Self::contains_source_column(primary_key_columns, column_name)
            || mysql_column_is_auto_increment_from_definition(column_definition)
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
        let primary_key_columns = Self::source_primary_key_columns(table_info);
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
            let add_column_definition = Self::dameng_column_definition(
                mysql_type.as_str(),
                def.as_str(),
                nullable_sql,
                false,
            );
            if let Some(existing_col) = existing_cols.get(target_key.as_str()) {
                let should_modify =
                    Self::should_modify_existing_column(&mysql_type, def.as_str(), existing_col);
                let default_mismatch =
                    Self::existing_column_default_mismatch(&mysql_type, def.as_str(), existing_col);
                if dameng_type.eq_ignore_ascii_case("CLOB") || should_modify || default_mismatch {
                    info!(
                        "Dameng column check: {}.{} existing={} data_length={} char_length={} default={:?} expected={} auto_modify={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type,
                        self.auto_modify_column
                    );
                }
                if default_mismatch {
                    warn!(
                        "Dameng skip auto modify default mismatch: {}.{} existing_default={:?} expected_default={:?}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.default_value,
                        Self::mysql_column_default_value_sql(mysql_type.as_str(), def.as_str())
                    );
                }
                if should_modify && !self.auto_modify_column {
                    return Err(format!(
                        "Dameng column type mismatch and auto_modify_column is false: {}.{} existing={} data_length={} char_length={} default={:?} expected={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type
                    ));
                }
                if should_modify
                    && Self::is_protected_source_column_for_auto_modify(
                        &primary_key_columns,
                        src_col,
                        def.as_str(),
                    )
                {
                    warn!(
                        "Dameng skip auto modify protected column: {}.{} existing={} data_length={} char_length={} default={:?} expected={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type
                    );
                    continue;
                }
                if should_modify {
                    let sql = format!(
                        "ALTER TABLE {} MODIFY {} {}",
                        Self::qualified_table(schema, table_info.table_name.as_str()),
                        Self::quote_ident(target_col.as_str()),
                        Self::dameng_modify_column_definition(mysql_type.as_str())
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
                "ALTER TABLE {} ADD {} {}",
                Self::qualified_table(schema, table_info.table_name.as_str()),
                Self::quote_ident(target_col.as_str()),
                add_column_definition
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
        let sql = Self::columns_metadata_sql(schema, table_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let metadata = rows
            .iter()
            .filter_map(|row| row.get::<String>(0).ok())
            .collect::<Vec<_>>()
            .join("\x1e");
        Ok(Self::parse_columns_metadata(metadata.as_str()))
    }

    fn columns_metadata_sql(schema: &str, table_name: &str) -> String {
        let expr = "COLUMN_NAME || CHR(31) || DATA_TYPE || CHR(31) || DATA_LENGTH || CHR(31) || CHAR_LENGTH || CHR(31) || NVL(REPLACE(REPLACE(TRIM(DATA_DEFAULT), CHR(30), ' '), CHR(31), ' '), '')";
        if schema.is_empty() {
            format!(
                "SELECT {} FROM USER_TAB_COLUMNS WHERE {} ORDER BY COLUMN_ID",
                expr,
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT {} FROM ALL_TAB_COLUMNS WHERE {} AND {} ORDER BY COLUMN_ID",
                expr,
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        }
    }

    async fn existing_column_comments(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<HashMap<String, String>, String> {
        if !schema.is_empty() {
            let dba_sql = Self::dba_column_comments_sql(schema, table_name);
            match self.query(dba_sql.as_str(), &[]).await {
                Ok(rows) => {
                    let comments = Self::column_comments_from_rows(&rows);
                    if !comments.is_empty() {
                        return Ok(comments);
                    }
                    warn!(
                        "Dameng DBA_COL_COMMENTS returned no column comments, fallback to ALL_COL_COMMENTS: {}",
                        Self::target_table_key(schema, table_name)
                    );
                }
                Err(e) => warn!(
                    "Dameng DBA_COL_COMMENTS query failed, fallback to ALL_COL_COMMENTS: {} {}",
                    Self::target_table_key(schema, table_name),
                    e
                ),
            }
        }

        let sql = Self::all_column_comments_sql(schema, table_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        Ok(Self::column_comments_from_rows(&rows))
    }

    fn column_comments_from_rows(rows: &dameng::ResultSet) -> HashMap<String, String> {
        let metadata = rows
            .iter()
            .filter_map(|row| row.get::<String>(0).ok())
            .collect::<Vec<_>>()
            .join("\x1e");
        Self::parse_column_comments_metadata(metadata.as_str())
    }

    fn dba_column_comments_sql(schema: &str, table_name: &str) -> String {
        format!(
            "SELECT c.COLUMN_NAME || CHR(31) || NVL(REPLACE(REPLACE(c.COMMENTS, CHR(30), ' '), CHR(31), ' '), '') FROM DBA_COL_COMMENTS c WHERE {} AND {} ORDER BY c.COLUMN_NAME",
            Self::eq_original_or_upper_sql("c.OWNER", schema),
            Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name)
        )
    }

    fn all_column_comments_sql(schema: &str, table_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT c.COLUMN_NAME || CHR(31) || NVL(REPLACE(REPLACE(cc.COMMENTS, CHR(30), ' '), CHR(31), ' '), '') FROM USER_TAB_COLUMNS c LEFT JOIN USER_COL_COMMENTS cc ON cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME WHERE {} ORDER BY c.COLUMN_ID",
                Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT c.COLUMN_NAME || CHR(31) || NVL(REPLACE(REPLACE(cc.COMMENTS, CHR(30), ' '), CHR(31), ' '), '') FROM ALL_TAB_COLUMNS c LEFT JOIN ALL_COL_COMMENTS cc ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME WHERE {} AND {} ORDER BY c.COLUMN_ID",
                Self::eq_original_or_upper_sql("c.OWNER", schema),
                Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name)
            )
        }
    }

    fn parse_columns_metadata(metadata: &str) -> HashMap<String, DamengColumnInfo> {
        let mut cols = HashMap::new();
        let row_separator = if metadata.contains('\x1e') || metadata.contains('\x1f') {
            '\x1e'
        } else {
            '|'
        };
        for column in metadata.split(row_separator).filter(|s| !s.is_empty()) {
            let field_separator = if column.contains('\x1f') { '\x1f' } else { ':' };
            if let Some((name, info)) = Self::parse_column_metadata(column, field_separator) {
                cols.insert(name.to_ascii_lowercase(), info);
            }
        }
        cols
    }

    fn parse_column_metadata(
        column: &str,
        field_separator: char,
    ) -> Option<(&str, DamengColumnInfo)> {
        let mut parts = column.splitn(5, field_separator);
        let name = match parts.next() {
            Some(v) if !v.is_empty() => v,
            _ => return None,
        };
        let data_type = match parts.next() {
            Some(v) if !v.is_empty() => v,
            _ => return None,
        };
        let data_length = parts
            .next()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let char_length = parts
            .next()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let default_value = parts
            .next()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string);
        Some((
            name,
            DamengColumnInfo {
                data_type: data_type.to_string(),
                data_length,
                char_length,
                default_value,
            },
        ))
    }

    fn parse_column_comments_metadata(metadata: &str) -> HashMap<String, String> {
        let mut comments = HashMap::new();
        for column in metadata.split('\x1e').filter(|s| !s.is_empty()) {
            let Some((name, comment)) = column.split_once('\x1f') else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            comments.insert(name.to_ascii_lowercase(), comment.to_string());
        }
        comments
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
        if !sql.is_ascii() {
            return self.execute_inline_params(sql, params, false).await;
        }

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
        if !sql.is_ascii() {
            return self.execute_inline_params(sql, params, true).await;
        }

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

    async fn execute_inline_params(
        &self,
        sql: &str,
        params: &[DamengParam],
        insert_control_flow_passthrough: bool,
    ) -> Result<u64, String> {
        let inline_sql = Self::inline_sql_params(sql, params)?;
        let first_result = {
            let mut client = self.client.lock().await;
            client.execute(inline_sql.as_str())
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if insert_control_flow_passthrough
                    && Self::is_insert_control_flow_error(msg.as_str())
                {
                    return Err(msg);
                }
                error!(
                    "Dameng execute inline params failed, retrying: {} sql: {}",
                    msg, sql
                );
                self.reconnect().await?;
                self.client
                    .lock()
                    .await
                    .execute(inline_sql.as_str())
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

    fn inline_sql_params(sql: &str, params: &[DamengParam]) -> Result<String, String> {
        let mut result = String::with_capacity(sql.len() + params.len() * 8);
        let mut param_index = 0;
        let mut quote: Option<char> = None;
        let mut chars = sql.chars().peekable();

        while let Some(ch) = chars.next() {
            if let Some(quote_char) = quote {
                result.push(ch);
                if ch == quote_char {
                    if chars.peek() == Some(&quote_char) {
                        result.push(chars.next().unwrap());
                    } else {
                        quote = None;
                    }
                }
                continue;
            }

            match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    result.push(ch);
                }
                '?' => {
                    let Some(param) = params.get(param_index) else {
                        return Err(format!(
                            "Dameng inline params placeholder count exceeds params: sql={}",
                            sql
                        ));
                    };
                    result.push_str(Self::dameng_param_sql_literal(param).as_str());
                    param_index += 1;
                }
                _ => result.push(ch),
            }
        }

        if param_index != params.len() {
            return Err(format!(
                "Dameng inline params count mismatch: placeholders={} params={} sql={}",
                param_index,
                params.len(),
                sql
            ));
        }
        Ok(result)
    }

    fn dameng_param_sql_literal(param: &DamengParam) -> String {
        match param {
            DamengParam::Null => "NULL".to_string(),
            DamengParam::Bool(v) => {
                if *v {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            DamengParam::I8(v) => v.to_string(),
            DamengParam::I16(v) => v.to_string(),
            DamengParam::I32(v) => v.to_string(),
            DamengParam::I64(v) => v.to_string(),
            DamengParam::F32(v) => v.to_string(),
            DamengParam::F64(v) => v.to_string(),
            DamengParam::Text(v) => Self::quote_literal(v),
            DamengParam::Bytes(v) => {
                format!("HEXTORAW({})", Self::quote_literal(&Self::bytes_to_hex(v)))
            }
        }
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

    fn foreign_key_sql(schema: &str, foreign_key: &ForeignKeyInfo) -> Option<String> {
        if foreign_key.columns.is_empty()
            || foreign_key.columns.len() != foreign_key.referenced_columns.len()
        {
            return None;
        }
        let columns = foreign_key
            .columns
            .iter()
            .map(|column| Self::quote_column_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let referenced_columns = foreign_key
            .referenced_columns
            .iter()
            .map(|column| Self::quote_column_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
            Self::qualified_table(schema, foreign_key.table_name.as_str()),
            Self::quote_ident(foreign_key.constraint_name.as_str()),
            columns,
            Self::qualified_table(schema, foreign_key.referenced_table_name.as_str()),
            referenced_columns
        );
        if let Some(delete_rule) = Self::dameng_fk_action(foreign_key.delete_rule.as_deref()) {
            sql.push_str(" ON DELETE ");
            sql.push_str(delete_rule);
        }
        if let Some(update_rule) = Self::dameng_fk_action(foreign_key.update_rule.as_deref()) {
            sql.push_str(" ON UPDATE ");
            sql.push_str(update_rule);
        }
        sql.push_str(" WITH INDEX");
        Some(sql)
    }

    fn dameng_fk_action(rule: Option<&str>) -> Option<&'static str> {
        match rule.unwrap_or("").trim().to_ascii_uppercase().as_str() {
            "CASCADE" => Some("CASCADE"),
            "SET NULL" => Some("SET NULL"),
            "SET DEFAULT" => Some("SET DEFAULT"),
            "NO ACTION" | "RESTRICT" => Some("NO ACTION"),
            _ => None,
        }
    }

    async fn foreign_key_exists(
        &self,
        schema: &str,
        table_name: &str,
        constraint_name: &str,
    ) -> Result<bool, String> {
        let sql = if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE {} AND {} AND CONSTRAINT_TYPE = 'R'",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name),
                Self::eq_original_or_upper_sql("CONSTRAINT_NAME", constraint_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_CONSTRAINTS WHERE {} AND {} AND {} AND CONSTRAINT_TYPE = 'R'",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name),
                Self::eq_original_or_upper_sql("CONSTRAINT_NAME", constraint_name)
            )
        };
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn ensure_foreign_keys(&self) -> Result<(), String> {
        if !self.sync_foreign_key_tables {
            return Ok(());
        }
        for table_info in &self.table_info_list {
            if table_info.foreign_keys.is_empty() {
                continue;
            }
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            for foreign_key in &table_info.foreign_keys {
                if self
                    .foreign_key_exists(
                        schema.as_str(),
                        table_info.table_name.as_str(),
                        foreign_key.constraint_name.as_str(),
                    )
                    .await?
                {
                    continue;
                }
                let Some(sql) = Self::foreign_key_sql(schema.as_str(), foreign_key) else {
                    warn!(
                        "Dameng skip invalid foreign key metadata: {}.{}",
                        table_info.table_name, foreign_key.constraint_name
                    );
                    continue;
                };
                match self.try_execute_optional(sql.as_str()).await {
                    Ok(_) => info!(
                        "Dameng auto add foreign key success: {}.{}",
                        schema, foreign_key.constraint_name
                    ),
                    Err(e) => warn!(
                        "Dameng skip foreign key after add failed: {}.{} {}",
                        schema, foreign_key.constraint_name, e
                    ),
                }
            }
        }
        Ok(())
    }

    async fn try_execute_optional(&self, sql: &str) -> Result<u64, String> {
        self.client
            .lock()
            .await
            .execute(sql)
            .map_err(|e| e.to_string())
    }

    fn qualified_column(schema: &str, table_name: &str, column_name: &str) -> String {
        format!(
            "{}.{}",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(column_name)
        )
    }

    fn comment_on_column_sql(
        schema: &str,
        table_name: &str,
        column_name: &str,
        comment: &str,
    ) -> String {
        format!(
            "COMMENT ON COLUMN {} IS {}",
            Self::qualified_column(schema, table_name, column_name),
            Self::quote_literal(comment)
        )
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

    fn dameng_column_definition(
        mysql_type_token: &str,
        mysql_column_definition: &str,
        nullable_sql: &str,
        auto_increment: bool,
    ) -> String {
        let mut parts = vec![Self::map_mysql_type_to_dameng_for_column(
            mysql_type_token,
            auto_increment,
        )];
        if !nullable_sql.trim().is_empty() {
            parts.push(nullable_sql.to_string());
        }
        if let Some(default_sql) =
            Self::mysql_column_default_sql(mysql_type_token, mysql_column_definition)
        {
            parts.push(default_sql);
        }
        parts.join(" ")
    }

    fn dameng_modify_column_definition(mysql_type_token: &str) -> String {
        Self::map_mysql_type_to_dameng_for_column(mysql_type_token, false)
    }

    fn mysql_column_default_sql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
    ) -> Option<String> {
        let default_value = Self::mysql_default_value_expression(mysql_column_definition)?;
        if Self::mysql_default_is_null(default_value.as_str()) {
            return None;
        }

        let t = mysql_type_token.to_ascii_lowercase();
        if (t.starts_with("timestamp") || t.starts_with("datetime"))
            && Self::mysql_default_is_current_timestamp(default_value.as_str())
        {
            return Some("DEFAULT CURRENT_TIMESTAMP".to_string());
        }

        let dameng_type = Self::map_mysql_type_to_dameng(mysql_type_token);
        if dameng_type.eq_ignore_ascii_case("CLOB") || dameng_type.eq_ignore_ascii_case("BLOB") {
            return None;
        }

        let default_literal =
            Self::mysql_default_literal_sql(mysql_type_token, default_value.as_str())?;
        Some(format!("DEFAULT {}", default_literal))
    }

    fn mysql_column_comments(table_info: &TableInfoVo) -> HashMap<String, String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let mut comments = HashMap::new();
        for src_col in &table_info.columns {
            let key = src_col.to_ascii_lowercase();
            let Some(def) = defs.get(key.as_str()) else {
                continue;
            };
            let Some(comment) = Self::mysql_column_comment(def.as_str()) else {
                continue;
            };
            comments.insert(
                Self::target_column_name(src_col).to_ascii_lowercase(),
                comment,
            );
        }
        comments
    }

    fn mysql_column_comment(mysql_column_definition: &str) -> Option<String> {
        let comment_pos = Self::find_keyword_outside_quotes(mysql_column_definition, 0, "comment")?;
        let rest = mysql_column_definition[comment_pos + "comment".len()..].trim_start();
        Self::mysql_string_literal_content(rest)
    }

    fn mysql_current_timestamp_default_sql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
    ) -> Option<&'static str> {
        let t = mysql_type_token.to_ascii_lowercase();
        if !t.starts_with("timestamp") && !t.starts_with("datetime") {
            return None;
        }
        let Some(default_value) = Self::mysql_default_value_expression(mysql_column_definition)
        else {
            return None;
        };
        if Self::mysql_default_is_current_timestamp(default_value.as_str()) {
            Some("DEFAULT CURRENT_TIMESTAMP")
        } else {
            None
        }
    }

    fn mysql_default_value_expression(definition: &str) -> Option<String> {
        let Some(default_pos) = Self::find_keyword_outside_quotes(definition, 0, "default") else {
            return None;
        };
        let rest = definition[default_pos + "default".len()..].trim_start();
        Self::take_mysql_default_expression(rest).map(ToString::to_string)
    }

    fn take_mysql_default_expression(rest: &str) -> Option<&str> {
        let rest = rest.trim_start();
        if rest.is_empty() {
            return None;
        }

        let bytes = rest.as_bytes();
        let mut quote: Option<u8> = None;
        let mut paren_depth = 0u32;
        let mut pos = 0usize;
        let mut end = bytes.len();

        while pos < bytes.len() {
            let byte = bytes[pos];
            if let Some(quote_byte) = quote {
                if byte == b'\\' {
                    pos = (pos + 2).min(bytes.len());
                    continue;
                }
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

            match byte {
                b'\'' | b'"' | b'`' => {
                    quote = Some(byte);
                    pos += 1;
                }
                b'(' => {
                    paren_depth += 1;
                    pos += 1;
                }
                b')' => {
                    paren_depth = paren_depth.saturating_sub(1);
                    pos += 1;
                }
                b if b.is_ascii_whitespace() && paren_depth == 0 => {
                    let next = Self::skip_ascii_whitespace(rest, pos);
                    if next < bytes.len() && Self::is_mysql_default_tail_keyword(rest, next) {
                        end = pos;
                        break;
                    }
                    pos += 1;
                }
                _ => pos += 1,
            }
        }

        let value = rest[..end].trim_end();
        if value.is_empty() { None } else { Some(value) }
    }

    fn skip_ascii_whitespace(value: &str, start: usize) -> usize {
        let bytes = value.as_bytes();
        let mut pos = start;
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        pos
    }

    fn is_mysql_default_tail_keyword(value: &str, pos: usize) -> bool {
        const TAIL_KEYWORDS: &[&str] = &[
            "comment",
            "on",
            "not",
            "null",
            "auto_increment",
            "primary",
            "unique",
            "collate",
            "character",
            "charset",
            "check",
            "constraint",
            "references",
            "visible",
            "invisible",
        ];
        TAIL_KEYWORDS
            .iter()
            .any(|keyword| Self::starts_with_keyword(value, pos, keyword))
    }

    fn mysql_default_is_null(default_value: &str) -> bool {
        default_value.trim().eq_ignore_ascii_case("null")
    }

    fn mysql_default_is_current_timestamp(default_value: &str) -> bool {
        let default_lower = default_value.trim().to_ascii_lowercase();
        let Some(tail) = default_lower.strip_prefix("current_timestamp") else {
            return false;
        };
        Self::current_timestamp_default_tail_ok(tail)
    }

    fn current_timestamp_default_tail_ok(tail: &str) -> bool {
        if tail.is_empty() {
            return true;
        }
        if tail.chars().next().is_some_and(char::is_whitespace) {
            return true;
        }
        if let Some(rest) = tail.strip_prefix('(') {
            let Some(end) = rest.find(')') else {
                return false;
            };
            return rest[..end].chars().all(|c| c.is_ascii_digit());
        }
        !tail
            .as_bytes()
            .first()
            .is_some_and(|byte| Self::is_ascii_identifier_byte(*byte))
    }

    fn mysql_default_literal_sql(mysql_type_token: &str, default_value: &str) -> Option<String> {
        let t = mysql_type_token.to_ascii_lowercase();
        if Self::mysql_numeric_or_boolean_type(t.as_str()) {
            return Self::mysql_numeric_default_literal_sql(default_value);
        }
        if t.starts_with("varchar") || t.starts_with("char") {
            let value = Self::mysql_string_literal_content(default_value)?;
            return Some(Self::quote_literal(value.as_str()));
        }
        None
    }

    fn mysql_numeric_or_boolean_type(mysql_type_token: &str) -> bool {
        mysql_type_token.starts_with("tinyint")
            || mysql_type_token.starts_with("smallint")
            || mysql_type_token.starts_with("mediumint")
            || mysql_type_token.starts_with("int")
            || mysql_type_token.starts_with("integer")
            || mysql_type_token.starts_with("bigint")
            || mysql_type_token.starts_with("float")
            || mysql_type_token.starts_with("double")
            || mysql_type_token.starts_with("real")
            || mysql_type_token.starts_with("decimal")
            || mysql_type_token.starts_with("numeric")
            || mysql_type_token.starts_with("bit")
            || mysql_type_token.starts_with("bool")
            || mysql_type_token.starts_with("boolean")
    }

    fn mysql_numeric_default_literal_sql(default_value: &str) -> Option<String> {
        let value = default_value.trim();
        if let Some(bit_value) = Self::mysql_bit_literal_to_decimal(value) {
            return Some(bit_value);
        }

        let unquoted =
            Self::mysql_string_literal_content(value).unwrap_or_else(|| value.to_string());
        let normalized = unquoted.trim();
        if normalized.eq_ignore_ascii_case("true") {
            return Some("1".to_string());
        }
        if normalized.eq_ignore_ascii_case("false") {
            return Some("0".to_string());
        }
        if Self::is_numeric_literal(normalized) {
            return Some(normalized.to_string());
        }
        None
    }

    fn mysql_bit_literal_to_decimal(value: &str) -> Option<String> {
        let value = value.trim();
        if value.len() < 4 || !value[..2].eq_ignore_ascii_case("b'") || !value.ends_with('\'') {
            return None;
        }
        let bits = &value[2..value.len() - 1];
        if bits.is_empty() || !bits.bytes().all(|b| matches!(b, b'0' | b'1')) {
            return None;
        }
        u128::from_str_radix(bits, 2).ok().map(|v| v.to_string())
    }

    fn is_numeric_literal(value: &str) -> bool {
        let value = value.trim();
        if value.is_empty() {
            return false;
        }
        let mut has_digit = false;
        for (idx, byte) in value.bytes().enumerate() {
            if byte.is_ascii_digit() {
                has_digit = true;
                continue;
            }
            match byte {
                b'+' | b'-' if idx == 0 => {}
                b'+' | b'-'
                    if matches!(value.as_bytes().get(idx.wrapping_sub(1)), Some(b'e' | b'E')) => {}
                b'.' | b'e' | b'E' => {}
                _ => return false,
            }
        }
        has_digit && value.parse::<f64>().is_ok()
    }

    fn mysql_string_literal_content(default_value: &str) -> Option<String> {
        let value = default_value.trim();
        let bytes = value.as_bytes();
        let quote_pos = if matches!(bytes.first(), Some(b'\'' | b'"')) {
            0
        } else if bytes.len() >= 2
            && matches!(bytes[0], b'n' | b'N')
            && matches!(bytes[1], b'\'' | b'"')
        {
            1
        } else if bytes.first() == Some(&b'_') {
            value.find(|c| matches!(c, '\'' | '"')).filter(|idx| {
                value[..*idx]
                    .bytes()
                    .all(|b| b == b'_' || b.is_ascii_alphanumeric())
            })?
        } else {
            return None;
        };

        let quote = bytes[quote_pos];
        let mut result = Vec::new();
        let mut pos = quote_pos + 1;
        while pos < bytes.len() {
            let byte = bytes[pos];
            if byte == b'\\' {
                let Some(next) = bytes.get(pos + 1) else {
                    return None;
                };
                result.push(*next);
                pos += 2;
                continue;
            }
            if byte == quote {
                if bytes.get(pos + 1) == Some(&quote) {
                    result.push(quote);
                    pos += 2;
                    continue;
                }
                if value[pos + 1..].trim().is_empty() {
                    return Some(String::from_utf8_lossy(result.as_slice()).into_owned());
                }
                return None;
            }
            result.push(byte);
            pos += 1;
        }
        None
    }

    fn dameng_default_is_current_timestamp(default_value: Option<&str>) -> bool {
        let Some(default_value) = default_value else {
            return false;
        };
        let normalized = default_value
            .trim()
            .trim_matches(|c| c == '(' || c == ')')
            .to_ascii_uppercase();
        normalized == "CURRENT_TIMESTAMP" || normalized.starts_with("CURRENT_TIMESTAMP(")
    }

    fn dameng_default_matches_mysql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
        default_value: Option<&str>,
    ) -> bool {
        let Some(expected_default) =
            Self::mysql_column_default_value_sql(mysql_type_token, mysql_column_definition)
        else {
            return true;
        };
        let Some(default_value) = default_value else {
            return false;
        };
        Self::normalize_default_for_compare(expected_default.as_str())
            == Self::normalize_default_for_compare(default_value)
    }

    fn existing_column_default_mismatch(
        mysql_type_token: &str,
        mysql_column_definition: &str,
        existing_col: &DamengColumnInfo,
    ) -> bool {
        !Self::dameng_default_matches_mysql(
            mysql_type_token,
            mysql_column_definition,
            existing_col.default_value.as_deref(),
        ) || (Self::mysql_current_timestamp_default_sql(mysql_type_token, mysql_column_definition)
            .is_some()
            && !Self::dameng_default_is_current_timestamp(existing_col.default_value.as_deref()))
    }

    fn mysql_column_default_value_sql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
    ) -> Option<String> {
        Self::mysql_column_default_sql(mysql_type_token, mysql_column_definition).and_then(|sql| {
            sql.trim_start()
                .strip_prefix("DEFAULT")
                .map(|value| value.trim_start().to_string())
        })
    }

    fn normalize_default_for_compare(default_value: &str) -> String {
        let value = default_value.trim();
        let value = if Self::starts_with_keyword(value, 0, "default") {
            value["default".len()..].trim_start()
        } else {
            value
        };
        if Self::mysql_default_is_current_timestamp(value) {
            return "CURRENT_TIMESTAMP".to_string();
        }
        let value = if value.starts_with('(') && value.ends_with(')') && value.len() > 2 {
            value[1..value.len() - 1].trim()
        } else {
            value
        };
        if Self::mysql_default_is_current_timestamp(value) {
            return "CURRENT_TIMESTAMP".to_string();
        }
        if let Some(string_value) = Self::mysql_string_literal_content(value) {
            return Self::quote_literal(string_value.as_str());
        }
        if Self::is_numeric_literal(value) {
            return value.to_string();
        }
        value.to_ascii_uppercase()
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
        let before_ok = pos == 0 || !Self::is_ascii_identifier_byte(bytes[pos - 1]);
        let after_pos = pos + keyword_bytes.len();
        let after_ok =
            after_pos >= bytes.len() || !Self::is_ascii_identifier_byte(bytes[after_pos]);
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
            if Self::starts_with_keyword(value, pos, keyword) {
                return Some(pos);
            }
            pos += 1;
        }
        None
    }

    fn is_ascii_identifier_byte(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_'
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

    fn dml_value_sql(table_info: &TableInfoVo, column_name: &str, value: &Value) -> &'static str {
        if Self::is_source_blob_column(table_info, column_name) && matches!(value, Value::Blob(_)) {
            "EMPTY_BLOB()"
        } else {
            "?"
        }
    }

    fn dml_value_param(
        table_info: &TableInfoVo,
        column_name: &str,
        value: &Value,
    ) -> Option<DamengParam> {
        if Self::is_source_blob_column(table_info, column_name) && matches!(value, Value::Blob(_)) {
            None
        } else if let Value::Blob(v) = value {
            Some(DamengParam::Text(String::from_utf8_lossy(v).to_string()))
        } else {
            Some(Self::value_to_param(value))
        }
    }

    fn bytes_to_hex(value: &[u8]) -> String {
        value.iter().map(|b| format!("{:02X}", b)).collect()
    }

    fn reset_blob_sql(schema: &str, table_name: &str, pk_name: &str, column_name: &str) -> String {
        format!(
            "UPDATE {} SET {} = EMPTY_BLOB() WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(column_name),
            Self::quote_column_ident(pk_name)
        )
    }

    fn append_blob_chunk_sql(
        schema: &str,
        table_name: &str,
        pk_name: &str,
        column_name: &str,
        chunk_len: usize,
    ) -> String {
        format!(
            "DECLARE\n  v_lob BLOB;\nBEGIN\n  SELECT {} INTO v_lob FROM {} WHERE {} = ? FOR UPDATE;\n  DBMS_LOB.WRITEAPPEND(v_lob, {}, HEXTORAW(?));\nEND;",
            Self::quote_column_ident(column_name),
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name),
            chunk_len
        )
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
                        (Self::table_primary_key_signature(table_info), signature),
                    );
                }
                Some((pk, existing_signature)) => {
                    if pk != &Self::table_primary_key_signature(table_info)
                        || existing_signature != &signature
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

    fn table_primary_key_signature(table_info: &TableInfoVo) -> String {
        Self::source_primary_key_columns(table_info)
            .iter()
            .map(|column| Self::target_column_name(column).to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(",")
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
                        let auto_increment =
                            mysql_column_is_auto_increment_from_definition(def.as_str());
                        let nullable_sql = "";
                        Some(Self::dameng_column_definition(
                            mysql_type.as_str(),
                            def.as_str(),
                            nullable_sql,
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
            Some(len) => format!("{}({} CHAR)", dameng_type, len.max(1)),
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
        _mysql_column_definition: &str,
        existing_col: &DamengColumnInfo,
    ) -> bool {
        let expected_type = Self::map_mysql_type_to_dameng(mysql_type_token);
        if existing_col.data_type.eq_ignore_ascii_case("CLOB")
            && Self::is_dameng_text_type(expected_type.as_str())
        {
            return false;
        }

        if expected_type == "CLOB" {
            return !existing_col.data_type.eq_ignore_ascii_case("CLOB");
        }

        let t = mysql_type_token.to_ascii_lowercase();
        if !t.starts_with("varchar") && !t.starts_with("char") {
            return false;
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

    fn is_dameng_text_type(dameng_type: &str) -> bool {
        let normalized = dameng_type.to_ascii_uppercase();
        normalized == "CLOB" || normalized.starts_with("VARCHAR") || normalized.starts_with("CHAR")
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

    async fn after_initialization(&mut self) -> Result<(), String> {
        self.flush_with_retry(&FlushByOperation::Init).await;
        self.ensure_foreign_keys().await
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

        self.insert_record(schema, table_name, table_key, pk_name, columns, record)
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
            .insert_record(schema, table_name, table_key, pk_name, columns, record)
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
                    Self::dml_value_sql(&table_info, c, record.after.get(c))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let update_sql = format!(
            "UPDATE {} SET {} WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            set_sql,
            Self::quote_column_ident(pk_name)
        );
        let mut update_params = Vec::with_capacity(update_columns.len() + 1);
        for col in update_columns {
            if let Some(param) = Self::dml_value_param(&table_info, col, record.after.get(col)) {
                update_params.push(param);
            }
        }
        update_params.push(Self::value_to_param(pk));
        debug!("Dameng UPDATE: {}", update_sql);
        let affected = self
            .execute_with_params(update_sql.as_str(), &update_params)
            .await?;
        if affected > 0 {
            self.write_blob_columns(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &table_info,
                record,
            )
            .await?;
        }
        Ok(affected)
    }

    async fn insert_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
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
            .map(|c| Self::dml_value_sql(&table_info, c, record.after.get(c)))
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
            .filter_map(|col| Self::dml_value_param(&table_info, col, record.after.get(col)))
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
            self.write_blob_columns(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &table_info,
                record,
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
        self.write_blob_columns(
            schema,
            table_name,
            table_key,
            pk_name,
            columns,
            &table_info,
            record,
        )
        .await?;
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
        _table_key: &str,
        pk_name: &str,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Ok(());
        }
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name)
        );
        let params = vec![Self::value_to_param(pk)];
        debug!("Dameng DELETE: {}", sql);
        self.execute_with_params(sql.as_str(), &params).await?;
        Ok(())
    }

    async fn write_blob_columns(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        table_info: &TableInfoVo,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none()
            && columns.iter().any(|col| {
                Self::is_source_blob_column(table_info, col)
                    && matches!(record.after.get(col), Value::Blob(_))
            })
        {
            return Err(format!(
                "pk is empty when writing Dameng BLOB columns: {}.{}",
                table_key, pk_name
            ));
        }

        for col in columns {
            if !Self::is_source_blob_column(table_info, col) {
                continue;
            }
            let Value::Blob(value) = record.after.get(col) else {
                continue;
            };
            self.write_blob_column(schema, table_name, pk_name, pk, col, value)
                .await?;
        }
        Ok(())
    }

    async fn write_blob_column(
        &self,
        schema: &str,
        table_name: &str,
        pk_name: &str,
        pk: &Value,
        column_name: &str,
        value: &[u8],
    ) -> Result<(), String> {
        let reset_sql = Self::reset_blob_sql(schema, table_name, pk_name, column_name);
        let reset_params = vec![Self::value_to_param(pk)];
        self.execute_with_params(reset_sql.as_str(), &reset_params)
            .await?;

        for chunk in value.chunks(DAMENG_BLOB_WRITE_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let append_sql =
                Self::append_blob_chunk_sql(schema, table_name, pk_name, column_name, chunk.len());
            let append_params = vec![
                Self::value_to_param(pk),
                DamengParam::Text(Self::bytes_to_hex(chunk)),
            ];
            self.execute_with_params(append_sql.as_str(), &append_params)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::{ForeignKeyInfo, MySqlRoutineKind, TableInfoVo, Value};
    use dameng::ToDmValue;
    use dameng_types::DmValue;
    use std::collections::HashMap;

    use super::{DamengParam, DamengSink};

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
            foreign_keys: vec![],
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
    fn routine_exists_sql_quotes_schema_kind_and_name() {
        assert_eq!(
            DamengSink::dameng_routine_exists_sql(
                "target_schema",
                MySqlRoutineKind::Function,
                "calc`demo"
            ),
            "SELECT COUNT(*) FROM ALL_OBJECTS WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND OBJECT_TYPE = 'FUNCTION' AND (OBJECT_NAME = 'calc`demo' OR OBJECT_NAME = 'CALC`DEMO')"
        );
        assert_eq!(
            DamengSink::dameng_routine_exists_sql("", MySqlRoutineKind::Procedure, "sync_demo"),
            "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PROCEDURE' AND (OBJECT_NAME = 'sync_demo' OR OBJECT_NAME = 'SYNC_DEMO')"
        );
    }

    #[test]
    fn conflict_routine_name_adds_kind_suffix_and_limits_length() {
        assert_eq!(
            DamengSink::conflict_routine_name(
                "ns_report_classification",
                MySqlRoutineKind::Procedure,
                0
            ),
            "ns_report_classification_procedure"
        );
        assert_eq!(
            DamengSink::conflict_routine_name("calc_total", MySqlRoutineKind::Function, 1),
            "calc_total_function_2"
        );

        let long_name = "a".repeat(140);
        let renamed =
            DamengSink::conflict_routine_name(long_name.as_str(), MySqlRoutineKind::Procedure, 0);
        assert_eq!(renamed.chars().count(), 128);
        assert!(renamed.ends_with("_procedure"));
    }

    #[test]
    fn object_name_exists_sql_checks_any_object_type() {
        assert_eq!(
            DamengSink::dameng_object_name_exists_sql("target_schema", "ns_report_classification"),
            "SELECT COUNT(*) FROM ALL_OBJECTS WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND (OBJECT_NAME = 'ns_report_classification' OR OBJECT_NAME = 'NS_REPORT_CLASSIFICATION')"
        );
        assert_eq!(
            DamengSink::dameng_object_name_exists_sql("", "sync_demo"),
            "SELECT COUNT(*) FROM USER_OBJECTS WHERE (OBJECT_NAME = 'sync_demo' OR OBJECT_NAME = 'SYNC_DEMO')"
        );
    }

    #[test]
    fn sql_preview_truncates_on_char_boundary() {
        let sql = format!("SELECT '{}'", "\u{5404}".repeat(600));
        let preview = DamengSink::sql_preview(sql.as_str());

        assert!(preview.ends_with("..."));
        assert!(preview.is_char_boundary(preview.len()));
    }

    #[test]
    fn foreign_key_sql_maps_restrict_to_no_action() {
        let foreign_key = ForeignKeyInfo {
            constraint_name: "fk_child_parent".to_string(),
            table_name: "child".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_table_name: "parent".to_string(),
            referenced_columns: vec!["rOWID".to_string()],
            update_rule: Some("RESTRICT".to_string()),
            delete_rule: Some("CASCADE".to_string()),
        };

        assert_eq!(
            DamengSink::foreign_key_sql("target_schema", &foreign_key).unwrap(),
            "ALTER TABLE \"target_schema\".\"child\" ADD CONSTRAINT \"fk_child_parent\" FOREIGN KEY (\"parent_id\") REFERENCES \"target_schema\".\"parent\" (\"rOWID_\") ON DELETE CASCADE ON UPDATE NO ACTION WITH INDEX"
        );
    }

    #[test]
    fn foreign_key_sql_rejects_mismatched_columns() {
        let foreign_key = ForeignKeyInfo {
            constraint_name: "fk_bad".to_string(),
            table_name: "child".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_table_name: "parent".to_string(),
            referenced_columns: vec![],
            update_rule: None,
            delete_rule: None,
        };

        assert!(DamengSink::foreign_key_sql("target_schema", &foreign_key).is_none());
    }

    #[test]
    fn create_table_sql_omits_primary_key_for_schema_only_table() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_system_organization_dimen_tree".to_string(),
            pk_column: "".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_system_organization_dimen_tree` (
  `organization_id` bigint NOT NULL,
  `organization_parent_id` bigint NOT NULL,
  `organization_path` varchar(400) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "organization_id".to_string(),
                "organization_parent_id".to_string(),
                "organization_path".to_string(),
            ],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(
            sql.starts_with("CREATE TABLE \"target_schema\".\"ns_system_organization_dimen_tree\"")
        );
        assert!(sql.contains("\"organization_id\" BIGINT NOT NULL"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn create_table_sql_preserves_string_primary_key_for_schema_only_table() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "act_ge_bytearray".to_string(),
            pk_column: "".to_string(),
            create_table_sql: r#"CREATE TABLE `act_ge_bytearray` (
  `ID_` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `REV_` int DEFAULT NULL,
  `NAME_` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `DEPLOYMENT_ID_` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `BYTES_` longblob,
  `GENERATED_` tinyint DEFAULT NULL,
  PRIMARY KEY (`ID_`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 ROW_FORMAT=DYNAMIC"#
                .to_string(),
            columns: vec![
                "ID_".to_string(),
                "REV_".to_string(),
                "NAME_".to_string(),
                "DEPLOYMENT_ID_".to_string(),
                "BYTES_".to_string(),
                "GENERATED_".to_string(),
            ],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"ID_\" VARCHAR(64 CHAR) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"ID_\")"));
    }

    #[test]
    fn create_table_sql_preserves_composite_primary_key_for_schema_only_table() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "tenant_code".to_string(),
            pk_column: "".to_string(),
            create_table_sql: r#"CREATE TABLE `tenant_code` (
  `tenant_id` varchar(64) NOT NULL,
  `code` varchar(64) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`tenant_id`,`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "tenant_id".to_string(),
                "code".to_string(),
                "name".to_string(),
            ],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"tenant_id\" VARCHAR(64 CHAR) NOT NULL"));
        assert!(sql.contains("\"code\" VARCHAR(64 CHAR) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"tenant_id\", \"code\")"));
    }

    #[test]
    fn create_table_sql_preserves_current_timestamp_default_for_time_columns() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `orders` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `sys_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `name` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "sys_time".to_string(),
                "update_time".to_string(),
                "name".to_string(),
            ],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"sys_time\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(sql.contains("\"update_time\" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(sql.contains("\"name\" VARCHAR(32 CHAR) NULL"));
    }

    #[test]
    fn create_table_sql_preserves_supported_literal_defaults() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "biz_building_info".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `biz_building_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `migrate_flag` tinyint NOT NULL DEFAULT '0' COMMENT '是否迁移业务表标志 0未迁移 1已迁移',
  `building_name` varchar(50) DEFAULT '未命名',
  `remark` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "migrate_flag".to_string(),
                "building_name".to_string(),
                "remark".to_string(),
            ],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"migrate_flag\" TINYINT NOT NULL DEFAULT 0"));
        assert!(sql.contains("\"building_name\" VARCHAR(50 CHAR) NULL DEFAULT '未命名'"));
        assert!(sql.contains("\"remark\" VARCHAR(50 CHAR) NULL"));
        assert!(!sql.contains("DEFAULT NULL"));
    }

    #[test]
    fn column_comments_are_converted_to_dameng_comment_sql() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "newsee-owner".to_string(),
            table_name: "biz_building_info".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `biz_building_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `migrate_flag` tinyint NOT NULL DEFAULT '0' COMMENT '是否迁移业务表标志 0未迁移 1已迁移',
  `name` varchar(50) DEFAULT NULL COMMENT '业主''名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "migrate_flag".to_string(),
                "name".to_string(),
            ],
            foreign_keys: vec![],
        };

        let comments = DamengSink::mysql_column_comments(&table_info);

        assert_eq!(
            comments.get("migrate_flag").map(String::as_str),
            Some("是否迁移业务表标志 0未迁移 1已迁移")
        );
        assert_eq!(comments.get("name").map(String::as_str), Some("业主'名称"));
        assert_eq!(
            DamengSink::comment_on_column_sql(
                "newsee-owner",
                "biz_building_info",
                "migrate_flag",
                comments.get("migrate_flag").unwrap()
            ),
            "COMMENT ON COLUMN \"newsee-owner\".\"biz_building_info\".\"migrate_flag\" IS '是否迁移业务表标志 0未迁移 1已迁移'"
        );
    }

    #[test]
    fn column_comment_update_skips_missing_target_columns() {
        let mut existing_comments = HashMap::new();
        existing_comments.insert("name".to_string(), "旧名称".to_string());
        existing_comments.insert("remark".to_string(), "备注".to_string());

        assert_eq!(
            DamengSink::column_comment_needs_update(
                &existing_comments,
                "third_part_id",
                "第三方ID"
            ),
            None
        );
        assert_eq!(
            DamengSink::column_comment_needs_update(&existing_comments, "remark", "备注"),
            Some(false)
        );
        assert_eq!(
            DamengSink::column_comment_needs_update(&existing_comments, "name", "名称"),
            Some(true)
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
            foreign_keys: vec![],
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
    fn columns_metadata_sql_reads_one_row_per_column() {
        let sql = DamengSink::columns_metadata_sql("target_schema", "orders");

        assert!(sql.contains("FROM ALL_TAB_COLUMNS"));
        assert!(sql.contains("ORDER BY COLUMN_ID"));
        assert!(sql.contains("CHR(31)"));
        assert!(!sql.contains("LISTAGG"));
        assert!(sql.contains("(OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA')"));
        assert!(sql.contains("(TABLE_NAME = 'orders' OR TABLE_NAME = 'ORDERS')"));
    }

    #[test]
    fn parses_dameng_columns_metadata() {
        let cols = DamengSink::parse_columns_metadata(
            "id:BIGINT:8:0:|param:VARCHAR:16000:4000:|sys_time:TIMESTAMP:8:0:CURRENT_TIMESTAMP",
        );

        let param = cols.get("param").unwrap();
        assert_eq!(param.data_type, "VARCHAR");
        assert_eq!(param.data_length, 16000);
        assert_eq!(param.char_length, 4000);
        assert_eq!(param.default_value, None);

        let sys_time = cols.get("sys_time").unwrap();
        assert_eq!(sys_time.data_type, "TIMESTAMP");
        assert_eq!(sys_time.default_value.as_deref(), Some("CURRENT_TIMESTAMP"));

        let row_cols = DamengSink::parse_columns_metadata(
            "incomeType\x1fINT\x1f4\x1f0\x1f0\x1eintegralDeductItems\x1fVARCHAR\x1f1024\x1f256\x1f",
        );
        assert_eq!(row_cols.get("incometype").unwrap().data_type, "INT");
        assert_eq!(
            row_cols.get("integraldeductitems").unwrap().char_length,
            256
        );
    }

    #[test]
    fn parses_dameng_column_comments_metadata() {
        let comments = DamengSink::parse_column_comments_metadata(
            "migrate_flag\x1f是否迁移业务表标志 0未迁移 1已迁移\x1ename\x1f业主名称",
        );

        assert_eq!(
            comments.get("migrate_flag").map(String::as_str),
            Some("是否迁移业务表标志 0未迁移 1已迁移")
        );
        assert_eq!(comments.get("name").map(String::as_str), Some("业主名称"));
    }

    #[test]
    fn column_comment_sql_prefers_dba_comments_for_schema() {
        let sql = DamengSink::dba_column_comments_sql("newsee-center-pay", "ns_application");

        assert!(sql.contains("FROM DBA_COL_COMMENTS c"));
        assert!(sql.contains("c.OWNER = 'newsee-center-pay'"));
        assert!(sql.contains("c.TABLE_NAME = 'ns_application'"));
        assert!(sql.contains("c.COMMENTS"));
    }

    #[test]
    fn all_column_comment_sql_keeps_user_and_all_fallbacks() {
        let user_sql = DamengSink::all_column_comments_sql("", "ns_application");
        let all_sql = DamengSink::all_column_comments_sql("newsee-center-pay", "ns_application");

        assert!(user_sql.contains("FROM USER_TAB_COLUMNS c"));
        assert!(user_sql.contains("LEFT JOIN USER_COL_COMMENTS cc"));
        assert!(all_sql.contains("FROM ALL_TAB_COLUMNS c"));
        assert!(all_sql.contains("LEFT JOIN ALL_COL_COMMENTS cc"));
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
    fn inline_sql_params_replaces_placeholders_outside_quotes() {
        let sql = "INSERT INTO \"S\".\"表\" (\"ID\", \"name\", \"raw\") VALUES (?, ?, HEXTORAW(?))";
        let params = vec![
            DamengParam::I64(7),
            DamengParam::Text("达'鑫".to_string()),
            DamengParam::Text("1F8B".to_string()),
        ];

        let inline_sql = DamengSink::inline_sql_params(sql, &params).unwrap();

        assert_eq!(
            inline_sql,
            "INSERT INTO \"S\".\"表\" (\"ID\", \"name\", \"raw\") VALUES (7, '达''鑫', HEXTORAW('1F8B'))"
        );
    }

    #[test]
    fn inline_sql_params_keeps_question_marks_inside_literals_and_identifiers() {
        let sql = "UPDATE \"S\".\"t?\" SET \"name\" = ? WHERE \"note\" = '?'";
        let params = vec![DamengParam::Text("x".to_string())];

        let inline_sql = DamengSink::inline_sql_params(sql, &params).unwrap();

        assert_eq!(
            inline_sql,
            "UPDATE \"S\".\"t?\" SET \"name\" = 'x' WHERE \"note\" = '?'"
        );
    }

    #[test]
    fn inline_sql_params_renders_bytes_as_hextoraw() {
        let inline_sql = DamengSink::inline_sql_params(
            "INSERT INTO \"S\".\"T\" (\"b\") VALUES (?)",
            &[DamengParam::Bytes(vec![0x1f, 0x8b])],
        )
        .unwrap();

        assert_eq!(
            inline_sql,
            "INSERT INTO \"S\".\"T\" (\"b\") VALUES (HEXTORAW('1F8B'))"
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
            DamengSink::map_mysql_type_to_dameng("varchar(0)"),
            "VARCHAR(1 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("char(20)"),
            "CHAR(20 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("char(0)"),
            "CHAR(1 CHAR)"
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
            default_value: None,
        };
        let char_semantics = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 200,
            char_length: 50,
            default_value: None,
        };
        assert!(DamengSink::should_modify_existing_column(
            "varchar(50)",
            "`name` varchar(50) DEFAULT NULL",
            &byte_semantics
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "varchar(50)",
            "`name` varchar(50) DEFAULT NULL",
            &char_semantics
        ));
    }

    #[test]
    fn existing_clob_is_not_narrowed_to_varchar() {
        let clob = super::DamengColumnInfo {
            data_type: "CLOB".to_string(),
            data_length: 2147483647,
            char_length: 0,
            default_value: None,
        };

        assert!(!DamengSink::should_modify_existing_column(
            "varchar(50)",
            "`errorMessage` varchar(50) DEFAULT NULL",
            &clob
        ));
    }

    #[test]
    fn existing_varchar_is_modified_to_clob_for_mysql_text() {
        let varchar = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 800,
            char_length: 200,
            default_value: None,
        };

        assert!(DamengSink::should_modify_existing_column(
            "text",
            "`billType` text COMMENT '票据可开类型'",
            &varchar
        ));
        assert_eq!(DamengSink::dameng_modify_column_definition("text"), "CLOB");
    }

    #[test]
    fn existing_column_modify_definition_uses_type_only() {
        assert_eq!(
            DamengSink::dameng_modify_column_definition("bigint"),
            "BIGINT"
        );
        assert_eq!(
            DamengSink::dameng_modify_column_definition("varchar(50)"),
            "VARCHAR(50 CHAR)"
        );
    }

    #[test]
    fn current_timestamp_default_detection_handles_supported_time_columns() {
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "timestamp",
                "`sys_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            ),
            Some("DEFAULT CURRENT_TIMESTAMP")
        );
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "datetime",
                "`update_time` datetime DEFAULT CURRENT_TIMESTAMP(3)",
            ),
            Some("DEFAULT CURRENT_TIMESTAMP")
        );
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "varchar(64)",
                "`name` varchar(64) DEFAULT CURRENT_TIMESTAMP",
            ),
            None
        );
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "timestamp",
                "`sys_time` timestamp NOT NULL COMMENT 'DEFAULT CURRENT_TIMESTAMP'",
            ),
            None
        );
    }

    #[test]
    fn current_timestamp_default_mismatch_is_skipped_for_existing_columns() {
        let missing_default = super::DamengColumnInfo {
            data_type: "TIMESTAMP".to_string(),
            data_length: 8,
            char_length: 0,
            default_value: None,
        };
        let matching_default = super::DamengColumnInfo {
            data_type: "TIMESTAMP".to_string(),
            data_length: 8,
            char_length: 0,
            default_value: Some("CURRENT_TIMESTAMP".to_string()),
        };
        let def =
            "`sys_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";

        assert!(DamengSink::existing_column_default_mismatch(
            "timestamp",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "timestamp",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "timestamp",
            def,
            &matching_default
        ));
    }

    #[test]
    fn literal_default_mismatch_is_skipped_for_existing_columns() {
        let missing_default = super::DamengColumnInfo {
            data_type: "TINYINT".to_string(),
            data_length: 1,
            char_length: 0,
            default_value: None,
        };
        let matching_default = super::DamengColumnInfo {
            data_type: "TINYINT".to_string(),
            data_length: 1,
            char_length: 0,
            default_value: Some("0".to_string()),
        };
        let def = "`migrate_flag` tinyint NOT NULL DEFAULT '0' COMMENT '是否迁移业务表标志 0未迁移 1已迁移'";

        assert!(DamengSink::existing_column_default_mismatch(
            "tinyint",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "tinyint",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "tinyint",
            def,
            &matching_default
        ));
    }

    #[test]
    fn primary_key_and_auto_increment_columns_are_protected_from_auto_modify() {
        let primary_key_columns = vec!["id".to_string()];

        assert!(DamengSink::is_protected_source_column_for_auto_modify(
            &primary_key_columns,
            "ID",
            "`id` bigint NOT NULL DEFAULT 0"
        ));
        assert!(DamengSink::is_protected_source_column_for_auto_modify(
            &[],
            "id",
            "`id` bigint NOT NULL AUTO_INCREMENT"
        ));
        assert!(!DamengSink::is_protected_source_column_for_auto_modify(
            &primary_key_columns,
            "migrate_flag",
            "`migrate_flag` tinyint NOT NULL DEFAULT '0'"
        ));
    }

    #[test]
    fn lob_columns_use_dameng_dml_expressions() {
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
            foreign_keys: vec![],
        };

        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "param", &Value::String("x".to_string())),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "opration", &Value::String("x".to_string())),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "modul", &Value::String("x".to_string())),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "operatorLogo", &Value::Blob(vec![1, 2, 3])),
            "EMPTY_BLOB()"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "operatorLogo", &Value::None),
            "?"
        );
    }

    #[test]
    fn blob_columns_are_written_as_hex_chunks() {
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
            foreign_keys: vec![],
        };

        let value = Value::Blob(vec![0x1f, 0x8b, 0x08]);
        assert!(DamengSink::dml_value_param(&table_info, "operatorLogo", &value).is_none());
        assert_eq!(
            DamengSink::dml_value_param(&table_info, "operatorLogo", &Value::None)
                .unwrap()
                .to_dm_value(),
            DmValue::Null
        );
        assert_eq!(DamengSink::bytes_to_hex(&[0x1f, 0x8b, 0x08]), "1F8B08");
        assert_eq!(
            DamengSink::reset_blob_sql("target_schema", "ns_soss_operator", "id", "operatorLogo"),
            "UPDATE \"target_schema\".\"ns_soss_operator\" SET \"operatorLogo\" = EMPTY_BLOB() WHERE \"id\" = ?"
        );
        assert_eq!(
            DamengSink::append_blob_chunk_sql(
                "target_schema",
                "ns_soss_operator",
                "id",
                "operatorLogo",
                3
            ),
            "DECLARE\n  v_lob BLOB;\nBEGIN\n  SELECT \"operatorLogo\" INTO v_lob FROM \"target_schema\".\"ns_soss_operator\" WHERE \"id\" = ? FOR UPDATE;\n  DBMS_LOB.WRITEAPPEND(v_lob, 3, HEXTORAW(?));\nEND;"
        );
    }

    #[test]
    fn text_columns_with_blob_runtime_values_bind_as_text() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "o2o_bbs_activity".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `o2o_bbs_activity` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `content` longtext DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec!["id".to_string(), "content".to_string()],
            foreign_keys: vec![],
        };

        let value = Value::Blob("正文内容".as_bytes().to_vec());

        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "content", &value),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_param(&table_info, "content", &value)
                .unwrap()
                .to_dm_value(),
            DmValue::Text("正文内容".to_string())
        );
    }
}
