use async_trait::async_trait;
use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecString,
};
use common::checkpoint_manager::CheckpointServiceHandle;
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::runtime_progress;
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_type_token_from_column_definition, normalize_mysql_column_type_token,
};
use common::{
    CdcConfig, DataBuffer, FlushByOperation, ForeignKeyInfo, MySqlRoutineDefinition,
    MySqlRoutineKind, MySqlViewDefinition, Operation, Sink, TableInfoVo, Value, database_table_key,
    fetch_mysql_routines, fetch_mysql_views, get_mysql_pool_by_url,
    mysql_connection_url_from_config, mysql_row_text_value, mysql_utf8mb4_string_expr,
    mysql_view_exists, rewrite_mysql_create_view_for_target,
};
#[cfg(test)]
use common::{show_create_mysql_routine_sql, strip_create_routine_definer};
use sqlx::mysql::{MySqlArguments, MySqlQueryResult};
use sqlx::query::Query;
use sqlx::{Executor, MySql, Pool, Row};
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::log::trace;
use tracing::{debug, error, info};

pub struct MySqlSink {
    primary_database: String,
    target_pools: HashMap<String, Pool<MySql>>,
    connection_urls: HashMap<String, String>,
    table_info_list: Vec<TableInfoVo>,
    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,
    source_initializing: RwLock<bool>,
    sink_batch_size: usize,
    sync_foreign_key_tables: bool,

    // 缓存所有字段名（第一批数据会取一次）
    table_info_cache: Mutex<CaseInsensitiveHashMapTableInfoVo>,
    columns_cache: Mutex<CaseInsensitiveHashMapVecString>,
    // pk_cache: Mutex<CaseInsensitiveHashMapVecString>,
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
    checkpoint_service: CheckpointServiceHandle,
}

impl MySqlSink {
    pub async fn new(
        config: &CdcConfig,
        table_info_list: Vec<TableInfoVo>,
        checkpoint_service: CheckpointServiceHandle,
    ) -> Self {
        if let Err(e) = Self::validate_merged_target_schema(&table_info_list) {
            panic!("{}", e);
        }

        let target_databases = config.sink_databases();
        let primary_database = target_databases.first().cloned().unwrap_or_default();
        let mut target_pools = HashMap::new();
        let mut connection_urls = HashMap::new();
        let target_database_stage = "sink.mysql.target_databases";
        runtime_progress::begin_schema_stage(
            target_database_stage,
            "MySQL sink 目标库准备",
            target_databases.len() as u64,
        )
        .await;
        for database in &target_databases {
            let connection_url =
                mysql_connection_url_from_config(&config.sink_config[0], Some(database));
            let pool = match Self::get_pool_auto_create_database(
                config,
                database.clone(),
                &connection_url,
            )
            .await
            {
                Ok(value) => value,
                Err(value) => {
                    runtime_progress::record_schema_stage_error(
                        target_database_stage,
                        database.as_str(),
                        "target database pool initialization returned early",
                    )
                    .await;
                    runtime_progress::finish_schema_stage(target_database_stage).await;
                    return value;
                }
            };
            runtime_progress::record_schema_stage_item(target_database_stage, database.as_str())
                .await;
            target_pools.insert(database.to_ascii_lowercase(), pool);
            connection_urls.insert(database.to_ascii_lowercase(), connection_url);
        }
        runtime_progress::finish_schema_stage(target_database_stage).await;

        if config.sync_stored_procedure_enabled() {
            let stage = "sink.mysql.stored_routines";
            runtime_progress::begin_schema_stage(stage, "MySQL sink 同步存储过程/函数", 1).await;
            match Self::sync_stored_routines(config, &target_pools, primary_database.as_str()).await
            {
                Ok(_) => {
                    runtime_progress::record_schema_stage_item(stage, "stored_routines").await;
                    runtime_progress::finish_schema_stage(stage).await;
                }
                Err(e) => {
                    runtime_progress::record_schema_stage_error(
                        stage,
                        "stored_routines",
                        e.as_str(),
                    )
                    .await;
                    runtime_progress::finish_schema_stage(stage).await;
                    panic!("MySQL sync stored routines failed: {}", e);
                }
            }
        }

        let mut target_tables: HashMap<String, TableInfoVo> = HashMap::new();
        for table_info in &table_info_list {
            let database = Self::table_info_target_database(table_info, primary_database.as_str());
            target_tables
                .entry(database_table_key(
                    database.as_str(),
                    table_info.table_name.as_str(),
                ))
                .or_insert_with(|| table_info.clone());
        }

        if config.auto_create_table.unwrap_or(true) {
            let stage = "sink.mysql.create_table";
            runtime_progress::begin_schema_stage(
                stage,
                "MySQL sink 自动建表",
                target_tables.len() as u64,
            )
            .await;
            let sql = "select * from information_schema.`COLUMNS` where TABLE_SCHEMA = ? AND TABLE_NAME = ?";
            for table_info in target_tables.values() {
                let database =
                    Self::table_info_target_database(table_info, primary_database.as_str());
                let pool = target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
                let table_label = database_table_key(database.as_str(), table_name.as_str());
                let is_empty = match sqlx::query(sql)
                    .persistent(false)
                    .bind(&database)
                    .bind(&table_name)
                    .fetch_all(pool)
                    .await
                {
                    Ok(rows) => rows.is_empty(),
                    Err(e) => {
                        runtime_progress::record_schema_stage_error(
                            stage,
                            table_label.as_str(),
                            e.to_string().as_str(),
                        )
                        .await;
                        runtime_progress::finish_schema_stage(stage).await;
                        panic!("query failed: {}", e);
                    }
                };
                if is_empty {
                    let create_table_sql = if config.sync_foreign_key_tables_enabled() {
                        Self::create_table_sql_without_foreign_keys(
                            table_info.create_table_sql.as_str(),
                        )
                    } else {
                        table_info.create_table_sql.clone()
                    };
                    if let Err(e) = pool.execute(create_table_sql.as_str()).await {
                        runtime_progress::record_schema_stage_error(
                            stage,
                            table_label.as_str(),
                            e.to_string().as_str(),
                        )
                        .await;
                        runtime_progress::finish_schema_stage(stage).await;
                        panic!("Failed to create table: {}", e);
                    }
                }
                runtime_progress::record_schema_stage_item(stage, table_label.as_str()).await;
            }
            runtime_progress::finish_schema_stage(stage).await;
        }
        if config.auto_add_column.unwrap_or(true) {
            let stage = "sink.mysql.add_column";
            runtime_progress::begin_schema_stage(
                stage,
                "MySQL sink 自动加字段",
                target_tables.len() as u64,
            )
            .await;
            for table_info in target_tables.values() {
                let database =
                    Self::table_info_target_database(table_info, primary_database.as_str());
                let pool = target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
                let table_label = database_table_key(database.as_str(), table_name.as_str());
                let rows = sqlx::query(
                    "select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE from information_schema.`COLUMNS` where TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                )
                .persistent(false)
                .bind(&database)
                .bind(&table_name)
                .fetch_all(pool)
                .await
                .unwrap_or_default();
                let exists_set: HashSet<String> = rows
                    .iter()
                    .map(|row| mysql_row_text_value(row, "COLUMN_NAME").to_ascii_lowercase())
                    .collect();

                let defs = extract_mysql_create_table_column_definitions(
                    table_info.create_table_sql.as_str(),
                );

                for src_col in &table_info.columns {
                    let key = src_col.to_ascii_lowercase();
                    if exists_set.contains(&key) {
                        continue;
                    }
                    let def = match defs.get(&key) {
                        None => continue,
                        Some(v) => v,
                    };
                    let alter_sql = format!("ALTER TABLE `{}` ADD COLUMN {}", table_name, def);
                    match pool.execute(alter_sql.as_str()).await {
                        Ok(_) => info!(
                            "auto add column success: {}.{} {}",
                            database, table_name, src_col
                        ),
                        Err(e) => {
                            error!(
                                "auto add column failed: {}.{} {} {}",
                                database, table_name, src_col, e
                            );
                            runtime_progress::record_schema_stage_error(
                                stage,
                                format!("{}.{}", table_label, src_col).as_str(),
                                e.to_string().as_str(),
                            )
                            .await;
                        }
                    }
                }
                runtime_progress::record_schema_stage_item(stage, table_label.as_str()).await;
            }
            runtime_progress::finish_schema_stage(stage).await;
        }
        if config.auto_modify_column.unwrap_or(true) {
            let stage = "sink.mysql.modify_column";
            runtime_progress::begin_schema_stage(
                stage,
                "MySQL sink 自动改字段",
                target_tables.len() as u64,
            )
            .await;
            for table_info in target_tables.values() {
                let database =
                    Self::table_info_target_database(table_info, primary_database.as_str());
                let pool = target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
                let table_label = database_table_key(database.as_str(), table_name.as_str());
                let defs = extract_mysql_create_table_column_definitions(
                    table_info.create_table_sql.as_str(),
                );

                let rows = sqlx::query(
                    "select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE from information_schema.`COLUMNS` where TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                )
                .persistent(false)
                .bind(&database)
                .bind(&table_name)
                .fetch_all(pool)
                .await
                .unwrap_or_default();

                let mut sink_meta: HashMap<String, (String, bool)> = HashMap::new();
                for row in rows.iter() {
                    let name = mysql_row_text_value(row, "COLUMN_NAME");
                    let typ = mysql_row_text_value(row, "COLUMN_TYPE");
                    let is_nullable = mysql_row_text_value(row, "IS_NULLABLE");
                    sink_meta.insert(
                        name.to_ascii_lowercase(),
                        (
                            typ.to_ascii_lowercase(),
                            is_nullable.eq_ignore_ascii_case("YES"),
                        ),
                    );
                }

                for src_col in &table_info.columns {
                    let key = src_col.to_ascii_lowercase();
                    let (sink_type, sink_nullable) = match sink_meta.get(&key) {
                        None => continue,
                        Some(v) => v.clone(),
                    };
                    let def = match defs.get(&key) {
                        None => continue,
                        Some(v) => v,
                    };
                    let src_type = match mysql_type_token_from_column_definition(def.as_str()) {
                        None => continue,
                        Some(v) => v.to_ascii_lowercase(),
                    };
                    let src_nullable = mysql_column_allows_null_from_definition(def.as_str());

                    let sink_type_normalized =
                        normalize_mysql_column_type_token(sink_type.as_str());
                    let src_type_normalized = normalize_mysql_column_type_token(src_type.as_str());
                    let need_modify_type = sink_type_normalized != src_type_normalized;
                    let need_modify_nullable = src_nullable && !sink_nullable;
                    if !need_modify_type && !need_modify_nullable {
                        continue;
                    }
                    let mut reasons = Vec::new();
                    if need_modify_type {
                        reasons.push(format!(
                            "type {} -> {}",
                            sink_type_normalized, src_type_normalized
                        ));
                    }
                    if need_modify_nullable {
                        reasons.push(format!("nullable {} -> {}", sink_nullable, src_nullable));
                    }
                    let reason = reasons.join(", ");
                    let modify_sql = format!("ALTER TABLE `{}` MODIFY COLUMN {}", table_name, def);
                    match pool.execute(modify_sql.as_str()).await {
                        Ok(_) => info!(
                            "auto modify column success: {}.{} {} reason: {}",
                            database, table_name, src_col, reason
                        ),
                        Err(e) => {
                            runtime_progress::record_schema_stage_error(
                                stage,
                                format!("{}.{}", table_label, src_col).as_str(),
                                e.to_string().as_str(),
                            )
                            .await;
                            error!(
                                "auto modify column failed: {}.{} {} {}",
                                database, table_name, src_col, e
                            );
                        }
                    }
                }
                runtime_progress::record_schema_stage_item(stage, table_label.as_str()).await;
            }
            runtime_progress::finish_schema_stage(stage).await;
        }
        if config.sync_stored_view_enabled() {
            let stage = "sink.mysql.stored_views";
            runtime_progress::begin_schema_stage(stage, "MySQL sink 同步视图", 1).await;
            match Self::sync_stored_views(config, &target_pools, primary_database.as_str()).await {
                Ok(_) => {
                    runtime_progress::record_schema_stage_item(stage, "stored_views").await;
                    runtime_progress::finish_schema_stage(stage).await;
                }
                Err(e) => {
                    runtime_progress::record_schema_stage_error(stage, "stored_views", e.as_str())
                        .await;
                    runtime_progress::finish_schema_stage(stage).await;
                    panic!("MySQL sync stored views failed: {}", e);
                }
            }
        }
        let sink_batch_size = config.sink_batch_size.unwrap_or(256);
        MySqlSink {
            primary_database,
            target_pools,
            connection_urls,
            table_info_list,
            buffer: Mutex::new(Vec::with_capacity(sink_batch_size)),
            initialized: RwLock::new(false),
            source_initializing: RwLock::new(true),
            sink_batch_size,
            sync_foreign_key_tables: config.sync_foreign_key_tables_enabled(),
            table_info_cache: Mutex::new(CaseInsensitiveHashMapTableInfoVo::new_with_no_arg()),
            columns_cache: Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg()),
            // pk_cache: Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg()),
            checkpoint: Mutex::new(HashMap::new()),
            checkpoint_service,
        }
    }

    async fn get_pool_auto_create_database(
        config: &CdcConfig,
        database: String,
        connection_url: &str,
    ) -> Result<Pool<MySql>, MySqlSink> {
        let pool: Pool<MySql> =
            match get_mysql_pool_by_url(connection_url, "mysql sink 自动创建数据库-探测").await
            {
                Ok(o) => o,
                Err(e) => {
                    if config.auto_create_database.unwrap_or(true) {
                        let pool_for_auto_create_database = get_mysql_pool_by_url(
                            &mysql_connection_url_from_config(&config.sink_config[0], None),
                            "mysql sink 自动创建数据库-创建",
                        )
                        .await
                        .unwrap_or_else(|e| panic!("MySQL sink 自动创建数据库连接失败: {}", e));
                        let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", database.clone());
                        match (&pool_for_auto_create_database).execute(sql.as_str()).await {
                            Ok(xx) => xx,
                            Err(e) => {
                                error!("Failed to create database: {}", e);
                                panic!("Failed to create database: {}", e);
                            }
                        };
                        let pool: Pool<MySql> =
                            get_mysql_pool_by_url(connection_url, "mysql sink 自动创建数据库-获取")
                                .await
                                .unwrap_or_else(|e| panic!("MySQL sink 获取数据库连接失败: {}", e));
                        return Ok(pool);
                    }
                    error!("Failed to connect to MySQL: {}", e);
                    panic!("Failed to connect to MySQL: {}", e);
                }
            };
        Ok(pool)
    }

    fn table_info_target_database(table_info: &TableInfoVo, fallback: &str) -> String {
        if table_info.target_database.trim().is_empty() {
            fallback.to_string()
        } else {
            table_info.target_database.clone()
        }
    }

    fn record_target_database(&self, record: &DataBuffer) -> String {
        if record.target_database.trim().is_empty() {
            self.primary_database.clone()
        } else {
            record.target_database.clone()
        }
    }

    fn target_table_key(database: &str, table_name: &str) -> String {
        database_table_key(database, table_name)
    }

    fn quote_mysql_identifier(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    fn qualified_table_name(database: &str, table_name: &str) -> String {
        format!(
            "{}.{}",
            Self::quote_mysql_identifier(database),
            Self::quote_mysql_identifier(table_name)
        )
    }

    fn create_table_sql_without_foreign_keys(create_table_sql: &str) -> String {
        let mut lines = Vec::new();
        for line in create_table_sql.lines() {
            let trimmed = line.trim_start();
            let lower = trimmed.to_ascii_lowercase();
            if lower.starts_with("constraint ") && lower.contains(" foreign key") {
                continue;
            }
            if lower.starts_with("foreign key") {
                continue;
            }
            lines.push(line.to_string());
        }

        let close_index = lines
            .iter()
            .position(|line| line.trim_start().starts_with(')'));
        if let Some(close_index) = close_index {
            for index in (0..close_index).rev() {
                let trimmed_len = lines[index].trim_end().len();
                if trimmed_len == 0 {
                    continue;
                }
                if lines[index].as_bytes().get(trimmed_len - 1) == Some(&b',') {
                    lines[index].replace_range(trimmed_len - 1..trimmed_len, "");
                }
                break;
            }
        }
        lines.join("\n")
    }

    fn foreign_key_sql(database: &str, foreign_key: &ForeignKeyInfo) -> Option<String> {
        if foreign_key.columns.is_empty()
            || foreign_key.columns.len() != foreign_key.referenced_columns.len()
        {
            return None;
        }
        let columns = foreign_key
            .columns
            .iter()
            .map(|column| Self::quote_mysql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        let referenced_columns = foreign_key
            .referenced_columns
            .iter()
            .map(|column| Self::quote_mysql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
            Self::qualified_table_name(database, foreign_key.table_name.as_str()),
            Self::quote_mysql_identifier(foreign_key.constraint_name.as_str()),
            columns,
            Self::qualified_table_name(database, foreign_key.referenced_table_name.as_str()),
            referenced_columns
        );
        if let Some(delete_rule) = Self::mysql_fk_action(foreign_key.delete_rule.as_deref()) {
            sql.push_str(" ON DELETE ");
            sql.push_str(delete_rule);
        }
        if let Some(update_rule) = Self::mysql_fk_action(foreign_key.update_rule.as_deref()) {
            sql.push_str(" ON UPDATE ");
            sql.push_str(update_rule);
        }
        Some(sql)
    }

    fn mysql_fk_action(rule: Option<&str>) -> Option<&'static str> {
        match rule.unwrap_or("").trim().to_ascii_uppercase().as_str() {
            "CASCADE" => Some("CASCADE"),
            "SET NULL" => Some("SET NULL"),
            "SET DEFAULT" => Some("SET DEFAULT"),
            "NO ACTION" => Some("NO ACTION"),
            "RESTRICT" => Some("RESTRICT"),
            _ => None,
        }
    }

    async fn foreign_key_exists(
        pool: &Pool<MySql>,
        database: &str,
        table_name: &str,
        constraint_name: &str,
    ) -> Result<bool, String> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS cnt
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = ?
              AND TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
              AND CONSTRAINT_NAME = ?
              AND CONSTRAINT_TYPE = 'FOREIGN KEY'
            "#,
        )
        .persistent(false)
        .bind(database)
        .bind(database)
        .bind(table_name)
        .bind(constraint_name)
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
        let count = row
            .try_get::<i64, _>("cnt")
            .or_else(|_| row.try_get::<u64, _>("cnt").map(|v| v as i64))
            .or_else(|_| row.try_get::<i64, _>(0))
            .or_else(|_| row.try_get::<u64, _>(0).map(|v| v as i64))
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn ensure_foreign_keys(&self) -> Result<(), String> {
        if !self.sync_foreign_key_tables {
            return Ok(());
        }
        let total = self
            .table_info_list
            .iter()
            .map(|table_info| table_info.foreign_keys.len() as u64)
            .sum();
        let stage = "sink.mysql.foreign_keys";
        runtime_progress::begin_schema_stage(stage, "MySQL sink 初始化后外键", total).await;
        for table_info in &self.table_info_list {
            if table_info.foreign_keys.is_empty() {
                continue;
            }
            let database =
                Self::table_info_target_database(table_info, self.primary_database.as_str());
            let pool = match self
                .target_pools
                .get(database.to_ascii_lowercase().as_str())
            {
                Some(pool) => pool,
                None => {
                    let error = format!("target database pool not found: {}", database);
                    runtime_progress::record_schema_stage_error(
                        stage,
                        database.as_str(),
                        error.as_str(),
                    )
                    .await;
                    runtime_progress::finish_schema_stage(stage).await;
                    return Err(error);
                }
            };
            for foreign_key in &table_info.foreign_keys {
                let exists = match Self::foreign_key_exists(
                    pool,
                    database.as_str(),
                    table_info.table_name.as_str(),
                    foreign_key.constraint_name.as_str(),
                )
                .await
                {
                    Ok(exists) => exists,
                    Err(e) => {
                        runtime_progress::record_schema_stage_error(
                            stage,
                            format!("{}.{}", database, foreign_key.constraint_name).as_str(),
                            e.as_str(),
                        )
                        .await;
                        runtime_progress::finish_schema_stage(stage).await;
                        return Err(e);
                    }
                };
                if exists {
                    runtime_progress::record_schema_stage_item(
                        stage,
                        format!("{}.{}", database, foreign_key.constraint_name).as_str(),
                    )
                    .await;
                    continue;
                }
                let Some(sql) = Self::foreign_key_sql(database.as_str(), foreign_key) else {
                    let error = format!(
                        "invalid foreign key metadata: {}.{}",
                        table_info.table_name, foreign_key.constraint_name
                    );
                    runtime_progress::record_schema_stage_error(
                        stage,
                        format!("{}.{}", database, foreign_key.constraint_name).as_str(),
                        error.as_str(),
                    )
                    .await;
                    runtime_progress::finish_schema_stage(stage).await;
                    return Err(error);
                };
                if let Err(e) = pool.execute(sql.as_str()).await {
                    let error = format!(
                        "MySQL auto add foreign key failed: {}.{} {}",
                        database, foreign_key.constraint_name, e
                    );
                    runtime_progress::record_schema_stage_error(
                        stage,
                        format!("{}.{}", database, foreign_key.constraint_name).as_str(),
                        error.as_str(),
                    )
                    .await;
                    runtime_progress::finish_schema_stage(stage).await;
                    return Err(error);
                }
                info!(
                    "MySQL auto add foreign key success: {}.{}",
                    database, foreign_key.constraint_name
                );
                runtime_progress::record_schema_stage_item(
                    stage,
                    format!("{}.{}", database, foreign_key.constraint_name).as_str(),
                )
                .await;
            }
        }
        runtime_progress::finish_schema_stage(stage).await;
        Ok(())
    }

    fn qualified_routine_name(database: &str, routine_name: &str) -> String {
        Self::qualified_table_name(database, routine_name)
    }

    async fn sync_stored_routines(
        config: &CdcConfig,
        target_pools: &HashMap<String, Pool<MySql>>,
        primary_database: &str,
    ) -> Result<(), String> {
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
            let target_database = if config.multi_mode_open() {
                config.target_database_for_source(source_database)
            } else {
                primary_database.to_string()
            };
            let target_pool = target_pools
                .get(target_database.to_ascii_lowercase().as_str())
                .ok_or_else(|| format!("target database pool not found: {}", target_database))?;
            let source_url = mysql_connection_url_from_config(
                &config.source_config[source_config_index],
                Some(source_database),
            );
            let source_pool =
                get_mysql_pool_by_url(&source_url, "mysql sink 同步存储程序-读取源库存储程序")
                    .await?;
            let routines = fetch_mysql_routines(
                &source_pool,
                source_database,
                &[MySqlRoutineKind::Function, MySqlRoutineKind::Procedure],
            )
            .await
            .map_err(|e| {
                format!(
                    "fetch source stored routines failed: {} {}",
                    source_database, e
                )
            })?;
            if routines.is_empty() {
                info!("MySQL source stored routine not found: {}", source_database);
                continue;
            }
            for routine in routines {
                Self::sync_one_stored_routine(
                    target_pool,
                    source_database,
                    target_database.as_str(),
                    &routine,
                    overwrite,
                )
                .await?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn show_create_procedure_sql(source_database: &str, routine_name: &str) -> String {
        show_create_mysql_routine_sql(source_database, MySqlRoutineKind::Procedure, routine_name)
    }

    fn mysql_utf8mb4_string_expr(value: &str) -> String {
        mysql_utf8mb4_string_expr(value)
    }

    #[cfg(test)]
    fn strip_create_procedure_definer(create_sql: &str) -> String {
        strip_create_routine_definer(create_sql, MySqlRoutineKind::Procedure)
    }

    async fn sync_one_stored_routine(
        target_pool: &Pool<MySql>,
        source_database: &str,
        target_database: &str,
        routine: &MySqlRoutineDefinition,
        overwrite: bool,
    ) -> Result<(), String> {
        let exists = Self::target_stored_routine_exists(
            target_pool,
            target_database,
            routine.kind,
            &routine.name,
        )
        .await?;
        if exists && !overwrite {
            info!(
                "MySQL stored routine exists, skip: {} {}.{} -> {}.{}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_database,
                routine.name
            );
            return Ok(());
        }

        let mut conn = target_pool.acquire().await.map_err(|e| e.to_string())?;
        let original_sql_mode = sqlx::raw_sql("SELECT @@SESSION.sql_mode AS sql_mode")
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| e.to_string())
            .map(|row| {
                row.try_get::<String, _>("sql_mode")
                    .or_else(|_| row.try_get::<String, _>(0))
                    .unwrap_or_default()
            })?;

        let sync_result = async {
            let use_sql = format!("USE {}", Self::quote_mysql_identifier(target_database));
            sqlx::raw_sql(use_sql.as_str())
                .execute(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            let set_sql_mode = format!(
                "SET SESSION sql_mode = {}",
                Self::mysql_utf8mb4_string_expr(routine.sql_mode.as_str())
            );
            sqlx::raw_sql(set_sql_mode.as_str())
                .execute(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            if exists {
                let drop_sql = format!(
                    "DROP {} IF EXISTS {}",
                    routine.kind.routine_type(),
                    Self::qualified_routine_name(target_database, routine.name.as_str())
                );
                sqlx::raw_sql(drop_sql.as_str())
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            sqlx::raw_sql(routine.create_sql.as_str())
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    format!(
                        "create stored routine failed: {} {}.{} {}",
                        routine.kind.routine_type(),
                        target_database,
                        routine.name,
                        e
                    )
                })
        }
        .await;
        let restore_sql_mode = format!(
            "SET SESSION sql_mode = {}",
            Self::mysql_utf8mb4_string_expr(original_sql_mode.as_str())
        );
        let restore_result = sqlx::raw_sql(restore_sql_mode.as_str())
            .execute(&mut *conn)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string());
        match (sync_result, restore_result) {
            (Ok(_), Ok(_)) => {}
            (Err(e), Ok(_)) => return Err(e),
            (Ok(_), Err(e)) => return Err(format!("restore target sql_mode failed: {}", e)),
            (Err(e), Err(restore_error)) => {
                return Err(format!(
                    "{}; restore target sql_mode failed: {}",
                    e, restore_error
                ));
            }
        }
        info!(
            "MySQL sync stored routine success: {} {}.{} -> {}.{} overwrite={}",
            routine.kind.routine_type(),
            source_database,
            routine.name,
            target_database,
            routine.name,
            overwrite
        );
        Ok(())
    }

    async fn target_stored_routine_exists(
        target_pool: &Pool<MySql>,
        target_database: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> Result<bool, String> {
        let sql = format!(
            r#"
            SELECT COUNT(*) AS cnt
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = {}
              AND ROUTINE_TYPE = '{}'
              AND ROUTINE_NAME = {}
            "#,
            Self::mysql_utf8mb4_string_expr(target_database),
            routine_kind.routine_type(),
            Self::mysql_utf8mb4_string_expr(routine_name)
        );
        let row = sqlx::raw_sql(sql.as_str())
            .fetch_one(target_pool)
            .await
            .map_err(|e| format!("{}: {}", sql, e))?;
        let count = row
            .try_get::<i64, _>("cnt")
            .or_else(|_| row.try_get::<i64, _>(0))
            .or_else(|_| row.try_get::<u64, _>("cnt").map(|v| v as i64))
            .or_else(|_| row.try_get::<u64, _>(0).map(|v| v as i64))
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn sync_stored_views(
        config: &CdcConfig,
        target_pools: &HashMap<String, Pool<MySql>>,
        primary_database: &str,
    ) -> Result<(), String> {
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
            let target_database = if config.multi_mode_open() {
                config.target_database_for_source(source_database)
            } else {
                primary_database.to_string()
            };
            let target_pool = target_pools
                .get(target_database.to_ascii_lowercase().as_str())
                .ok_or_else(|| format!("target database pool not found: {}", target_database))?;
            let source_url = mysql_connection_url_from_config(
                &config.source_config[source_config_index],
                Some(source_database),
            );
            let source_pool =
                get_mysql_pool_by_url(&source_url, "mysql sink 同步视图-读取源库视图").await?;
            let views = fetch_mysql_views(&source_pool, source_database, config)
                .await
                .map_err(|e| {
                    format!(
                        "fetch source views failed: {} -> {} {}",
                        source_database, target_database, e
                    )
                })?;
            if views.is_empty() {
                info!("MySQL source view not found: {}", source_database);
                continue;
            }
            Self::sync_views_with_retry(
                target_pool,
                source_database,
                target_database.as_str(),
                views,
            )
            .await?;
        }
        Ok(())
    }

    async fn sync_views_with_retry(
        target_pool: &Pool<MySql>,
        source_database: &str,
        target_database: &str,
        mut pending: Vec<MySqlViewDefinition>,
    ) -> Result<(), String> {
        while !pending.is_empty() {
            let mut next_pending = Vec::new();
            let mut errors = Vec::new();
            let mut progress = false;
            for view in pending {
                match Self::sync_one_stored_view(
                    target_pool,
                    source_database,
                    target_database,
                    &view,
                )
                .await
                {
                    Ok(created) => {
                        if created {
                            progress = true;
                        }
                    }
                    Err(e) => {
                        errors.push(e);
                        next_pending.push(view);
                    }
                }
            }
            if next_pending.is_empty() {
                return Ok(());
            }
            if !progress {
                return Err(format!(
                    "create MySQL views failed after dependency retry:\n{}",
                    errors.join("\n")
                ));
            }
            pending = next_pending;
        }
        Ok(())
    }

    async fn sync_one_stored_view(
        target_pool: &Pool<MySql>,
        source_database: &str,
        target_database: &str,
        view: &MySqlViewDefinition,
    ) -> Result<bool, String> {
        if mysql_view_exists(target_pool, target_database, view.name.as_str()).await? {
            info!(
                "MySQL view exists, skip: {}.{} -> {}.{}",
                source_database, view.name, target_database, view.name
            );
            return Ok(false);
        }

        let create_sql = rewrite_mysql_create_view_for_target(
            view.create_sql.as_str(),
            source_database,
            target_database,
            view.name.as_str(),
        )
        .map_err(|e| {
            format!(
                "rewrite MySQL view failed: {}.{} -> {}.{} {}",
                source_database, view.name, target_database, view.name, e
            )
        })?;
        Self::create_view_on_target(target_pool, target_database, view, create_sql.as_str())
            .await
            .map_err(|e| {
                format!(
                    "create MySQL view failed: {}.{} -> {}.{} sql={} error={}",
                    source_database,
                    view.name,
                    target_database,
                    view.name,
                    Self::sql_preview(create_sql.as_str()),
                    e
                )
            })?;
        info!(
            "MySQL sync view success: {}.{} -> {}.{}",
            source_database, view.name, target_database, view.name
        );
        Ok(true)
    }

    async fn create_view_on_target(
        target_pool: &Pool<MySql>,
        target_database: &str,
        view: &MySqlViewDefinition,
        create_sql: &str,
    ) -> Result<(), String> {
        let mut conn = target_pool.acquire().await.map_err(|e| e.to_string())?;
        let charset_row =
            sqlx::raw_sql("SELECT @@SESSION.character_set_client AS character_set_client, @@SESSION.collation_connection AS collation_connection")
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
        let original_character_set_client =
            mysql_row_text_value(&charset_row, "character_set_client");
        let original_collation_connection =
            mysql_row_text_value(&charset_row, "collation_connection");

        let sync_result = async {
            let use_sql = format!("USE {}", Self::quote_mysql_identifier(target_database));
            sqlx::raw_sql(use_sql.as_str())
                .execute(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            if !view.character_set_client.trim().is_empty() {
                let set_sql = format!(
                    "SET SESSION character_set_client = {}",
                    Self::mysql_utf8mb4_string_expr(view.character_set_client.as_str())
                );
                sqlx::raw_sql(set_sql.as_str())
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            if !view.collation_connection.trim().is_empty() {
                let set_sql = format!(
                    "SET SESSION collation_connection = {}",
                    Self::mysql_utf8mb4_string_expr(view.collation_connection.as_str())
                );
                sqlx::raw_sql(set_sql.as_str())
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            sqlx::raw_sql(create_sql)
                .execute(&mut *conn)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        .await;

        let restore_result = async {
            let restore_character_set_sql = format!(
                "SET SESSION character_set_client = {}",
                Self::mysql_utf8mb4_string_expr(original_character_set_client.as_str())
            );
            sqlx::raw_sql(restore_character_set_sql.as_str())
                .execute(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            let restore_collation_sql = format!(
                "SET SESSION collation_connection = {}",
                Self::mysql_utf8mb4_string_expr(original_collation_connection.as_str())
            );
            sqlx::raw_sql(restore_collation_sql.as_str())
                .execute(&mut *conn)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        }
        .await;

        match (sync_result, restore_result) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), Ok(_)) => Err(e),
            (Ok(_), Err(e)) => Err(format!("restore target charset failed: {}", e)),
            (Err(e), Err(restore_error)) => Err(format!(
                "{}; restore target charset failed: {}",
                e, restore_error
            )),
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

    fn validate_merged_target_schema(table_info_list: &[TableInfoVo]) -> Result<(), String> {
        let mut signatures: HashMap<String, (String, Vec<(String, String)>)> = HashMap::new();
        for table_info in table_info_list {
            let database = if table_info.target_database.trim().is_empty() {
                "".to_string()
            } else {
                table_info.target_database.clone()
            };
            let target_key =
                Self::target_table_key(database.as_str(), table_info.table_name.as_str())
                    .to_ascii_lowercase();
            let signature = Self::table_schema_signature(table_info);
            match signatures.get(&target_key) {
                None => {
                    signatures.insert(
                        target_key,
                        (table_info.pk_column.to_ascii_lowercase(), signature),
                    );
                }
                Some((pk, existing_signature)) => {
                    if !pk.eq_ignore_ascii_case(table_info.pk_column.as_str())
                        || existing_signature != &signature
                    {
                        return Err(format!(
                            "multi_mode target table schema mismatch: {}.{}",
                            database, table_info.table_name
                        ));
                    }
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
                    .and_then(|def| mysql_type_token_from_column_definition(def.as_str()))
                    .map(|type_token| normalize_mysql_column_type_token(type_token.as_str()))
                    .unwrap_or_else(|| "__missing_definition__".to_string())
                    .to_ascii_lowercase();
                (key, type_token)
            })
            .collect()
    }

    async fn get_stored_cols(
        pool: &Pool<MySql>,
        database: &str,
        table_name: &String,
    ) -> Vec<String> {
        // 去掉那些STORED的字段
        let stored_cols_sql = r#"
                    select COLUMN_NAME from information_schema.columns where EXTRA = 'STORED GENERATED' AND TABLE_SCHEMA = ? AND TABLE_NAME = ?;
                "#;
        let stored_cols: Vec<String> = sqlx::query(stored_cols_sql)
            .persistent(false)
            .bind(database)
            .bind(table_name)
            .fetch_all(pool)
            .await
            .unwrap()
            .iter()
            .map(|row| mysql_row_text_value(row, "COLUMN_NAME"))
            .collect();
        stored_cols
    }
    async fn get_exists_cols(
        pool: &Pool<MySql>,
        database: &str,
        table_name: &String,
    ) -> Vec<String> {
        // 去掉那些STORED的字段
        let stored_cols_sql = r#"
                    select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA = ? AND TABLE_NAME = ?;
                "#;
        let cols: Vec<String> = sqlx::query(stored_cols_sql)
            .persistent(false)
            .bind(database)
            .bind(table_name)
            .fetch_all(pool)
            .await
            .unwrap()
            .iter()
            .map(|row| mysql_row_text_value(row, "COLUMN_NAME"))
            .collect();
        cols
    }

    fn remove_cols(cols: &mut [String], to_remove_cols: &Vec<String>) -> Vec<String> {
        cols.iter()
            .filter(|f| {
                let mut b = true;
                for to_check in to_remove_cols {
                    if to_check.eq_ignore_ascii_case(f) {
                        b = false;
                        break;
                    }
                }
                b
            })
            .map(|c| c.to_string())
            .collect()
    }
    fn contains_cols(cols: &mut [String], to_remove_cols: &Vec<String>) -> Vec<String> {
        cols.iter()
            .filter(|f| {
                let mut b = false;
                for to_check in to_remove_cols {
                    if to_check.eq_ignore_ascii_case(f) {
                        b = true;
                        break;
                    }
                }
                b
            })
            .map(|c| c.to_string())
            .collect()
    }

    async fn execute_upsert_batch(
        pool: &Pool<MySql>,
        connection_url: &str,
        database: &str,
        table_name: &str,
        columns: &[String],
        inserts: &[common::case_insensitive_hash_map::CaseInsensitiveHashMap],
        disable_foreign_key_checks: bool,
    ) -> Result<MySqlQueryResult, String> {
        let cols_str = columns
            .iter()
            .map(|c| Self::quote_mysql_identifier(c))
            .collect::<Vec<_>>()
            .join(",");

        let placeholders_row = format!("({})", vec!["?"; columns.len()].join(","));

        let values_sql = (0..inserts.len())
            .map(|_| placeholders_row.clone())
            .collect::<Vec<_>>()
            .join(",");

        let updates_sql = columns
            .iter()
            .map(|c| {
                let col = Self::quote_mysql_identifier(c);
                format!("{} = VALUES({})", col, col)
            })
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
            Self::qualified_table_name(database, table_name),
            cols_str,
            values_sql,
            updates_sql
        );

        let query = Self::bind_upsert_query(sql.as_str(), columns, inserts);
        match Self::execute_query(pool, query, disable_foreign_key_checks).await {
            Ok(ok) => Ok(ok),
            Err(err) => {
                error!("Failed to execute query: {} 进行重试, sql: {}", err, sql);
                let error_message = err.to_string();
                Self::log_upsert_debug_rows(table_name, columns, inserts, error_message.as_str());
                if let Some(error_column) = mysql_error_column_name(error_message.as_str()) {
                    Self::log_target_column_metadata(pool, database, table_name, &error_column)
                        .await;
                }
                let new_pool =
                    get_mysql_pool_by_url(connection_url, "sql执行遇到报错，尝试重新获取连接")
                        .await
                        .map_err(|e| e.to_string())?;
                let retry_query = Self::bind_upsert_query(sql.as_str(), columns, inserts);
                Self::execute_query(&new_pool, retry_query, disable_foreign_key_checks)
                    .await
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn execute_query(
        pool: &Pool<MySql>,
        query: Query<'_, MySql, MySqlArguments>,
        disable_foreign_key_checks: bool,
    ) -> Result<MySqlQueryResult, sqlx::Error> {
        if !disable_foreign_key_checks {
            return query.execute(pool).await;
        }

        let mut conn = pool.acquire().await?;
        sqlx::query(Self::foreign_key_checks_sql(false))
            .persistent(false)
            .execute(&mut *conn)
            .await?;
        let query_result = query.execute(&mut *conn).await;
        let restore_result = sqlx::query(Self::foreign_key_checks_sql(true))
            .persistent(false)
            .execute(&mut *conn)
            .await;

        match (query_result, restore_result) {
            (Ok(ok), Ok(_)) => Ok(ok),
            (Err(e), Ok(_)) => Err(e),
            (Ok(_), Err(e)) => Err(e),
            (Err(e), Err(restore_error)) => {
                error!(
                    "Failed to restore MySQL FOREIGN_KEY_CHECKS after query error: {}",
                    restore_error
                );
                Err(e)
            }
        }
    }

    fn foreign_key_checks_sql(enabled: bool) -> &'static str {
        if enabled {
            "SET FOREIGN_KEY_CHECKS=1"
        } else {
            "SET FOREIGN_KEY_CHECKS=0"
        }
    }

    fn log_upsert_debug_rows(
        table_name: &str,
        columns: &[String],
        inserts: &[common::case_insensitive_hash_map::CaseInsensitiveHashMap],
        error_message: &str,
    ) {
        let mut debug_columns: Vec<String> = vec![];
        let mut seen = HashSet::new();
        let preferred_columns = [
            "Id",
            "path",
            "HouseId",
            "fullPath",
            "BadDebtDate",
            "bankCollectionLock",
            "beforePrice",
            "buildingName",
            "Sequence",
            "settleDate",
            "ShouldChargeDate",
            "stage",
            "standrdId",
        ];

        for preferred in preferred_columns {
            if let Some(column) = columns
                .iter()
                .find(|column| column.eq_ignore_ascii_case(preferred))
                && seen.insert(column.to_ascii_lowercase())
            {
                debug_columns.push(column.clone());
            }
        }

        let focus_columns = ["settleDate", "bankCollectionLock"];
        for focus_column in focus_columns {
            Self::push_debug_columns_around(columns, focus_column, &mut seen, &mut debug_columns);
        }

        if let Some(error_column) = mysql_error_column_name(error_message) {
            if let Some(column) = columns
                .iter()
                .find(|column| column.eq_ignore_ascii_case(error_column.as_str()))
                && seen.insert(column.to_ascii_lowercase())
            {
                debug_columns.push(column.clone());
            }
            Self::push_debug_columns_around(
                columns,
                error_column.as_str(),
                &mut seen,
                &mut debug_columns,
            );
        }

        for (row_index, row) in inserts.iter().take(3).enumerate() {
            let values = debug_columns
                .iter()
                .map(|column| format!("{}={:?}", column, row.get(column)))
                .collect::<Vec<_>>()
                .join(", ");
            error!(
                "MySQL UPSERT debug table={} row={} error_column={:?} values: {}",
                table_name,
                row_index + 1,
                mysql_error_column_name(error_message),
                values
            );
        }
    }

    fn push_debug_columns_around(
        columns: &[String],
        focus_column: &str,
        seen: &mut HashSet<String>,
        debug_columns: &mut Vec<String>,
    ) {
        if let Some(focus_index) = columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case(focus_column))
        {
            let start = focus_index.saturating_sub(5);
            let end = (focus_index + 5).min(columns.len().saturating_sub(1));
            for column in &columns[start..=end] {
                if seen.insert(column.to_ascii_lowercase()) {
                    debug_columns.push(column.clone());
                }
            }
        }
    }

    async fn log_target_column_metadata(
        pool: &Pool<MySql>,
        database: &str,
        table_name: &str,
        column_name: &str,
    ) {
        let sql = r#"
            SELECT
                COLUMN_NAME,
                COLUMN_TYPE,
                IS_NULLABLE,
                COALESCE(COLUMN_DEFAULT, '<NULL>') AS COLUMN_DEFAULT,
                EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
        "#;
        match sqlx::query(sql)
            .persistent(false)
            .bind(database)
            .bind(table_name)
            .bind(column_name)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(row)) => {
                error!(
                    "MySQL UPSERT target column metadata table={}.{} column={} type={} nullable={} default={} extra={}",
                    database,
                    table_name,
                    mysql_row_text_value(&row, "COLUMN_NAME"),
                    mysql_row_text_value(&row, "COLUMN_TYPE"),
                    mysql_row_text_value(&row, "IS_NULLABLE"),
                    mysql_row_text_value(&row, "COLUMN_DEFAULT"),
                    mysql_row_text_value(&row, "EXTRA")
                );
            }
            Ok(None) => {
                error!(
                    "MySQL UPSERT target column metadata not found table={}.{} column={}",
                    database, table_name, column_name
                );
            }
            Err(e) => {
                error!(
                    "MySQL UPSERT target column metadata query failed table={}.{} column={} error={}",
                    database, table_name, column_name, e
                );
            }
        }
    }

    fn bind_upsert_query<'a>(
        sql: &'a str,
        columns: &'a [String],
        inserts: &'a [common::case_insensitive_hash_map::CaseInsensitiveHashMap],
    ) -> Query<'a, MySql, MySqlArguments> {
        let mut query = sqlx::query(sql).persistent(false);
        for row in inserts {
            for col in columns {
                let x = row.get(col);
                debug!("inserting {:?} into {}", x, col);
                if !x.is_none() {
                    if x.is_json()
                        && let Value::Json(json) = x
                        && (json.is_empty() || json.eq_ignore_ascii_case("null"))
                    {
                        query = query.bind("null");
                    } else if let Value::Blob(bytes) = x {
                        query = query.bind(bytes.to_vec());
                    } else if let Value::Bit(v) = x {
                        query = query.bind(*v);
                    } else {
                        query = query.bind(x.resolve_string());
                    }
                } else {
                    query = query.bind(None::<String>);
                }
            }
        }
        query
    }

    async fn execute_delete_batch(
        pool: &Pool<MySql>,
        connection_url: &str,
        database: &str,
        table_name: &str,
        pk_name: &str,
        deletes: &[String],
        disable_foreign_key_checks: bool,
    ) -> Result<MySqlQueryResult, String> {
        let ph = (0..deletes.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!(
            "DELETE FROM {} WHERE {} IN ({})",
            Self::qualified_table_name(database, table_name),
            Self::quote_mysql_identifier(pk_name),
            ph
        );

        let query = Self::bind_delete_query(sql.as_str(), deletes);
        match Self::execute_query(pool, query, disable_foreign_key_checks).await {
            Ok(ok) => Ok(ok),
            Err(err) => {
                error!("Failed to execute query: {} 进行重试, sql: {}", err, sql);
                let new_pool =
                    get_mysql_pool_by_url(connection_url, "sql执行遇到报错，尝试重新获取连接")
                        .await
                        .map_err(|e| e.to_string())?;
                let retry_query = Self::bind_delete_query(sql.as_str(), deletes);
                Self::execute_query(&new_pool, retry_query, disable_foreign_key_checks)
                    .await
                    .map_err(|e| e.to_string())
            }
        }
    }

    fn bind_delete_query<'a>(
        sql: &'a str,
        deletes: &'a [String],
    ) -> Query<'a, MySql, MySqlArguments> {
        let mut query = sqlx::query(sql).persistent(false);
        for pk in deletes {
            query = query.bind(pk);
        }
        query
    }

    async fn flush_database_batch(
        database: String,
        pool: Pool<MySql>,
        connection_url: String,
        batch: Vec<DataBuffer>,
        columns_by_table: HashMap<String, Vec<String>>,
        pk_by_table: HashMap<String, String>,
        disable_foreign_key_checks: bool,
    ) -> Result<(), String> {
        let mut insert_map: HashMap<
            String,
            Vec<common::case_insensitive_hash_map::CaseInsensitiveHashMap>,
        > = HashMap::new();
        let mut delete_map: HashMap<String, Vec<String>> = HashMap::new();

        for r in batch {
            let table_name = r.table_name.clone();
            let table_key = Self::target_table_key(database.as_str(), table_name.as_str());
            let pk_name = pk_by_table
                .get(table_key.to_ascii_lowercase().as_str())
                .cloned()
                .unwrap_or_else(|| panic!("pk not found: {}", table_key));
            match r.op {
                Operation::CREATE(_) | Operation::UPDATE => {
                    insert_map
                        .entry(table_name.clone())
                        .or_default()
                        .push(r.after);
                }
                Operation::DELETE => {
                    let pk = r.get_pk(pk_name.as_str());
                    if !pk.is_none() {
                        delete_map
                            .entry(table_name.clone())
                            .or_default()
                            .push(pk.resolve_string());
                    }
                }
                _ => {
                    panic!("unexpected operation {:?}", r.op);
                }
            }
        }

        for (table_name, inserts) in insert_map {
            if inserts.is_empty() {
                error!("inserts is empty: {}.{}", database, table_name);
                continue;
            }
            let table_key =
                Self::target_table_key(database.as_str(), table_name.as_str()).to_ascii_lowercase();
            let columns = columns_by_table
                .get(table_key.as_str())
                .cloned()
                .unwrap_or_default();
            if columns.is_empty() {
                error!("columns is empty: {}.{}", database, table_name);
                continue;
            }
            if let Err(e) = Self::execute_upsert_batch(
                &pool,
                connection_url.as_str(),
                database.as_str(),
                table_name.as_str(),
                &columns,
                &inserts,
                disable_foreign_key_checks,
            )
            .await
            {
                error!("MySQL batch UPSERT error: {:?}", e);
                SINK_FLUSH_ERRORS_TOTAL
                    .with_label_values(&["mysql", "upsert"])
                    .inc();
                return Err("sql执行报错".to_string());
            }
        }

        for (table_name, deletes) in delete_map {
            if deletes.is_empty() {
                continue;
            }
            let table_key =
                Self::target_table_key(database.as_str(), table_name.as_str()).to_ascii_lowercase();
            let pk_name = pk_by_table
                .get(table_key.as_str())
                .cloned()
                .unwrap_or_else(|| panic!("pk not found: {}", table_key));
            if let Err(e) = Self::execute_delete_batch(
                &pool,
                connection_url.as_str(),
                database.as_str(),
                table_name.as_str(),
                pk_name.as_str(),
                &deletes,
                disable_foreign_key_checks,
            )
            .await
            {
                error!("MySQL batch delete error: {:?}", e);
                SINK_FLUSH_ERRORS_TOTAL
                    .with_label_values(&["mysql", "delete"])
                    .inc();
                return Err(e);
            }
        }

        Ok(())
    }

    async fn flush_records_in_order(
        &self,
        batch: &[DataBuffer],
        columns_by_table: &HashMap<String, Vec<String>>,
        pk_by_table: &HashMap<String, String>,
        disable_foreign_key_checks: bool,
    ) -> Result<(), String> {
        for record in batch {
            let database = self.record_target_database(record);
            let table_name = record.table_name.clone();
            let table_key =
                Self::target_table_key(database.as_str(), table_name.as_str()).to_ascii_lowercase();
            let pool = self
                .target_pools
                .get(database.to_ascii_lowercase().as_str())
                .unwrap_or_else(|| panic!("target database pool not found: {}", database));
            let connection_url = self
                .connection_urls
                .get(database.to_ascii_lowercase().as_str())
                .unwrap_or_else(|| panic!("target database url not found: {}", database));
            let pk_name = pk_by_table
                .get(table_key.as_str())
                .cloned()
                .unwrap_or_else(|| panic!("pk not found: {}", table_key));
            match record.op {
                Operation::CREATE(_) | Operation::UPDATE => {
                    let columns = columns_by_table
                        .get(table_key.as_str())
                        .cloned()
                        .unwrap_or_default();
                    if columns.is_empty() {
                        error!("columns is empty: {}.{}", database, table_name);
                        continue;
                    }
                    Self::execute_upsert_batch(
                        pool,
                        connection_url.as_str(),
                        database.as_str(),
                        table_name.as_str(),
                        &columns,
                        std::slice::from_ref(&record.after),
                        disable_foreign_key_checks,
                    )
                    .await
                    .map(|_| ())?;
                }
                Operation::DELETE => {
                    let pk = record.get_pk(pk_name.as_str());
                    if pk.is_none() {
                        continue;
                    }
                    let deletes = vec![pk.resolve_string()];
                    Self::execute_delete_batch(
                        pool,
                        connection_url.as_str(),
                        database.as_str(),
                        table_name.as_str(),
                        pk_name.as_str(),
                        &deletes,
                        disable_foreign_key_checks,
                    )
                    .await
                    .map(|_| ())?;
                }
                _ => {
                    return Err(format!("unexpected operation {:?}", record.op));
                }
            }
        }
        Ok(())
    }

    fn has_foreign_key_metadata(&self) -> bool {
        self.sync_foreign_key_tables
            && self
                .table_info_list
                .iter()
                .any(|table_info| !table_info.foreign_keys.is_empty())
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 测试连接
        for pool in self.target_pools.values() {
            pool.execute("SELECT 1").await?;
        }
        info!("Connected to MySQL via SQLx");
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
            .with_label_values(&["mysql", &trigger])
            .start_timer();

        if !*self.initialized.read().await {
            let stage = "sink.mysql.column_cache";
            let table_total = self
                .table_info_list
                .iter()
                .map(|table_info| {
                    let database = Self::table_info_target_database(
                        table_info,
                        self.primary_database.as_str(),
                    );
                    Self::target_table_key(database.as_str(), table_info.table_name.as_str())
                        .to_ascii_lowercase()
                })
                .collect::<HashSet<_>>()
                .len() as u64;
            runtime_progress::begin_schema_stage(stage, "MySQL sink 首次字段缓存", table_total)
                .await;
            let mut inserted_tables = HashSet::new();
            for table_info in &self.table_info_list {
                let database =
                    Self::table_info_target_database(table_info, self.primary_database.as_str());
                let table_key =
                    Self::target_table_key(database.as_str(), table_info.table_name.as_str());
                if !inserted_tables.insert(table_key.to_ascii_lowercase()) {
                    continue;
                }
                self.table_info_cache
                    .lock()
                    .await
                    .insert(table_key.clone(), table_info.clone());
            }

            let mut col_info: CaseInsensitiveHashMapVecString =
                CaseInsensitiveHashMapVecString::new_with_no_arg();
            for table_info in &self.table_info_list {
                let database =
                    Self::table_info_target_database(table_info, self.primary_database.as_str());
                let table_key =
                    Self::target_table_key(database.as_str(), table_info.table_name.as_str());
                if !col_info.get(table_key.as_str()).is_empty() {
                    continue;
                }
                let pool = self
                    .target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
                let mut cols = table_info.columns.clone();
                let stored_cols = Self::get_stored_cols(pool, database.as_str(), &table_name).await;
                let exists_cols = Self::get_exists_cols(pool, database.as_str(), &table_name).await;
                cols = Self::remove_cols(&mut cols, &stored_cols);
                cols = Self::contains_cols(&mut cols, &exists_cols);

                for c in cols {
                    col_info.entry_insert(table_key.as_str(), c.clone());
                }
                runtime_progress::record_schema_stage_item(stage, table_key.as_str()).await;
            }
            let mut cols = self.columns_cache.lock().await;

            *cols = col_info;
            *self.initialized.write().await = true;
            runtime_progress::finish_schema_stage(stage).await;
        }

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

        let cache_for_roll_back = batch.clone();
        let mut pk_by_table = HashMap::new();
        for table_info in &self.table_info_list {
            let database =
                Self::table_info_target_database(table_info, self.primary_database.as_str());
            let table_key =
                Self::target_table_key(database.as_str(), table_info.table_name.as_str())
                    .to_ascii_lowercase();
            pk_by_table
                .entry(table_key)
                .or_insert_with(|| table_info.pk_column.clone());
        }
        let columns_cache = self.columns_cache.lock().await.clone();
        let mut columns_by_table = HashMap::new();
        for table_key in columns_cache.keys() {
            columns_by_table.insert(table_key.to_ascii_lowercase(), columns_cache.get(table_key));
        }
        let disable_foreign_key_checks = *self.source_initializing.read().await;

        if self.has_foreign_key_metadata() {
            for record in &batch {
                let database = self.record_target_database(record);
                let table_label =
                    Self::target_table_key(database.as_str(), record.table_name.as_str());
                let op_str = match record.op {
                    Operation::CREATE(_) => "create",
                    Operation::UPDATE => "update",
                    Operation::DELETE => "delete",
                    _ => "other",
                };
                SINK_EVENTS_TOTAL
                    .with_label_values(&["mysql", table_label.as_str(), op_str])
                    .inc();
            }
            if let Err(e) = self
                .flush_records_in_order(
                    &batch,
                    &columns_by_table,
                    &pk_by_table,
                    disable_foreign_key_checks,
                )
                .await
            {
                error!("need to do it again: {}", cache_for_roll_back.len());
                let mut buf = self.buffer.lock().await;
                for cached_data_buffer in cache_for_roll_back {
                    buf.push(cached_data_buffer);
                }
                return Err(e);
            }
            return Ok(());
        }

        let mut db_batches: HashMap<String, Vec<DataBuffer>> = HashMap::new();
        for r in batch {
            let database = self.record_target_database(&r);
            let table_name = r.table_name.clone();
            debug!("Flushing Mysql Sink: {:?}", r);
            let op_str = match r.op {
                Operation::CREATE(_) => "create",
                Operation::UPDATE => "update",
                Operation::DELETE => "delete",
                _ => "other",
            };
            let table_label = Self::target_table_key(database.as_str(), table_name.as_str());
            SINK_EVENTS_TOTAL
                .with_label_values(&["mysql", table_label.as_str(), op_str])
                .inc();
            db_batches.entry(database).or_default().push(r);
        }

        let mut tasks = JoinSet::new();
        for (database, db_batch) in db_batches {
            let pool = self
                .target_pools
                .get(database.to_ascii_lowercase().as_str())
                .unwrap_or_else(|| panic!("target database pool not found: {}", database))
                .clone();
            let connection_url = self
                .connection_urls
                .get(database.to_ascii_lowercase().as_str())
                .unwrap_or_else(|| panic!("target database url not found: {}", database))
                .clone();
            let columns_by_table = columns_by_table.clone();
            let pk_by_table = pk_by_table.clone();
            tasks.spawn(async move {
                Self::flush_database_batch(
                    database,
                    pool,
                    connection_url,
                    db_batch,
                    columns_by_table,
                    pk_by_table,
                    disable_foreign_key_checks,
                )
                .await
            });
        }

        let mut first_error = None;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    first_error.get_or_insert(e);
                }
                Err(e) => {
                    first_error.get_or_insert(e.to_string());
                }
            }
        }
        if let Some(e) = first_error {
            error!("need to do it again: {}", cache_for_roll_back.len());
            let mut buf = self.buffer.lock().await;
            for cached_data_buffer in cache_for_roll_back {
                buf.push(cached_data_buffer);
            }
            return Err(e);
        }

        Ok(())
    }

    async fn alter_flush(&mut self) -> Result<(), String> {
        let entries = {
            let checkpoint = self.checkpoint.lock().await;
            checkpoint
                .iter()
                .map(|(key, cp)| (key.clone(), cp.clone()))
                .collect::<Vec<_>>()
        };
        if entries.is_empty() {
            return Ok(());
        }
        self.checkpoint_service
            .record_table_applied_many(entries)
            .await?;
        self.checkpoint.lock().await.clear();
        trace!("alter flush done");
        Ok(())
    }

    async fn after_initialization(&mut self) -> Result<(), String> {
        self.flush_with_retry(&FlushByOperation::Init).await;
        *self.source_initializing.write().await = false;
        self.ensure_foreign_keys().await?;
        runtime_progress::finish_schema_initialization().await;
        Ok(())
    }
}

fn mysql_error_column_name(message: &str) -> Option<String> {
    let marker = "for column '";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_info(source_database: &str, target_database: &str, column_type: &str) -> TableInfoVo {
        TableInfoVo {
            source_database: source_database.to_string(),
            target_database: target_database.to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: format!(
                "CREATE TABLE `orders` (\n  `id` bigint NOT NULL,\n  `name` {} DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
                column_type
            ),
            columns: vec!["id".to_string(), "name".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        }
    }

    #[test]
    fn create_table_sql_without_foreign_keys_removes_constraints() {
        let sql = r#"CREATE TABLE `child` (
  `id` bigint NOT NULL,
  `parent_id` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_parent_id` (`parent_id`),
  CONSTRAINT `fk_child_parent` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB"#;

        let stripped = MySqlSink::create_table_sql_without_foreign_keys(sql);

        assert!(!stripped.contains("FOREIGN KEY"));
        assert!(stripped.contains("KEY `idx_parent_id` (`parent_id`)"));
        assert!(stripped.contains("PRIMARY KEY (`id`)"));
        assert!(stripped.contains("KEY `idx_parent_id` (`parent_id`)\n) ENGINE=InnoDB"));
    }

    #[test]
    fn foreign_key_sql_quotes_identifiers_and_actions() {
        let foreign_key = ForeignKeyInfo {
            constraint_name: "fk_child_parent".to_string(),
            table_name: "child".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_table_name: "parent".to_string(),
            referenced_columns: vec!["id".to_string()],
            update_rule: Some("CASCADE".to_string()),
            delete_rule: Some("RESTRICT".to_string()),
        };

        assert_eq!(
            MySqlSink::foreign_key_sql("target-db", &foreign_key).unwrap(),
            "ALTER TABLE `target-db`.`child` ADD CONSTRAINT `fk_child_parent` FOREIGN KEY (`parent_id`) REFERENCES `target-db`.`parent` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE"
        );
    }

    #[test]
    fn foreign_key_checks_sql_toggles_mysql_session_setting() {
        assert_eq!(
            MySqlSink::foreign_key_checks_sql(false),
            "SET FOREIGN_KEY_CHECKS=0"
        );
        assert_eq!(
            MySqlSink::foreign_key_checks_sql(true),
            "SET FOREIGN_KEY_CHECKS=1"
        );
    }

    #[test]
    fn merged_target_schema_allows_identical_tables() {
        let tables = vec![
            table_info("src_a", "dst", "varchar(64)"),
            table_info("src_b", "dst", "varchar(64)"),
        ];

        assert!(MySqlSink::validate_merged_target_schema(&tables).is_ok());
    }

    #[test]
    fn merged_target_schema_rejects_type_mismatch() {
        let tables = vec![
            table_info("src_a", "dst", "varchar(64)"),
            table_info("src_b", "dst", "varchar(128)"),
        ];

        let err = MySqlSink::validate_merged_target_schema(&tables).unwrap_err();

        assert!(err.contains("schema mismatch"));
    }

    #[test]
    fn merged_target_schema_allows_same_table_in_different_targets() {
        let tables = vec![
            table_info("src_a", "dst_a", "varchar(64)"),
            table_info("src_b", "dst_b", "varchar(128)"),
        ];

        assert!(MySqlSink::validate_merged_target_schema(&tables).is_ok());
    }

    #[test]
    fn merged_target_schema_allows_integer_display_width_mismatch() {
        let tables = vec![
            table_info("src_a", "dst", "bigint(20)"),
            table_info("src_b", "dst", "bigint"),
        ];

        assert!(MySqlSink::validate_merged_target_schema(&tables).is_ok());
    }

    #[test]
    fn mysql_utf8mb4_string_expr_uses_hex_literal() {
        assert_eq!(
            MySqlSink::mysql_utf8mb4_string_expr("newsee-backlog"),
            "CONVERT(0x6E65777365652D6261636B6C6F67 USING utf8mb4)"
        );
        assert_eq!(MySqlSink::mysql_utf8mb4_string_expr(""), "''");
    }

    #[test]
    fn show_create_procedure_sql_quotes_database_and_routine() {
        assert_eq!(
            MySqlSink::show_create_procedure_sql("source-db", "sync`demo"),
            "SHOW CREATE PROCEDURE `source-db`.`sync``demo`"
        );
    }

    #[test]
    fn strips_create_procedure_definer() {
        let sql = "CREATE DEFINER=`source_user`@`%` PROCEDURE `sync_demo`() BEGIN SELECT 1; END";

        assert_eq!(
            MySqlSink::strip_create_procedure_definer(sql),
            "CREATE PROCEDURE `sync_demo`() BEGIN SELECT 1; END"
        );
    }

    #[test]
    fn strips_create_procedure_definer_with_spaces_inside_quotes() {
        let sql = "CREATE DEFINER=`source user`@`host name` PROCEDURE `sync_demo`() SQL SECURITY DEFINER BEGIN SELECT 'PROCEDURE'; END";

        let stripped = MySqlSink::strip_create_procedure_definer(sql);

        assert_eq!(
            stripped,
            "CREATE PROCEDURE `sync_demo`() SQL SECURITY DEFINER BEGIN SELECT 'PROCEDURE'; END"
        );
        assert!(!stripped.contains("source user"));
        assert!(stripped.contains("SQL SECURITY DEFINER"));
    }

    #[test]
    fn strip_create_procedure_definer_leaves_sql_without_definer() {
        let sql = "CREATE PROCEDURE `sync_demo`() BEGIN SELECT 1; END";

        assert_eq!(MySqlSink::strip_create_procedure_definer(sql), sql);
    }

    #[test]
    fn bit_values_resolve_as_numeric_text() {
        assert_eq!(Value::Bit(1).resolve_string(), "1");
        assert_eq!(Value::Bit(0).resolve_string(), "0");
    }

    #[test]
    fn mysql_error_column_name_extracts_column() {
        assert_eq!(
            mysql_error_column_name(
                "error returned from database: 1265 (01000): Data truncated for column 'bankCollectionLock' at row 1"
            )
            .as_deref(),
            Some("bankCollectionLock")
        );
    }
}
