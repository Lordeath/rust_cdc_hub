use async_trait::async_trait;
use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecString,
};
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_type_token_from_column_definition, normalize_mysql_column_type_token,
};
use common::{
    CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value,
    database_table_key, get_mysql_pool_by_url, mysql_connection_url_from_config,
    mysql_row_text_value,
};
use sqlx::mysql::{MySqlArguments, MySqlQueryResult};
use sqlx::query::Query;
use sqlx::{Executor, MySql, Pool};
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
    sink_batch_size: usize,

    // 缓存所有字段名（第一批数据会取一次）
    table_info_cache: Mutex<CaseInsensitiveHashMapTableInfoVo>,
    columns_cache: Mutex<CaseInsensitiveHashMapVecString>,
    // pk_cache: Mutex<CaseInsensitiveHashMapVecString>,
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
}

impl MySqlSink {
    pub async fn new(config: &CdcConfig, table_info_list: Vec<TableInfoVo>) -> Self {
        if let Err(e) = Self::validate_merged_target_schema(&table_info_list) {
            panic!("{}", e);
        }

        let target_databases = config.sink_databases();
        let primary_database = target_databases.first().cloned().unwrap_or_default();
        let mut target_pools = HashMap::new();
        let mut connection_urls = HashMap::new();
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
                Err(value) => return value,
            };
            target_pools.insert(database.to_ascii_lowercase(), pool);
            connection_urls.insert(database.to_ascii_lowercase(), connection_url);
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
            let sql = "select * from information_schema.`COLUMNS` where TABLE_SCHEMA = ? AND TABLE_NAME = ?";
            for table_info in target_tables.values() {
                let database =
                    Self::table_info_target_database(table_info, primary_database.as_str());
                let pool = target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
                let is_empty = sqlx::query(sql)
                    .persistent(false)
                    .bind(&database)
                    .bind(&table_name)
                    .fetch_all(pool)
                    .await
                    .unwrap()
                    .is_empty();
                if is_empty {
                    let create_table_sql = table_info.create_table_sql.clone();
                    pool.execute(create_table_sql.as_str())
                        .await
                        .expect("Failed to create table");
                }
            }
        }
        if config.auto_add_column.unwrap_or(true) {
            for table_info in target_tables.values() {
                let database =
                    Self::table_info_target_database(table_info, primary_database.as_str());
                let pool = target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
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
                            )
                        }
                    }
                }
            }
        }
        if config.auto_modify_column.unwrap_or(true) {
            for table_info in target_tables.values() {
                let database =
                    Self::table_info_target_database(table_info, primary_database.as_str());
                let pool = target_pools
                    .get(database.to_ascii_lowercase().as_str())
                    .unwrap_or_else(|| panic!("target database pool not found: {}", database));
                let table_name = table_info.table_name.clone();
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
                        Err(e) => error!(
                            "auto modify column failed: {}.{} {} {}",
                            database, table_name, src_col, e
                        ),
                    }
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
            sink_batch_size,
            table_info_cache: Mutex::new(CaseInsensitiveHashMapTableInfoVo::new_with_no_arg()),
            columns_cache: Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg()),
            // pk_cache: Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg()),
            checkpoint: Mutex::new(HashMap::new()),
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
        match query.execute(pool).await {
            Ok(ok) => Ok(ok),
            Err(err) => {
                error!("Failed to execute query: {} 进行重试, sql: {}", err, sql);
                let new_pool =
                    get_mysql_pool_by_url(connection_url, "sql执行遇到报错，尝试重新获取连接")
                        .await
                        .map_err(|e| e.to_string())?;
                let retry_query = Self::bind_upsert_query(sql.as_str(), columns, inserts);
                retry_query
                    .execute(&new_pool)
                    .await
                    .map_err(|e| e.to_string())
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
        match query.execute(pool).await {
            Ok(ok) => Ok(ok),
            Err(err) => {
                error!("Failed to execute query: {} 进行重试, sql: {}", err, sql);
                let new_pool =
                    get_mysql_pool_by_url(connection_url, "sql执行遇到报错，尝试重新获取连接")
                        .await
                        .map_err(|e| e.to_string())?;
                let retry_query = Self::bind_delete_query(sql.as_str(), deletes);
                retry_query
                    .execute(&new_pool)
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
            }
            let mut cols = self.columns_cache.lock().await;

            *cols = col_info;
            *self.initialized.write().await = true;
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
        let err_messages: Vec<String> = self
            .checkpoint
            .lock()
            .await
            .values()
            .map(|s| {
                match s.save() {
                    Ok(_) => "".to_string(),
                    Err(msg) => {
                        error!("{}", msg);
                        // Err(msg);
                        msg
                    }
                }
            })
            .find(|x| !x.is_empty())
            .into_iter()
            .collect();
        if !err_messages.is_empty() {
            return Err(err_messages.join("\n").to_string());
        }
        trace!("alter flush done");
        Ok(())
    }
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
        }
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
    fn bit_values_resolve_as_numeric_text() {
        assert_eq!(Value::Bit(1).resolve_string(), "1");
        assert_eq!(Value::Bit(0).resolve_string(), "0");
    }
}
