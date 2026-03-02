use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecCaseInsensitiveHashMap,
    CaseInsensitiveHashMapVecString,
};
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::{
    CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value,
    get_mysql_pool_by_url, mysql_row_to_hashmap,
};
use common::schema::{
    extract_mysql_create_table_column_definitions, mysql_column_allows_null_from_definition,
    mysql_type_token_from_column_definition,
};
use async_trait::async_trait;
use sqlx::mysql::{MySqlArguments, MySqlQueryResult};
use sqlx::query::Query;
use sqlx::{MySql, Pool};
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::log::trace;
use tracing::{debug, error, info};

pub struct MySqlSink {
    pool: Mutex<Pool<MySql>>,
    connection_url: String,
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
        let username = config.first_sink("username");
        let password = config.first_sink("password");
        let host = config.first_sink("host");
        let port = config.first_sink("port");
        let database = config.first_sink("database");
        let connection_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            username,
            password,
            host,
            port,
            database.clone(),
        );
        let pool = match Self::get_pool_auto_create_database(
            config,
            &username,
            &password,
            &host,
            &port,
            database,
            &connection_url,
        )
        .await
        {
            Ok(value) => value,
            Err(value) => return value,
        };

        // judge is need to create table
        if config.auto_create_table.unwrap_or(true) {
            let sql = "select * from information_schema.`COLUMNS` where TABLE_SCHEMA = (select database()) AND TABLE_NAME = ?";
            for table_info in &table_info_list {
                let table_name = table_info.table_name.clone();
                let is_empty = sqlx::query(sql)
                    .bind(&table_name)
                    .fetch_all(&pool)
                    .await
                    .unwrap()
                    .is_empty();
                if is_empty {
                    let create_table_sql = table_info.create_table_sql.clone();
                    sqlx::query(&create_table_sql)
                        .execute(&pool)
                        .await
                        .expect("Failed to create table");
                }
            }
        }
        if config.auto_add_column.unwrap_or(true) {
            for table_info in &table_info_list {
                let table_name = table_info.table_name.clone();
                let rows = sqlx::query(
                    "select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE from information_schema.`COLUMNS` where TABLE_SCHEMA = (select database()) AND TABLE_NAME = ?",
                )
                .bind(&table_name)
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
                let exists_set: HashSet<String> = rows
                    .iter()
                    .map(mysql_row_to_hashmap)
                    .map(|row| row.get("COLUMN_NAME").resolve_string().to_ascii_lowercase())
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
                    match sqlx::query(&alter_sql).execute(&pool).await {
                        Ok(_) => info!("auto add column success: {} {}", table_name, src_col),
                        Err(e) => error!("auto add column failed: {} {} {}", table_name, src_col, e),
                    }
                }
            }
        }
        if config.auto_modify_column.unwrap_or(true) {
            for table_info in &table_info_list {
                let table_name = table_info.table_name.clone();
                let defs = extract_mysql_create_table_column_definitions(
                    table_info.create_table_sql.as_str(),
                );

                let rows = sqlx::query(
                    "select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE from information_schema.`COLUMNS` where TABLE_SCHEMA = (select database()) AND TABLE_NAME = ?",
                )
                .bind(&table_name)
                .fetch_all(&pool)
                .await
                .unwrap_or_default();

                let mut sink_meta: HashMap<String, (String, bool)> = HashMap::new();
                for r in rows.iter().map(mysql_row_to_hashmap) {
                    let name = r.get("COLUMN_NAME").resolve_string();
                    let typ = r.get("COLUMN_TYPE").resolve_string();
                    let is_nullable = r.get("IS_NULLABLE").resolve_string();
                    sink_meta.insert(
                        name.to_ascii_lowercase(),
                        (typ.to_ascii_lowercase(), is_nullable.eq_ignore_ascii_case("YES")),
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

                    let need_modify_type = sink_type != src_type;
                    let need_modify_nullable = src_nullable && !sink_nullable;
                    if !need_modify_type && !need_modify_nullable {
                        continue;
                    }
                    let modify_sql = format!("ALTER TABLE `{}` MODIFY COLUMN {}", table_name, def);
                    match sqlx::query(&modify_sql).execute(&pool).await {
                        Ok(_) => info!("auto modify column success: {} {}", table_name, src_col),
                        Err(e) => error!(
                            "auto modify column failed: {} {} {}",
                            table_name, src_col, e
                        ),
                    }
                }
            }
        }
        let sink_batch_size = config.sink_batch_size.unwrap_or(256);
        MySqlSink {
            pool: Mutex::new(pool),
            connection_url,
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
        username: &String,
        password: &String,
        host: &String,
        port: &String,
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
                            &format!("mysql://{}:{}@{}:{}", username, password, host, port,),
                            "mysql sink 自动创建数据库-创建",
                        )
                        .await
                        .unwrap();
                        let sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", database.clone());
                        match sqlx::query(&sql)
                            .execute(&pool_for_auto_create_database)
                            .await
                        {
                            Ok(xx) => xx,
                            Err(e) => {
                                error!("Failed to create database: {}", e);
                                panic!("Failed to create database: {}", e);
                            }
                        };
                        let pool: Pool<MySql> =
                            get_mysql_pool_by_url(connection_url, "mysql sink 自动创建数据库-获取")
                                .await
                                .unwrap();
                        return Ok(pool);
                    }
                    error!("Failed to connect to MySQL: {}", e);
                    panic!("Failed to connect to MySQL: {}", e);
                }
            };
        Ok(pool)
    }

    async fn get_pk_name_from_cache(&self, table_name: &str) -> String {
        self.table_info_cache
            .lock()
            .await
            .get(table_name)
            .pk_column
            .to_string()
    }

    async fn execute_with_retry(
        &self,
        query: Query<'_, MySql, MySqlArguments>,
        sql: String,
    ) -> Result<MySqlQueryResult, String> {
        let result: Result<(Option<MySqlQueryResult>, Option<Pool<MySql>>), String> =
            match query.execute(&*self.pool.lock().await).await {
                Ok(ok) => Ok((Some(ok), None)),
                Err(err) => {
                    error!("Failed to execute query: {} 进行重试, sql: {}", err, sql);
                    match get_mysql_pool_by_url(
                        &self.connection_url,
                        "sql执行遇到报错，尝试重新获取连接",
                    )
                    .await
                    {
                        Ok(new_pool) => Ok((None, Some(new_pool))),
                        Err(e) => {
                            info!("重连失败");
                            Err(e.to_string())
                        }
                    }
                }
            };
        if result.is_ok() {
            let (query_result, new_pool) = result?;
            if let Some(query_result) = query_result {
                return Ok(query_result);
            }
            if let Some(new_pool) = new_pool {
                info!("重连成功，正在进行赋值");
                *self.pool.lock().await = new_pool;
                info!("赋值成功");
            }
        }
        Err("sql执行失败".to_string())
    }

    async fn get_stored_cols(&self, table_name: &String) -> Vec<String> {
        // 去掉那些STORED的字段
        let stored_cols_sql = r#"
                    select COLUMN_NAME from information_schema.columns where EXTRA = 'STORED GENERATED' AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?;
                "#;
        let stored_cols: Vec<String> = sqlx::query(stored_cols_sql)
            .bind(table_name)
            .fetch_all(&*self.pool.lock().await)
            .await
            .unwrap()
            .iter()
            .map(mysql_row_to_hashmap)
            .map(|row| row.get("COLUMN_NAME").resolve_string())
            .collect();
        stored_cols
    }
    async fn get_exists_cols(&self, table_name: &String) -> Vec<String> {
        // 去掉那些STORED的字段
        let stored_cols_sql = r#"
                    select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?;
                "#;
        let cols: Vec<String> = sqlx::query(stored_cols_sql)
            .bind(table_name)
            .fetch_all(&*self.pool.lock().await)
            .await
            .unwrap()
            .iter()
            .map(mysql_row_to_hashmap)
            .map(|row| row.get("COLUMN_NAME").resolve_string())
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
}

#[async_trait]
impl Sink for MySqlSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 测试连接
        sqlx::query("SELECT 1")
            .execute(&*self.pool.lock().await)
            .await?;
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
            for table_info in &self.table_info_list {
                self.table_info_cache
                    .lock()
                    .await
                    .insert(table_info.table_name.clone(), table_info.clone());
            }

            let mut col_info: CaseInsensitiveHashMapVecString =
                CaseInsensitiveHashMapVecString::new_with_no_arg();
            for table_info in &self.table_info_list {
                let table_name = table_info.table_name.clone();
                let mut cols = table_info.columns.clone();
                let stored_cols = self.get_stored_cols(&table_name).await;
                let exists_cols = self.get_exists_cols(&table_name).await;
                cols = Self::remove_cols(&mut cols, &stored_cols);
                cols = Self::contains_cols(&mut cols, &exists_cols);

                for c in cols {
                    col_info.entry_insert(&table_name, c.clone());
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


        let mut insert_map: CaseInsensitiveHashMapVecCaseInsensitiveHashMap =
            CaseInsensitiveHashMapVecCaseInsensitiveHashMap::new_with_no_arg();
        let mut delete_map: CaseInsensitiveHashMapVecString =
            CaseInsensitiveHashMapVecString::new_with_no_arg();

        let mut cache_for_roll_back: Vec<DataBuffer> = vec![];
        for r in batch {
            let table_name = r.table_name.clone();
            let pk_name = self.get_pk_name_from_cache(&table_name).await;
            cache_for_roll_back.push(r.clone());
            debug!("Flushing Mysql Sink: {:?}", r);
            let op_str = match r.op {
                Operation::CREATE(_) => "create",
                Operation::UPDATE => "update",
                Operation::DELETE => "delete",
                _ => "other",
            };
            SINK_EVENTS_TOTAL.with_label_values(&["mysql", &table_name, op_str]).inc();

            match r.op {
                Operation::CREATE(_) | Operation::UPDATE => {
                    insert_map.entry_insert(table_name.clone(), r.after);
                }
                Operation::DELETE => {
                    let pk = r.get_pk(pk_name.as_str());
                    if !pk.is_none() {
                        delete_map.entry_insert(&table_name.clone(), pk.resolve_string());
                    }
                }
                _ => {
                    panic!("unexpected operation {:?}", r.op);
                }
            }
        }


        let column_map = self.columns_cache.lock().await;

        // ======================================
        //       批量 UPSERT（INSERT ... ON DUP）
        // ======================================
        if !insert_map.is_empty() {
            for table_name in insert_map.keys() {
                let inserts = insert_map.get(table_name.clone().as_str());
                if inserts.is_empty() {
                    error!("inserts is empty: {}", &table_name);
                    continue;
                }

                let columns = column_map.get(table_name);
                if columns.is_empty() {
                    error!("columns is empty: {}", &table_name);
                    continue;
                }
                // let pk_name = self.get_pk_name_from_cache(&table_name).await;
                let cols_str = columns
                    .iter()
                    .map(|c| format!("`{}`", c))
                    .collect::<Vec<_>>()
                    .join(",");

                let placeholders_row = format!("({})", vec!["?"; columns.len()].join(","));

                let values_sql = (0..inserts.len())
                    .map(|_| placeholders_row.clone())
                    .collect::<Vec<_>>()
                    .join(",");

                let updates_sql = columns
                    .iter()
                    // .filter(|c| !c.eq_ignore_ascii_case(pk_name.as_str()))
                    .map(|c| format!("`{}` = VALUES(`{}`)", c, c))
                    .collect::<Vec<_>>()
                    .join(",");

                let sql = format!(
                    "INSERT INTO `{}` ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
                    table_name, cols_str, values_sql, updates_sql
                );

                let mut query = sqlx::query(&sql);

                for row in &inserts {
                    for col in &columns {
                        let x = row.get(col);
                        debug!("inserting {:?} into {}.{}", x, table_name, col);
                        if !x.is_none() {
                            if x.is_json()
                                && let Value::Json(json) = x
                                && (json.is_empty() || json.eq_ignore_ascii_case("null"))
                            {
                                // query = query.bind::<Json<_>>(Json(json));
                                query = query.bind("null");
                            } else {
                                query = query.bind(x.resolve_string());
                            }
                        } else {
                            query = query.bind(None::<String>);
                        }
                    }
                }
                debug!("MySQL batch UPSERT: {}", sql);
                if let Err(e) = self.execute_with_retry(query, sql.clone()).await {
                    error!("MySQL batch UPSERT error: {:?}", e);
                    SINK_FLUSH_ERRORS_TOTAL.with_label_values(&["mysql", "upsert"]).inc();
                    error!("need to do it again: {}", cache_for_roll_back.len());
                    let mut buf = self.buffer.lock().await;
                    for cached_data_buffer in cache_for_roll_back {
                        buf.push(cached_data_buffer);
                    }
                    return Err("sql执行报错".to_string());
                }
            }
        }

        // ======================================
        //             批量 DELETE
        // ======================================
        for table_name in delete_map.keys() {
            let deletes = delete_map.get(table_name);
            if !deletes.is_empty() {
                let pk_name = self.get_pk_name_from_cache(table_name).await;
                let ph = (0..deletes.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",");

                let sql = format!(
                    "DELETE FROM `{}` WHERE `{}` IN ({})",
                    table_name, pk_name, ph
                );

                let mut query = sqlx::query(&sql);
                for pk in deletes {
                    query = query.bind(pk);
                }

                if let Err(e) = self.execute_with_retry(query, sql.clone()).await {
                    error!("MySQL batch delete error: {:?}", e);
                    SINK_FLUSH_ERRORS_TOTAL.with_label_values(&["mysql", "delete"]).inc();
                    error!("need to do it again: {}", cache_for_roll_back.len());
                    let mut buf = self.buffer.lock().await;
                    for cached_data_buffer in cache_for_roll_back {
                        buf.push(cached_data_buffer);
                    }
                    return Err(e);
                }
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
