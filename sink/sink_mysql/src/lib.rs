use common::{
    CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo, Value,
    mysql_row_to_hashmap,
};
use meilisearch_sdk::macro_helper::async_trait;
use sqlx::{MySql, MySqlPool, Pool};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

const BATCH_SIZE: usize = 256;

pub struct MySqlSink {
    pool: Pool<MySql>,
    // table_name_list: Vec<String>,
    // pk_column: String,
    table_info_list: Vec<TableInfoVo>,
    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,

    // 缓存所有字段名（第一批数据会取一次）
    table_info_cache: Mutex<HashMap<String, TableInfoVo>>,
    columns_cache: Mutex<HashMap<String, Vec<String>>>,
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

        MySqlSink {
            pool,
            table_info_list,
            // table_name_list: vec![table_name],
            // pk_column,
            buffer: Mutex::new(Vec::with_capacity(BATCH_SIZE)),
            initialized: RwLock::new(false),
            table_info_cache: Mutex::new(HashMap::new()),
            columns_cache: Mutex::new(HashMap::new()),
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
        let pool: Pool<MySql> = match MySqlPool::connect(connection_url).await {
            Ok(o) => o,
            Err(e) => {
                if config.auto_create_database.unwrap_or(true) {
                    let pool_for_auto_create_database = MySqlPool::connect(&format!(
                        "mysql://{}:{}@{}:{}",
                        username, password, host, port,
                    ))
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
                    let pool: Pool<MySql> = MySqlPool::connect(connection_url).await.unwrap();
                    return Ok(pool);
                }
                error!("Failed to connect to MySQL: {}", e);
                panic!("Failed to connect to MySQL: {}", e);
            }
        };
        Ok(pool)
    }

    async fn get_pk_name_from_cache(&self, table_name: &String) -> String {
        let pk_name = self
            .table_info_cache
            .lock()
            .await
            .get(&table_name.clone())
            .unwrap()
            .pk_column
            .to_string();
        pk_name
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 测试连接
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        info!("Connected to MySQL via SQLx");
        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;
        buf.push(record.clone());

        if buf.len() >= BATCH_SIZE {
            drop(buf);
            self.flush_with_retry(&FlushByOperation::Signal).await;
        }

        Ok(())
    }

    async fn flush(
        &self,
        flush_by_operation: &FlushByOperation,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

        // let mut inserts = vec![];
        // let mut deletes: Vec<String> = vec![];
        let mut insert_map: HashMap<String, Vec<HashMap<String, Value>>> = HashMap::new();
        let mut delete_map: HashMap<String, Vec<String>> = HashMap::new();

        // let pk_name = self.pk_column.as_str();

        let mut cache_for_roll_back: Vec<DataBuffer> = vec![];
        for r in batch {
            let table_name = r.table_name.clone();
            // let mut table_info_vo = self.table_info_cache.lock().await
            //     .get(&table_name);
            if !self.table_info_cache.lock().await.contains_key(&table_name) {
                for table_info in &self.table_info_list {
                    if table_info.table_name == table_name {
                        // table_info_vo = Some(&table_info);
                        self.table_info_cache
                            .lock()
                            .await
                            .insert(table_name.clone(), table_info.clone());
                        break;
                    }
                }
            }
            // let table_info_vo = self.table_info_cache.lock().await.get(&table_name).unwrap();
            let pk_name = self
                .table_info_cache
                .lock()
                .await
                .get(&table_name)
                .unwrap()
                .pk_column
                .to_string();
            let table_name = self
                .table_info_cache
                .lock()
                .await
                .get(&table_name)
                .unwrap()
                .table_name
                .clone();
            cache_for_roll_back.push(r.clone());
            match r.op {
                Operation::CREATE | Operation::UPDATE => {
                    insert_map.entry(table_name).or_default().push(r.after)
                }
                Operation::DELETE => {
                    let pk = r.get_pk(pk_name.as_str());
                    if !pk.is_none() {
                        // deletes.push(pk.resolve_string());
                        delete_map
                            .entry(table_name)
                            .or_default()
                            .push(pk.resolve_string());
                    }
                }
                _ => {
                    panic!("unexpected operation {:?}", r.op);
                }
            }
        }

        // 初始化字段名（只做一次）
        if !*self.initialized.read().await {
            let mut col_info: HashMap<String, Vec<String>> = HashMap::new();
            for table_info in &self.table_info_list {
                let table_name = table_info.table_name.clone();
                let mut cols = table_info.columns.clone();
                // 去掉那些STORED的字段
                let stored_cols_sql = r#"
                    select COLUMN_NAME from information_schema.columns where EXTRA = 'STORED GENERATED' AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?;
                "#;
                let stored_cols: Vec<String> = sqlx::query(stored_cols_sql)
                    .bind(&table_name)
                    .fetch_all(&self.pool)
                    .await?
                    .iter()
                    .map(mysql_row_to_hashmap)
                    .map(|row| row.get("COLUMN_NAME").unwrap().resolve_string())
                    .collect();
                cols.retain(|c| !stored_cols.contains(c));

                col_info.entry(table_name).or_default().extend(cols);
            }
            let mut cols = self.columns_cache.lock().await;

            *cols = col_info;
            *self.initialized.write().await = true;
        }

        let column_map = self.columns_cache.lock().await;

        // ======================================
        //       批量 UPSERT（INSERT ... ON DUP）
        // ======================================
        if !insert_map.is_empty() {
            for (table_name, inserts) in insert_map {
                let columns = column_map.get(&table_name).unwrap();
                // let pk_name = self
                //     .table_info_cache
                //     .lock()
                //     .await
                //     .get(&table_name)
                //     .unwrap()
                //     .pk_column
                //     .to_string();
                let pk_name = self.get_pk_name_from_cache(&table_name).await;
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
                    .map(|c| format!("`{}` = VALUES(`{}`)", c, c))
                    .filter(|c| c != pk_name.as_str())
                    .collect::<Vec<_>>()
                    .join(",");

                let sql = format!(
                    "INSERT INTO `{}` ({})
                 VALUES {}
                 ON DUPLICATE KEY UPDATE {}",
                    table_name, cols_str, values_sql, updates_sql
                );

                let mut query = sqlx::query(&sql);

                for row in &inserts {
                    for col in columns {
                        // let value = row.get(col);
                        let v = row
                            .get(col)
                            .map(|v| v.resolve_string())
                            .unwrap_or_else(|| "null".to_string());
                        if v.eq("null") {
                            query = query.bind(None::<String>);
                        } else {
                            query = query.bind(v);
                        }
                    }
                }

                if let Err(e) = query.execute(&self.pool).await {
                    error!("MySQL batch UPSERT error: {:?}", e);
                    error!("need do it again: {}", cache_for_roll_back.len());
                    let mut buf = self.buffer.lock().await;
                    for cached_data_buffer in cache_for_roll_back {
                        buf.push(cached_data_buffer);
                    }
                    return Err(Box::new(e));
                }
            }
        }

        // ======================================
        //             批量 DELETE
        // ======================================
        for (table_name, deletes) in delete_map {
            if !deletes.is_empty() {
                // let table_info_vo = self.table_info_cache.lock().await.get(&table_name).unwrap();
                let pk_name = self.get_pk_name_from_cache(&table_name).await;
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

                if let Err(e) = query.execute(&self.pool).await {
                    error!("MySQL batch delete error: {:?}", e);
                    error!("need do it again: {}", cache_for_roll_back.len());
                    let mut buf = self.buffer.lock().await;
                    for cached_data_buffer in cache_for_roll_back {
                        buf.push(cached_data_buffer);
                    }
                    return Err(Box::new(e));
                }
            }
        }

        Ok(())
    }
}
