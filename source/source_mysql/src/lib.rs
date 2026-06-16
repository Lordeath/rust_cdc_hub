extern crate core;

use async_trait::async_trait;
use common::case_insensitive_hash_map::{CaseInsensitiveHashMap, CaseInsensitiveHashMapVecString};
use common::checkpoint_manager::{CheckpointManager, FileCheckpointManager};
use common::custom_error::{CustomError, CustomErrorType};
use common::metrics::{SOURCE_EVENTS_TOTAL, SOURCE_LAG_POSITION};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::runtime_progress;
use common::{
    CdcConfig, DataBuffer, FlushByOperation, Operation, Plugin, Sink, Source, TableInfoVo, Value,
    database_table_key, get_mysql_pool_by_url_with_max_connections,
    mysql_connection_url_from_config, mysql_row_text_value, mysql_row_to_hashmap,
    redact_connection_url_password,
};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::column::json::json_binary::JsonBinary;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Column;
use sqlx::FromRow;
use sqlx::Row;
use sqlx::mysql::MySqlRow;
use sqlx::{MySql, Pool, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

pub struct MySQLSource {
    streams: Vec<Option<BinlogStream>>,
    stream_groups: Vec<MysqlStreamGroup>,
    mysql_source: Vec<MysqlSourceConfigDetail>,
    pools: Vec<Pool<MySql>>,
    checkpoint_entities: Mutex<Vec<Mutex<HashMap<String, MysqlCheckPointDetailEntity>>>>,
    plugins: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>,
    binlog_filename_list: Mutex<Vec<Mutex<String>>>,
    binlog_position_list: Mutex<Vec<Mutex<u32>>>,
    checkpoint_manager: Arc<dyn CheckpointManager>,
    init_parallelism: usize,
}

#[derive(Debug, Clone)]
struct MysqlSourceConfig {
    mysql_source: Vec<MysqlSourceConfigDetail>,
    pools: Vec<Pool<MySql>>,
    stream_groups: Vec<MysqlStreamGroup>,
}

#[derive(Debug, Clone)]
struct MysqlStreamGroup {
    connection_url: String,
    server_id: u64,
    source_indices: Vec<usize>,
    start_binlog_filename: Option<String>,
    start_binlog_position: Option<u32>,
}

#[derive(Debug, Clone)]
struct BinlogTableColumnInfo {
    database_name: String,
    table_name: String,
    column_count: usize,
    row_column_names: Option<Vec<Option<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSourceConfigDetail {
    username: String,
    password: String,
    host: String,
    port: String,
    database: String,
    target_database: String,
    server_id: u64,
    connection_url: String,
    table_info_list: Vec<TableInfoVo>,
    batchsize: usize,
    start_binlog_filename: Option<String>,
    start_binlog_position: Option<u32>,
}

impl MysqlSourceConfig {
    pub async fn new(config: &CdcConfig) -> Self {
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        let mut pools: Vec<Pool<MySql>> = vec![];
        let mut stream_groups: Vec<MysqlStreamGroup> = vec![];

        // split table_name
        let configured_table_names: Vec<String> =
            CdcConfig::split_csv_value(config.first_source("table_name").as_str());

        let except_table_name_prefix: Vec<String> =
            CdcConfig::split_csv_value(config.first_source("except_table_name_prefix").as_str());

        let include_table_regex_str = config.first_source("include_table_regex");
        let include_regex = if !include_table_regex_str.is_empty() {
            Some(Regex::new(&include_table_regex_str).expect("Invalid include_table_regex"))
        } else {
            None
        };

        let exclude_table_regex_str = config.first_source("exclude_table_regex");
        let exclude_regex = if !exclude_table_regex_str.is_empty() {
            Some(Regex::new(&exclude_table_regex_str).expect("Invalid exclude_table_regex"))
        } else {
            None
        };

        let mut all_tables_collected: Vec<String> = vec![];
        let source_databases = config.source_databases();
        let logical_source_size = if config.multi_mode_open() {
            source_databases.len()
        } else {
            config.source_config.len()
        };

        for i in 0..logical_source_size {
            let config_index = if config.multi_mode_open() { 0 } else { i };
            let username = config.source("username", config_index);
            let password = config.source("password", config_index);
            let host = config.source("host", config_index);
            let port = config.source("port", config_index);
            let database = if config.multi_mode_open() {
                source_databases[i].clone()
            } else {
                config.source("database", config_index)
            };
            let target_database = config.target_database_for_source(&database);
            let server_id: u64 = config
                .source("server_id", config_index)
                .parse::<u64>()
                .unwrap_or(0);
            let start_binlog_filename = match config.source("binlog_filename", config_index) {
                s if s.is_empty() => None,
                s => Some(s),
            };
            let start_binlog_position = config
                .source("binlog_position", config_index)
                .parse::<u32>()
                .ok()
                .filter(|p| *p > 0);
            let connection_url = mysql_connection_url_from_config(
                &config.source_config[config_index],
                Some(&database),
            );
            let mut table_info_list: Vec<TableInfoVo> = vec![];
            let pool = get_mysql_pool_by_url_with_max_connections(
                &connection_url,
                "mysql source 初始化获取数据结构",
                1,
            )
            .await
            .unwrap_or_else(|e| panic!("MySQL source 初始化获取数据结构连接失败: {}", e));

            let mut current_source_tables = configured_table_names.clone();

            if current_source_tables.is_empty()
                || (current_source_tables.len() == 1
                    && (current_source_tables[0].eq_ignore_ascii_case("all")
                        || current_source_tables[0].eq_ignore_ascii_case("*")))
            {
                // get all tables
                let show_tables_sql = r#"
                    SELECT distinct c.TABLE_NAME AS table_name
                    FROM information_schema.COLUMNS c
                    WHERE c.TABLE_SCHEMA = (SELECT DATABASE())
                      AND c.COLUMN_KEY = 'PRI'
                      AND c.DATA_TYPE = 'bigint'
                      AND c.TABLE_NAME NOT IN (
                            SELECT TABLE_NAME
                            FROM information_schema.KEY_COLUMN_USAGE
                            WHERE TABLE_SCHEMA = (SELECT DATABASE())
                              AND REFERENCED_TABLE_NAME IS NOT NULL
                       )
                       AND c.TABLE_NAME NOT IN (
                            SELECT REFERENCED_TABLE_NAME
                            FROM information_schema.KEY_COLUMN_USAGE
                            WHERE TABLE_SCHEMA = (SELECT DATABASE())
                              AND REFERENCED_TABLE_NAME IS NOT NULL
                       )
                        AND c.TABLE_NAME NOT IN (
                                SELECT cc.TABLE_NAME AS table_name
                                FROM information_schema.COLUMNS cc
                                WHERE cc.TABLE_SCHEMA = (SELECT DATABASE())
                                    AND cc.COLUMN_KEY = 'PRI'
                                    AND cc.DATA_TYPE = 'bigint'
                                GROUP BY cc.TABLE_NAME
                                HAVING COUNT(*) > 1
                        )
                "#;
                let tables: Vec<String> = sqlx::query(show_tables_sql)
                    .fetch_all(&pool)
                    .await
                    .expect("query failed")
                    .into_iter()
                    .map(|row| mysql_row_text_value(&row, "table_name"))
                    // .map(|row| row.table_name)
                    .collect();
                info!("get all tables from {}: {:?}", database, tables);
                current_source_tables = tables;
            }

            // Apply filters
            current_source_tables.retain(|table_name| {
                if Self::judge_is_skip(&except_table_name_prefix, table_name) {
                    return false;
                }
                if let Some(re) = &exclude_regex {
                    if re.is_match(table_name) {
                        info!("Exclude table by regex: {}", table_name);
                        return false;
                    }
                }
                if let Some(re) = &include_regex {
                    if !re.is_match(table_name) {
                        info!("Skip table not matching include regex: {}", table_name);
                        return false;
                    }
                }
                true
            });

            for table_name in current_source_tables.clone() {
                // fill table_info
                let show_create_table_sql = format!(
                    "show create table {}",
                    qualified_mysql_table_name(&database, &table_name)
                );
                info!("{}", show_create_table_sql);
                let show_create_table_result = sqlx::query(&show_create_table_sql)
                    .fetch_one(&pool)
                    .await
                    .expect("query failed");
                let create_table_sql = show_create_table_result.get(1);
                let pk_column_sql = r#"
                    select COLUMN_NAME as column_name, COLUMN_KEY as column_key, DATA_TYPE as data_type
                    from information_schema.`COLUMNS`
                    where TABLE_SCHEMA = (select database()) AND TABLE_NAME = ?
                "#;
                let col_list = sqlx::query_as::<_, ColumnInfoFromMysql>(pk_column_sql)
                    .bind(table_name.clone())
                    .fetch_all(&pool)
                    .await
                    .expect("query failed");

                let pk_column: Vec<String> = col_list
                    .iter()
                    .filter(|c| c.column_key == "PRI")
                    .filter(|c| c.data_type.eq_ignore_ascii_case("bigint"))
                    .map(|c| c.column_name.clone())
                    .collect();
                if pk_column.is_empty() || pk_column.len() > 1 {
                    error!(
                        "pk_column is empty or more than one for table {}",
                        table_name
                    );
                    // panic!("pk_column is empty or more than one");
                    continue;
                }
                let pk_column = pk_column[0].clone();
                let columns: Vec<String> = col_list.iter().map(|c| c.column_name.clone()).collect();

                table_info_list.push(TableInfoVo {
                    source_database: database.clone(),
                    target_database: target_database.clone(),
                    table_name: table_name.clone(),
                    pk_column,
                    create_table_sql,
                    columns,
                });
            }
            mysql_source.push(MysqlSourceConfigDetail {
                username,
                password,
                host,
                port,
                database,
                target_database,
                server_id,
                connection_url,
                table_info_list,
                batchsize: config.source_batch_size.unwrap_or(8192),
                start_binlog_filename,
                start_binlog_position,
            });
            pools.push(pool);

            all_tables_collected.extend(current_source_tables);
        }

        if all_tables_collected.is_empty() {
            error!("no table found after filtering");
            panic!("no table found after filtering");
        }

        if config.multi_mode_open() {
            stream_groups.push(MysqlStreamGroup {
                connection_url: mysql_connection_url_from_config(&config.source_config[0], None),
                server_id: config.source("server_id", 0).parse::<u64>().unwrap_or(0),
                source_indices: (0..mysql_source.len()).collect(),
                start_binlog_filename: match config.source("binlog_filename", 0) {
                    s if s.is_empty() => None,
                    s => Some(s),
                },
                start_binlog_position: config
                    .source("binlog_position", 0)
                    .parse::<u32>()
                    .ok()
                    .filter(|p| *p > 0),
            });
        } else {
            for (index, source) in mysql_source.iter().enumerate() {
                stream_groups.push(MysqlStreamGroup {
                    connection_url: source.connection_url.clone(),
                    server_id: source.server_id,
                    source_indices: vec![index],
                    start_binlog_filename: source.start_binlog_filename.clone(),
                    start_binlog_position: source.start_binlog_position,
                });
            }
        }

        MysqlSourceConfig {
            mysql_source,
            pools,
            stream_groups,
        }
    }

    fn judge_is_skip(except_table_name_prefix: &Vec<String>, table_name: &String) -> bool {
        let mut skip = false;
        for skip_prefix in except_table_name_prefix {
            if table_name
                .to_lowercase()
                .starts_with(skip_prefix.to_lowercase().as_str())
            {
                skip = true;
                info!("跳过表: {} 使用的表前缀: {}", table_name, skip_prefix);
                break;
            }
        }
        skip
    }
}

#[derive(Debug, FromRow)]
pub struct ColumnInfoFromMysql {
    pub column_name: String,
    // 或者使用自定义类型转换
    #[sqlx(try_from = "Vec<u8>")]
    pub column_key: String,
    #[sqlx(try_from = "Vec<u8>")]
    pub data_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitPkCursor {
    Start,
    Signed(i64),
    Unsigned(u64),
}

impl InitPkCursor {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int8(v) => Some(Self::Signed(*v as i64)),
            Value::Int16(v) => Some(Self::Signed(*v as i64)),
            Value::Int32(v) => Some(Self::Signed(*v as i64)),
            Value::Int64(v) => Some(Self::Signed(*v)),
            Value::UnsignedInt8(v) => Some(Self::Unsigned(*v as u64)),
            Value::UnsignedInt16(v) => Some(Self::Unsigned(*v as u64)),
            Value::UnsignedInt32(v) => Some(Self::Unsigned(*v as u64)),
            Value::UnsignedInt64(v) => Some(Self::Unsigned(*v)),
            _ => None,
        }
    }

    fn sql_literal(&self) -> Option<String> {
        match self {
            Self::Start => None,
            Self::Signed(v) => Some(v.to_string()),
            Self::Unsigned(v) => Some(v.to_string()),
        }
    }

    fn display_value(&self) -> String {
        self.sql_literal().unwrap_or_else(|| "START".to_string())
    }

    fn advance(&mut self, next: Self) {
        match (*self, next) {
            (Self::Start, _) => *self = next,
            (Self::Signed(current), Self::Signed(next_value)) if next_value > current => {
                *self = next
            }
            (Self::Unsigned(current), Self::Unsigned(next_value)) if next_value > current => {
                *self = next
            }
            (Self::Signed(current), Self::Unsigned(next_value))
                if current < 0 || next_value > current as u64 =>
            {
                *self = next
            }
            (Self::Unsigned(current), Self::Signed(next_value))
                if next_value >= 0 && (next_value as u64) > current =>
            {
                *self = next
            }
            _ => {}
        }
    }
}

impl MysqlSourceConfigDetail {
    #[inline]
    fn source_table_key(&self, table_name: &str) -> String {
        database_table_key(self.database.as_str(), table_name)
    }

    #[inline]
    fn is_target_database_and_table(&self, database_name: &str, table_name: &str) -> bool {
        if !self.database.eq_ignore_ascii_case(database_name) {
            return false;
        }
        for table_info in &self.table_info_list {
            if table_name.eq_ignore_ascii_case(&table_info.table_name) {
                return true;
            }
        }
        false
    }

    #[inline]
    async fn fill_table_column(&self, table_name: &str, pool: &Pool<MySql>) -> Vec<String> {
        let sql = r#"
            select COLUMN_NAME as column_name from information_schema.`COLUMNS` c
            where 1=1
            and c.TABLE_SCHEMA = ?
            and c.TABLE_NAME = ?
            order by c.ORDINAL_POSITION
        "#;
        sqlx::query(sql)
            .bind(self.database.clone())
            .bind(table_name)
            .fetch_all(pool)
            .await
            .unwrap_or_else(|e| panic!("Error executing query: {}", e))
            .into_iter()
            .map(|row| row.get("column_name"))
            .collect()
    }

    #[inline]
    async fn extract_init_data(
        &self,
        table_name: &str,
        pk_column: &str,
        cursor: &InitPkCursor,
        executor: &mut Transaction<'_, MySql>,
    ) -> Vec<DataBuffer> {
        let where_clause = cursor
            .sql_literal()
            .map(|id| format!("where {} > {}", quote_mysql_identifier(pk_column), id))
            .unwrap_or_default();
        let sql = format!(
            r#"
                select *
                FROM {}
                {}
                order by {}
                limit {}
            "#,
            qualified_mysql_table_name(&self.database, table_name),
            where_clause,
            quote_mysql_identifier(pk_column),
            self.batchsize
        );
        debug!(
            "extract_init_data: [{}.{}] {} {}",
            self.database,
            table_name,
            pk_column,
            cursor.display_value()
        );
        // 查询 Row，而不是 HashMap
        let rows: Vec<MySqlRow> = sqlx::query(&sql)
            .fetch_all(&mut **executor)
            .await
            .expect("query failed");
        info!(
            "extract_init_data: [{}.{}] {} {} {} rows",
            self.database,
            table_name,
            pk_column,
            cursor.display_value(),
            rows.len()
        );
        let mut result: Vec<DataBuffer> = vec![];
        for row in rows {
            let before = CaseInsensitiveHashMap::new(HashMap::new());
            let after = mysql_row_to_hashmap(&row);
            let op = Operation::CREATE(true);
            result.push(DataBuffer::new_with_route(
                self.database.clone(),
                self.target_database.clone(),
                table_name.to_string(),
                before,
                after,
                op,
                "".to_string(),
                0,
                0,
            ));
        }
        result
    }
}

impl MySQLSource {
    pub async fn new(config: &CdcConfig) -> Self {
        let mut streams: Vec<Option<BinlogStream>> = vec![];
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        let mut pools: Vec<Pool<MySql>> = vec![];
        // 这里支持多个数据源配置
        let cfg: MysqlSourceConfig = MysqlSourceConfig::new(config).await;
        let size = cfg.mysql_source.len();
        let binlog_filename_list: Mutex<Vec<Mutex<String>>> = Mutex::new(Vec::new());
        let binlog_position_list: Mutex<Vec<Mutex<u32>>> = Mutex::new(Vec::new());
        let mut checkpoint_entities: Vec<Mutex<HashMap<String, MysqlCheckPointDetailEntity>>> =
            vec![];
        for i in 0..size {
            let connection_url = cfg.mysql_source[i].connection_url.clone();
            let tables: Vec<String> = cfg.mysql_source[i]
                .table_info_list
                .clone()
                .iter()
                .map(|t| database_table_key(t.source_database.as_str(), t.table_name.as_str()))
                .collect::<Vec<String>>();
            let mut mysql_checkpoint_detail_entity_map = HashMap::new();
            for table in tables {
                let mysql_checkpoint_detail_entity = MysqlCheckPointDetailEntity::from_config(
                    config
                        .checkpoint_file_path
                        .clone()
                        .unwrap_or("/checkpoint".to_string()),
                    &connection_url,
                    table.clone(),
                )
                .await;
                mysql_checkpoint_detail_entity_map
                    .insert(table.to_lowercase(), mysql_checkpoint_detail_entity);
            }

            if let (Some(f), Some(p)) = (
                cfg.mysql_source[i].start_binlog_filename.clone(),
                cfg.mysql_source[i].start_binlog_position,
            ) {
                for entity in mysql_checkpoint_detail_entity_map.values_mut() {
                    if entity.is_new {
                        entity.last_binlog_filename = f.clone();
                        entity.last_binlog_position = p;
                    }
                }
            }
            checkpoint_entities.push(Mutex::new(mysql_checkpoint_detail_entity_map.clone()));

            mysql_source.push(cfg.mysql_source[i].clone());
            let pool: Pool<MySql> = cfg.pools[i].clone();
            pools.push(pool);
        }
        for _ in &cfg.stream_groups {
            // Stream creation deferred to start()
            streams.push(None);
            binlog_filename_list
                .lock()
                .await
                .push(Mutex::new("".to_string()));
            binlog_position_list.lock().await.push(Mutex::new(0));
        }

        let checkpoint_manager = Arc::new(FileCheckpointManager::new(
            config
                .checkpoint_file_path
                .clone()
                .unwrap_or("/checkpoint".to_string()),
        ));

        Self {
            streams,
            stream_groups: cfg.stream_groups,
            mysql_source,
            pools,
            checkpoint_entities: Mutex::new(checkpoint_entities),
            plugins: vec![],
            binlog_filename_list,
            binlog_position_list,
            checkpoint_manager,
            init_parallelism: config.multi_mode_init_parallelism(),
        }
    }

    async fn save_checkpoints(&self) {
        let max = self.pools.len();
        for i in 0..max {
            let checkpoints_guard = self.checkpoint_entities.lock().await;
            let checkpoints = checkpoints_guard[i].lock().await;
            for (table_name, cp) in checkpoints.iter() {
                if let Err(e) = self.checkpoint_manager.save(table_name, cp).await {
                    error!("Failed to save checkpoint for {}: {}", table_name, e);
                }
            }
        }
    }

    async fn write_record_with_retry(
        sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>,
        data_buffer: &DataBuffer,
        mysql_check_point_detail_entity: Option<MysqlCheckPointDetailEntity>,
    ) {
        let mut loop_count = 0;
        loop {
            trace!("write record retry: {}", loop_count);
            let sink_result = sink
                .lock()
                .await
                .write_record(data_buffer, &mysql_check_point_detail_entity)
                .await;
            trace!("write record retry result: {:?}", sink_result);
            if sink_result.is_ok() {
                break;
            }
            loop_count += 1;
            if loop_count >= 3 {
                panic!("flush error");
            }
        }
    }

    async fn fetch_master_status(pool: &Pool<MySql>) -> Result<(String, u32), CustomError> {
        let row: MySqlRow = sqlx::query("SHOW MASTER STATUS")
            .fetch_one(pool)
            .await
            .map_err(|e| CustomError {
                message: e.to_string(),
                error_type: CustomErrorType::Restart,
            })?;
        let columns: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
        let file: String = row
            .try_get::<String, _>("File")
            .or_else(|_| row.try_get::<String, _>("Log_name"))
            .or_else(|_| row.try_get::<String, _>(0))
            .map_err(|e| CustomError {
                message: format!(
                    "SHOW MASTER STATUS 读取 binlog file 失败: {}; columns={:?}",
                    e, columns
                ),
                error_type: CustomErrorType::Restart,
            })?;
        let pos_u64 = match row
            .try_get::<u64, _>("Position")
            .or_else(|_| row.try_get::<u64, _>("Pos"))
            .or_else(|_| row.try_get::<u64, _>(1))
        {
            Ok(v) => v,
            Err(e1) => {
                let s: String = row
                    .try_get::<String, _>("Position")
                    .or_else(|_| row.try_get::<String, _>(1))
                    .map_err(|e2| CustomError {
                        message: format!(
                            "SHOW MASTER STATUS 读取 Position 失败: {}; prior_err={}; columns={:?}",
                            e2, e1, columns
                        ),
                        error_type: CustomErrorType::Restart,
                    })?;
                s.parse::<u64>().map_err(|e| CustomError {
                    message: format!("SHOW MASTER STATUS Position 解析失败: {}; value={}", e, s),
                    error_type: CustomErrorType::Restart,
                })?
            }
        };
        let pos: u32 = u32::try_from(pos_u64).map_err(|_| CustomError {
            message: format!("SHOW MASTER STATUS Position 超出 u32: {}", pos_u64),
            error_type: CustomErrorType::Restart,
        })?;
        Ok((file, pos))
    }

    async fn initialize_source_index(
        source_index: usize,
        config: MysqlSourceConfigDetail,
        pool: Pool<MySql>,
        mut checkpoints: HashMap<String, MysqlCheckPointDetailEntity>,
        plugins: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>,
        mut sink: Arc<Mutex<dyn Sink + Send + Sync>>,
        checkpoint_manager: Arc<dyn CheckpointManager>,
    ) -> Result<(usize, HashMap<String, MysqlCheckPointDetailEntity>, bool), CustomError> {
        let any_new = config.table_info_list.iter().any(|table_info| {
            let table_key = config.source_table_key(table_info.table_name.as_str());
            checkpoints
                .get(table_key.to_lowercase().as_str())
                .map(|cp| cp.is_new)
                .unwrap_or(false)
        });
        if !any_new {
            info!(
                "No new tables found for {}, skipping full load",
                config.database
            );
            return Ok((source_index, checkpoints, false));
        }

        info!(
            "Detected new tables for {}, starting consistent snapshot initialization",
            config.database
        );
        let (file, pos) = Self::fetch_master_status(&pool).await?;
        info!(
            "Consistent Snapshot Position for {}: {}/{}",
            config.database, file, pos
        );

        for table_info in &config.table_info_list {
            let table_key = config.source_table_key(table_info.table_name.as_str());
            if let Some(cp) = checkpoints.get_mut(table_key.to_lowercase().as_str())
                && cp.is_new
            {
                cp.last_binlog_filename = file.clone();
                cp.last_binlog_position = pos;
            }
        }

        let mut tx = pool.begin().await.map_err(|e| CustomError {
            message: e.to_string(),
            error_type: CustomErrorType::Restart,
        })?;

        for table_info_vo in config.table_info_list.clone() {
            let table_name = table_info_vo.table_name.clone();
            let table_key = config.source_table_key(table_name.as_str());
            let is_new_table = checkpoints
                .get(table_key.to_lowercase().as_str())
                .map(|c| c.is_new)
                .unwrap_or(false);

            if !is_new_table {
                continue;
            }

            let pk_column = table_info_vo.pk_column.clone();
            let redacted_connection_url = redact_connection_url_password(&config.connection_url);
            let progress_label =
                progress_table_label(config.database.as_str(), table_name.as_str());
            info!(
                "开始初始化数据源: {}.{}",
                redacted_connection_url, table_name
            );
            runtime_progress::begin_table_initialization(&progress_label).await;
            let start = Instant::now();
            let mut count = 0;
            let mut cursor = InitPkCursor::Start;
            loop {
                let data_buffer_list: Vec<DataBuffer> = config
                    .extract_init_data(&table_name, &pk_column, &cursor, &mut tx)
                    .await;
                let len = data_buffer_list.len();
                for data_buffer in data_buffer_list {
                    runtime_progress::record_read(
                        &progress_label,
                        "initializing",
                        pk_value_for_progress(&data_buffer, &pk_column),
                    )
                    .await;
                    let this_id = data_buffer.after.get(&pk_column);
                    let next_id = InitPkCursor::from_value(this_id).unwrap_or_else(|| {
                        panic!(
                            "pk_column value is not supported as init cursor: {}.{} {} {:?}",
                            config.database, table_name, pk_column, this_id
                        )
                    });
                    let plugin_data = detail_with_plugin(&plugins, data_buffer).await;
                    if let Ok(item) = plugin_data {
                        Self::write_record_with_retry(&mut sink, &item, None).await;
                        runtime_progress::record_synced(&progress_label).await;
                    } else {
                        runtime_progress::record_filtered(&progress_label).await;
                    }
                    cursor.advance(next_id);
                }
                count += len;
                debug!("当前最大id为 {}", cursor.display_value());
                if len != config.batchsize {
                    break;
                }
            }
            sink.lock()
                .await
                .flush_with_retry(&FlushByOperation::Init)
                .await;
            info!(
                "MySQL数据源初始化完成 {}.{} count: {} cost: {:?}",
                redacted_connection_url,
                table_name,
                count,
                start.elapsed()
            );
            runtime_progress::finish_table_initialization(&progress_label).await;
            if let Some(cp) = checkpoints.get_mut(table_key.to_lowercase().as_str()) {
                cp.is_new = false;
                match checkpoint_manager.save(&table_key, cp).await {
                    Ok(_) => info!("alter_flush success {}", cp.checkpoint_filepath),
                    Err(e) => error!("alter_flush error: {}", e),
                }
            }
        }
        tx.commit().await.map_err(|e| CustomError {
            message: e.to_string(),
            error_type: CustomErrorType::Restart,
        })?;
        Ok((source_index, checkpoints, true))
    }

    fn source_index_for_event(
        &self,
        stream_index: usize,
        database_name: &str,
        table_name: &str,
    ) -> Option<usize> {
        self.stream_groups
            .get(stream_index)?
            .source_indices
            .iter()
            .copied()
            .find(|source_index| {
                self.mysql_source[*source_index]
                    .is_target_database_and_table(database_name, table_name)
            })
    }
}

fn binlog_table_column_info(event: &TableMapEvent) -> BinlogTableColumnInfo {
    BinlogTableColumnInfo {
        database_name: event.database_name.clone(),
        table_name: event.table_name.clone(),
        column_count: event.column_types.len(),
        row_column_names: table_map_column_names(event)
            .map(|names| names.into_iter().map(Some).collect()),
    }
}

fn table_map_column_names(event: &TableMapEvent) -> Option<Vec<String>> {
    let metadata = event.table_metadata.as_ref()?;
    if metadata.columns.len() != event.column_types.len() {
        return None;
    }

    let column_names: Option<Vec<String>> = metadata
        .columns
        .iter()
        .map(|column| column.column_name.clone())
        .collect();
    column_names.filter(|names| names.iter().all(|name| !name.is_empty()))
}

fn table_map_column_visibility(event: &TableMapEvent) -> Option<Vec<bool>> {
    let metadata = event.table_metadata.as_ref()?;
    if metadata.columns.len() != event.column_types.len() {
        return None;
    }

    let mut has_visibility_metadata = false;
    let visibility = metadata
        .columns
        .iter()
        .map(|column| match column.is_visible {
            Some(is_visible) => {
                has_visibility_metadata = true;
                is_visible
            }
            None => true,
        })
        .collect::<Vec<_>>();
    has_visibility_metadata.then_some(visibility)
}

fn align_current_columns_to_row_columns(
    table_label: &str,
    columns: Vec<String>,
    row_column_count: usize,
    visibility: Option<Vec<bool>>,
    generated_columns: &[(usize, String)],
    column_source: &str,
) -> Vec<Option<String>> {
    if columns.len() == row_column_count {
        return columns.into_iter().map(Some).collect();
    }

    if let Some(visibility) = visibility
        && visibility.len() == row_column_count
    {
        let visible_count = visibility.iter().filter(|visible| **visible).count();
        if visible_count == columns.len() {
            let mut columns = columns.into_iter();
            return visibility
                .into_iter()
                .map(|visible| if visible { columns.next() } else { None })
                .collect();
        }
        warn!(
            "MySQL binlog TableMap可见列数与{}列数不一致 table={} row_columns={} visible_columns={} {}_columns={}",
            column_source,
            table_label,
            row_column_count,
            visible_count,
            column_source,
            columns.len()
        );
    }

    if columns.len() + generated_columns.len() == row_column_count
        && generated_columns
            .iter()
            .all(|(ordinal, _)| *ordinal < row_column_count)
    {
        warn!(
            "MySQL binlog row列数与{}列数不一致 table={} row_columns={} {}_columns={}; \
             根据SHOW CREATE TABLE中的生成列位置跳过: {:?}",
            column_source,
            table_label,
            row_column_count,
            column_source,
            columns.len(),
            generated_columns
        );
        let mut columns = columns.into_iter();
        return (0..row_column_count)
            .map(|ordinal| {
                if generated_columns
                    .iter()
                    .any(|(generated_ordinal, _)| *generated_ordinal == ordinal)
                {
                    None
                } else {
                    columns.next()
                }
            })
            .collect();
    }

    warn!(
        "MySQL binlog row列数与{}列数不一致 table={} row_columns={} {}_columns={}; \
         将按可匹配列解析，多余字段值会被忽略。若缺少的是隐藏列且TableMap没有visibility元数据，字段仍可能错位",
        column_source,
        table_label,
        row_column_count,
        column_source,
        columns.len()
    );
    let mut columns = columns.into_iter().map(Some).collect::<Vec<_>>();
    if columns.len() > row_column_count {
        columns.truncate(row_column_count);
    } else {
        columns.resize(row_column_count, None);
    }
    columns
}

fn reconcile_row_column_names(
    table_label: &str,
    mut column_names: Vec<Option<String>>,
    row_column_count: usize,
    column_source: &str,
) -> Vec<Option<String>> {
    if column_names.len() == row_column_count {
        return column_names;
    }

    warn!(
        "MySQL binlog row列数与{}列数不一致 table={} row_columns={} {}_columns={}; \
         将按可匹配列解析，多余字段值会被忽略",
        column_source,
        table_label,
        row_column_count,
        column_source,
        column_names.len()
    );
    if column_names.len() > row_column_count {
        column_names.truncate(row_column_count);
    } else {
        column_names.resize(row_column_count, None);
    }
    column_names
}

fn generated_columns_from_create_table_sql(create_table_sql: &str) -> Vec<(usize, String)> {
    let mut result = vec![];
    let mut ordinal = 0usize;
    for (column_name, line) in column_lines_from_create_table_sql(create_table_sql) {
        let line_lower = line.to_ascii_lowercase();
        if line_lower.contains("generated always")
            || line_lower.contains("stored generated")
            || line_lower.contains("virtual generated")
        {
            result.push((ordinal, column_name));
        }
        ordinal += 1;
    }
    result
}

fn column_names_from_create_table_sql(create_table_sql: &str) -> Vec<String> {
    column_lines_from_create_table_sql(create_table_sql)
        .into_iter()
        .map(|(column_name, _)| column_name)
        .collect()
}

fn column_lines_from_create_table_sql(create_table_sql: &str) -> Vec<(String, String)> {
    let mut result = vec![];
    for raw_line in create_table_sql.lines() {
        let line = raw_line.trim();
        if !line.starts_with('`') {
            continue;
        }
        let Some(second_tick) = line[1..].find('`').map(|i| i + 1) else {
            continue;
        };
        result.push((line[1..second_tick].to_string(), line.to_string()));
    }
    result
}

fn generated_columns_for_table(
    config: &MysqlSourceConfigDetail,
    table_name: &str,
) -> Vec<(usize, String)> {
    config
        .table_info_list
        .iter()
        .find(|table_info| table_info.table_name.eq_ignore_ascii_case(table_name))
        .map(|table_info| {
            generated_columns_from_create_table_sql(table_info.create_table_sql.as_str())
        })
        .unwrap_or_default()
}

fn create_table_columns_for_table(
    config: &MysqlSourceConfigDetail,
    table_name: &str,
) -> Vec<String> {
    config
        .table_info_list
        .iter()
        .find(|table_info| table_info.table_name.eq_ignore_ascii_case(table_name))
        .map(|table_info| column_names_from_create_table_sql(table_info.create_table_sql.as_str()))
        .unwrap_or_default()
}

async fn current_table_columns_for_row(
    table_name: &str,
    row_column_count: usize,
    columns_map: &Mutex<CaseInsensitiveHashMapVecString>,
    config: &MysqlSourceConfigDetail,
    pool: &Pool<MySql>,
) -> Vec<String> {
    let table_key = config.source_table_key(table_name);
    let mut columns = columns_map.lock().await.get(table_key.as_str());
    if columns.is_empty() || columns.len() != row_column_count {
        let columns_new = config.fill_table_column(table_name, pool).await;
        columns = columns_new.clone();
        columns_map.lock().await.insert(table_key, columns_new);
    }
    columns
}

async fn resolve_table_map_column_info(
    event: &TableMapEvent,
    existing: Option<&BinlogTableColumnInfo>,
    columns_map: &Mutex<CaseInsensitiveHashMapVecString>,
    config: &MysqlSourceConfigDetail,
    pool: &Pool<MySql>,
) -> BinlogTableColumnInfo {
    let mut info = binlog_table_column_info(event);
    if info.row_column_names.is_some() {
        return info;
    }

    if let Some(existing) = existing
        && existing
            .database_name
            .eq_ignore_ascii_case(&info.database_name)
        && existing.table_name.eq_ignore_ascii_case(&info.table_name)
        && existing.column_count == info.column_count
        && existing.row_column_names.is_some()
    {
        info.row_column_names = existing.row_column_names.clone();
        return info;
    }

    let create_table_columns = create_table_columns_for_table(config, info.table_name.as_str());
    if create_table_columns.len() == info.column_count {
        let table_label = database_table_key(info.database_name.as_str(), info.table_name.as_str());
        warn!(
            "MySQL binlog TableMap缺少列名元数据 table={} row_columns={}; 使用SHOW CREATE TABLE完整列顺序对齐",
            table_label, info.column_count
        );
        info.row_column_names = Some(create_table_columns.into_iter().map(Some).collect());
        return info;
    }

    let current_columns = current_table_columns_for_row(
        info.table_name.as_str(),
        info.column_count,
        columns_map,
        config,
        pool,
    )
    .await;
    let table_label = database_table_key(info.database_name.as_str(), info.table_name.as_str());
    info.row_column_names = Some(align_current_columns_to_row_columns(
        table_label.as_str(),
        current_columns,
        info.column_count,
        table_map_column_visibility(event),
        &generated_columns_for_table(config, info.table_name.as_str()),
        "当前表结构",
    ));
    info
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResumePosition {
    Latest,
    BinlogPosition(String, u32),
}

fn compute_resume_position(
    runtime_binlog_filename: Option<&str>,
    runtime_binlog_position: Option<u32>,
    checkpoints: &HashMap<String, MysqlCheckPointDetailEntity>,
) -> ResumePosition {
    if let (Some(f), Some(p)) = (runtime_binlog_filename, runtime_binlog_position)
        && !f.is_empty()
        && p > 0
    {
        return ResumePosition::BinlogPosition(f.to_string(), p);
    }
    let max: MysqlCheckPointDetailEntity = checkpoints
        .values()
        .max_by(|a, b| {
            a.last_binlog_filename
                .cmp(&b.last_binlog_filename)
                .then(a.last_binlog_position.cmp(&b.last_binlog_position))
        })
        .unwrap()
        .clone();
    if max.last_binlog_filename.is_empty() || max.last_binlog_position == 0 {
        return ResumePosition::Latest;
    }
    ResumePosition::BinlogPosition(max.last_binlog_filename, max.last_binlog_position)
}

fn should_ignore_read_error(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    msg.contains("timeout") || msg.contains("timed out")
}

fn should_reconnect_read_error(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    msg.contains("unexpected end of file")
        || msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("connection closed")
        || msg.contains("connection aborted")
        || msg.contains("eof")
}

fn quote_mysql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn qualified_mysql_table_name(database: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        quote_mysql_identifier(database),
        quote_mysql_identifier(table_name)
    )
}

fn pk_value_for_progress(data_buffer: &DataBuffer, pk_column: &str) -> Option<String> {
    let value = data_buffer.get_pk(pk_column);
    if value.is_none() {
        None
    } else {
        Some(value.resolve_string())
    }
}

fn progress_table_label(source_database: &str, table_name: &str) -> String {
    database_table_key(source_database, table_name)
}

fn table_pk_column(config: &MysqlSourceConfigDetail, table_name: &str) -> Option<String> {
    config
        .table_info_list
        .iter()
        .find(|table| table.table_name.eq_ignore_ascii_case(table_name))
        .map(|table| table.pk_column.clone())
}

async fn detail_with_plugin(
    plugins: &Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>,
    data_buffer: DataBuffer,
) -> Result<DataBuffer, ()> {
    if plugins.is_empty() {
        return Ok(data_buffer);
    }
    let mut data_buffer = data_buffer;
    for p in plugins {
        let after_plugin = p.lock().await.collect(data_buffer).await;
        if after_plugin.is_err() {
            return Err(());
        }
        data_buffer = after_plugin?;
    }
    Ok(data_buffer)
}

impl MySQLSource {
    /// 关闭MySQLSource并释放所有连接池资源
    pub async fn close(&mut self) {
        info!("Closing MySQLSource and releasing connection pools...");
        // 关闭所有连接池
        for pool in self.pools.drain(..) {
            pool.close().await;
        }
        // 清空streams
        for stream in self.streams.iter_mut() {
            *stream = None;
        }
        info!("MySQLSource closed successfully");
    }
}

#[async_trait]
impl Source for MySQLSource {
    async fn start(
        &mut self,
        mut sink: Arc<Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), CustomError> {
        {
            info!("开始MySQL数据源初始化");
            let start_all = Instant::now();
            let semaphore = Arc::new(Semaphore::new(self.init_parallelism));
            let mut init_tasks = JoinSet::new();
            for i in 0..self.pools.len() {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| CustomError {
                        message: e.to_string(),
                        error_type: CustomErrorType::Restart,
                    })?;
                let config = self.mysql_source[i].clone();
                let pool = self.pools[i].clone();
                let checkpoints = {
                    let checkpoints_guard = self.checkpoint_entities.lock().await;
                    checkpoints_guard[i].lock().await.clone()
                };
                let plugins = self.plugins.clone();
                let sink_for_task = sink.clone();
                let checkpoint_manager = self.checkpoint_manager.clone();
                init_tasks.spawn(async move {
                    let _permit = permit;
                    Self::initialize_source_index(
                        i,
                        config,
                        pool,
                        checkpoints,
                        plugins,
                        sink_for_task,
                        checkpoint_manager,
                    )
                    .await
                });
            }
            let mut any_initialized = false;
            while let Some(result) = init_tasks.join_next().await {
                let (source_index, checkpoints, initialized) =
                    result.map_err(|e| CustomError {
                        message: e.to_string(),
                        error_type: CustomErrorType::Restart,
                    })??;
                any_initialized |= initialized;
                let checkpoints_guard = self.checkpoint_entities.lock().await;
                *checkpoints_guard[source_index].lock().await = checkpoints;
            }
            if any_initialized {
                runtime_progress::finish_initialization().await;
            }

            for (stream_index, group) in self.stream_groups.iter().enumerate() {
                let mut group_checkpoints: HashMap<String, MysqlCheckPointDetailEntity> =
                    HashMap::new();
                {
                    let checkpoints_guard = self.checkpoint_entities.lock().await;
                    for source_index in &group.source_indices {
                        group_checkpoints
                            .extend(checkpoints_guard[*source_index].lock().await.clone());
                    }
                }

                let resume_position = compute_resume_position(
                    group.start_binlog_filename.as_deref(),
                    group.start_binlog_position,
                    &group_checkpoints,
                );
                let start_position = match resume_position {
                    ResumePosition::Latest => StartPosition::Latest,
                    ResumePosition::BinlogPosition(f, p) => StartPosition::BinlogPosition(f, p),
                };
                let start_pos_str = match &start_position {
                    StartPosition::Latest => "Latest".to_string(),
                    StartPosition::BinlogPosition(f, p) => format!("{}/{}", f, p),
                    StartPosition::Gtid(g) => format!("GTID:{}", g),
                };
                info!(
                    "Connecting to Binlog: {} with start_position: {}",
                    redact_connection_url_password(&group.connection_url),
                    start_pos_str
                );
                let client =
                    BinlogClient::new(&group.connection_url, group.server_id, start_position)
                        .with_master_heartbeat(Duration::from_secs(5))
                        .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
                        .connect()
                        .await
                        .unwrap_or_else(|e| {
                            error!(
                                "MySQL source binlog连接失败 url: {} error: {}",
                                redact_connection_url_password(&group.connection_url),
                                e
                            );
                            panic!("MySQL source binlog连接失败: {}", e);
                        });
                self.streams[stream_index] = Some(client);
            }
            info!("MySQL数据源初始化完成, cost: {:?}", start_all.elapsed());
        }

        info!("Starting MySQL binlog source");
        let mut columns: Mutex<CaseInsensitiveHashMapVecString> =
            Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg());
        // 这里获取列名
        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        let mut table_column_info_map: HashMap<(usize, u64), BinlogTableColumnInfo> =
            HashMap::new();
        let mut last_checkpoint_save = Instant::now();
        loop {
            if last_checkpoint_save.elapsed().as_secs() >= 5 {
                self.save_checkpoints().await;
                last_checkpoint_save = Instant::now();
            }
            let max = self.streams.len();
            for i in 0..max {
                match self.streams[i].as_mut().unwrap().read().await {
                    Ok((header, data)) => {
                        *self.binlog_position_list.lock().await[i].lock().await =
                            header.next_event_position;
                        match data {
                            EventData::Rotate(event) => {
                                *self.binlog_filename_list.lock().await[i].lock().await =
                                    event.binlog_filename.clone();
                            }
                            EventData::TableMap(event) => {
                                let key = (i, event.table_id);
                                let column_info = match self.source_index_for_event(
                                    i,
                                    event.database_name.as_str(),
                                    event.table_name.as_str(),
                                ) {
                                    Some(source_index) => {
                                        resolve_table_map_column_info(
                                            &event,
                                            table_column_info_map.get(&key),
                                            &columns,
                                            &self.mysql_source[source_index],
                                            &self.pools[source_index],
                                        )
                                        .await
                                    }
                                    None => binlog_table_column_info(&event),
                                };
                                let table_name = event.table_name;
                                let table_id = event.table_id;
                                let database_name = event.database_name;
                                table_map.insert((i, table_id), table_name);
                                table_database_map.insert((i, table_id), database_name);
                                table_column_info_map.insert(key, column_info);
                            }
                            EventData::WriteRows(event) => {
                                let table_name = table_map
                                    .get(&(i, event.table_id))
                                    .unwrap_or_else(|| {
                                        panic!("Table id {} not found", event.table_id)
                                    })
                                    .clone();
                                let database_name = table_database_map
                                    .get(&(i, event.table_id))
                                    .unwrap_or_else(|| {
                                        panic!("Table id {} not found", event.table_id)
                                    })
                                    .clone();
                                let Some(source_index) = self.source_index_for_event(
                                    i,
                                    database_name.as_str(),
                                    table_name.as_str(),
                                ) else {
                                    continue;
                                };
                                let plugins = self.plugins.clone();
                                let pool: &mut Pool<MySql> = &mut self.pools[source_index];
                                let config: &MysqlSourceConfigDetail =
                                    &mut self.mysql_source[source_index];
                                let table_key = config.source_table_key(table_name.as_str());
                                let progress_label = progress_table_label(
                                    database_name.as_str(),
                                    table_name.as_str(),
                                );
                                let binlog_columns =
                                    table_column_info_map.get(&(i, event.table_id)).cloned();
                                let mut to_modify = self.checkpoint_entities.lock().await
                                    [source_index]
                                    .lock()
                                    .await
                                    .clone();
                                if config.is_target_database_and_table(
                                    database_name.as_str(),
                                    table_name.as_str(),
                                ) {
                                    debug!("WriteRows: {}.{}", database_name, table_name);

                                    let checkpoint_entity = self.checkpoint_entities.lock().await
                                        [source_index]
                                        .lock()
                                        .await
                                        .get(table_key.to_lowercase().as_str())
                                        .expect("checkpoint_entity not found")
                                        .clone();
                                    let mut checkpoint_entity = checkpoint_entity.clone();

                                    let binlog_filename = self.binlog_filename_list.lock().await[i]
                                        .lock()
                                        .await
                                        .clone();
                                    let timestamp = header.timestamp;
                                    let next_event_position = header.next_event_position;
                                    let pk_column = table_pk_column(config, table_name.as_str());
                                    for row in event.rows {
                                        let before: CaseInsensitiveHashMap =
                                            CaseInsensitiveHashMap::new_with_no_arg();
                                        let after: CaseInsensitiveHashMap = parse_row(
                                            row,
                                            table_name.as_str(),
                                            &mut columns,
                                            config,
                                            pool,
                                            binlog_columns.as_ref(),
                                        )
                                        .await;
                                        let op = Operation::CREATE(false);
                                        SOURCE_EVENTS_TOTAL
                                            .with_label_values(&[
                                                "mysql",
                                                progress_label.as_str(),
                                                "create",
                                            ])
                                            .inc();
                                        let data_buffer = DataBuffer::new_with_route(
                                            database_name.clone(),
                                            config.target_database.clone(),
                                            table_name.clone(),
                                            before,
                                            after,
                                            op,
                                            binlog_filename.clone(),
                                            timestamp,
                                            next_event_position,
                                        );
                                        runtime_progress::record_read(
                                            &progress_label,
                                            "cdc",
                                            pk_column.as_ref().and_then(|pk| {
                                                pk_value_for_progress(&data_buffer, pk)
                                            }),
                                        )
                                        .await;
                                        let plugin_data =
                                            detail_with_plugin(&plugins, data_buffer).await;
                                        if let Ok(item) = plugin_data {
                                            Self::write_record_with_retry(
                                                &mut sink,
                                                &item,
                                                Some(checkpoint_entity.clone()),
                                            )
                                            .await;
                                            runtime_progress::record_synced(&progress_label).await;
                                        } else {
                                            runtime_progress::record_filtered(&progress_label)
                                                .await;
                                        }
                                    }
                                    if !binlog_filename.is_empty() {
                                        SOURCE_LAG_POSITION
                                            .with_label_values(&["mysql", progress_label.as_str()])
                                            .set(next_event_position as i64);
                                        checkpoint_entity = checkpoint_entity
                                            .update(binlog_filename, next_event_position);
                                        to_modify.insert(
                                            checkpoint_entity.table.clone(),
                                            checkpoint_entity,
                                        );
                                    }
                                }
                                trace!("Checkpoint: {:?}", to_modify);
                                *self.checkpoint_entities.lock().await[source_index]
                                    .lock()
                                    .await = to_modify;
                            }
                            EventData::DeleteRows(event) => {
                                let table_name = table_map
                                    .get(&(i, event.table_id))
                                    .unwrap_or_else(|| {
                                        panic!("Table id {} not found", event.table_id)
                                    })
                                    .clone();
                                let database_name = table_database_map
                                    .get(&(i, event.table_id))
                                    .unwrap_or_else(|| {
                                        panic!("Table id {} not found", event.table_id)
                                    })
                                    .clone();
                                let Some(source_index) = self.source_index_for_event(
                                    i,
                                    database_name.as_str(),
                                    table_name.as_str(),
                                ) else {
                                    continue;
                                };
                                let plugins = self.plugins.clone();
                                let pool: &mut Pool<MySql> = &mut self.pools[source_index];
                                let config: &MysqlSourceConfigDetail =
                                    &mut self.mysql_source[source_index];
                                let table_key = config.source_table_key(table_name.as_str());
                                let progress_label = progress_table_label(
                                    database_name.as_str(),
                                    table_name.as_str(),
                                );
                                let binlog_columns =
                                    table_column_info_map.get(&(i, event.table_id)).cloned();
                                let mut to_modify = self.checkpoint_entities.lock().await
                                    [source_index]
                                    .lock()
                                    .await
                                    .clone();
                                if config.is_target_database_and_table(
                                    database_name.as_str(),
                                    table_name.as_str(),
                                ) {
                                    debug!("DeleteRows: {}.{}", database_name, table_name);
                                    let checkpoint_entity = self.checkpoint_entities.lock().await
                                        [source_index]
                                        .lock()
                                        .await
                                        .get(table_key.to_lowercase().as_str())
                                        .expect("checkpoint_entity not found")
                                        .clone();
                                    let mut checkpoint_entity = checkpoint_entity.clone();

                                    let binlog_filename = self.binlog_filename_list.lock().await[i]
                                        .lock()
                                        .await
                                        .clone();
                                    let timestamp = header.timestamp;
                                    let next_event_position = header.next_event_position;
                                    let pk_column = table_pk_column(config, table_name.as_str());
                                    for row in event.rows {
                                        let before: CaseInsensitiveHashMap = parse_row(
                                            row,
                                            table_name.as_str(),
                                            &mut columns,
                                            config,
                                            pool,
                                            binlog_columns.as_ref(),
                                        )
                                        .await;
                                        let after: CaseInsensitiveHashMap =
                                            CaseInsensitiveHashMap::new_with_no_arg();
                                        let op = Operation::DELETE;
                                        SOURCE_EVENTS_TOTAL
                                            .with_label_values(&[
                                                "mysql",
                                                progress_label.as_str(),
                                                "delete",
                                            ])
                                            .inc();
                                        let data_buffer = DataBuffer::new_with_route(
                                            database_name.clone(),
                                            config.target_database.clone(),
                                            table_name.clone(),
                                            before,
                                            after,
                                            op,
                                            binlog_filename.clone(),
                                            timestamp,
                                            next_event_position,
                                        );
                                        runtime_progress::record_read(
                                            &progress_label,
                                            "cdc",
                                            pk_column.as_ref().and_then(|pk| {
                                                pk_value_for_progress(&data_buffer, pk)
                                            }),
                                        )
                                        .await;
                                        let plugin_data =
                                            detail_with_plugin(&plugins, data_buffer).await;
                                        if let Ok(item) = plugin_data {
                                            Self::write_record_with_retry(
                                                &mut sink,
                                                &item,
                                                Some(checkpoint_entity.clone()),
                                            )
                                            .await;
                                            runtime_progress::record_synced(&progress_label).await;
                                        } else {
                                            runtime_progress::record_filtered(&progress_label)
                                                .await;
                                        }
                                    }
                                    if !binlog_filename.is_empty() {
                                        SOURCE_LAG_POSITION
                                            .with_label_values(&["mysql", progress_label.as_str()])
                                            .set(next_event_position as i64);
                                        checkpoint_entity = checkpoint_entity
                                            .update(binlog_filename, next_event_position);
                                        to_modify.insert(
                                            checkpoint_entity.table.clone(),
                                            checkpoint_entity,
                                        );
                                    }
                                }
                                trace!("Checkpoint: {:?}", to_modify);
                                *self.checkpoint_entities.lock().await[source_index]
                                    .lock()
                                    .await = to_modify;
                            }
                            EventData::UpdateRows(event) => {
                                let table_name = table_map
                                    .get(&(i, event.table_id))
                                    .unwrap_or_else(|| {
                                        panic!("Table id {} not found", event.table_id)
                                    })
                                    .clone();
                                let database_name = table_database_map
                                    .get(&(i, event.table_id))
                                    .unwrap_or_else(|| {
                                        panic!("Table id {} not found", event.table_id)
                                    })
                                    .clone();
                                let Some(source_index) = self.source_index_for_event(
                                    i,
                                    database_name.as_str(),
                                    table_name.as_str(),
                                ) else {
                                    continue;
                                };
                                let plugins = self.plugins.clone();
                                let pool: &mut Pool<MySql> = &mut self.pools[source_index];
                                let config: &MysqlSourceConfigDetail =
                                    &mut self.mysql_source[source_index];
                                let table_key = config.source_table_key(table_name.as_str());
                                let progress_label = progress_table_label(
                                    database_name.as_str(),
                                    table_name.as_str(),
                                );
                                let binlog_columns =
                                    table_column_info_map.get(&(i, event.table_id)).cloned();
                                let mut to_modify = self.checkpoint_entities.lock().await
                                    [source_index]
                                    .lock()
                                    .await
                                    .clone();
                                if config.is_target_database_and_table(
                                    database_name.as_str(),
                                    table_name.as_str(),
                                ) {
                                    debug!("UpdateRows: {}.{}", database_name, table_name);
                                    let checkpoint_entity = self.checkpoint_entities.lock().await
                                        [source_index]
                                        .lock()
                                        .await
                                        .get(table_key.to_lowercase().as_str())
                                        .expect("checkpoint_entity not found")
                                        .clone();
                                    let mut checkpoint_entity = checkpoint_entity.clone();

                                    let binlog_filename = self.binlog_filename_list.lock().await[i]
                                        .lock()
                                        .await
                                        .clone();
                                    let timestamp = header.timestamp;
                                    let next_event_position = header.next_event_position;
                                    let pk_column = table_pk_column(config, table_name.as_str());
                                    for (b, a) in event.rows {
                                        let before: CaseInsensitiveHashMap = parse_row(
                                            b,
                                            table_name.as_str(),
                                            &mut columns,
                                            config,
                                            pool,
                                            binlog_columns.as_ref(),
                                        )
                                        .await;
                                        let after: CaseInsensitiveHashMap = parse_row(
                                            a,
                                            table_name.as_str(),
                                            &mut columns,
                                            config,
                                            pool,
                                            binlog_columns.as_ref(),
                                        )
                                        .await;
                                        let op = Operation::UPDATE;
                                        SOURCE_EVENTS_TOTAL
                                            .with_label_values(&[
                                                "mysql",
                                                progress_label.as_str(),
                                                "update",
                                            ])
                                            .inc();
                                        let data_buffer = DataBuffer::new_with_route(
                                            database_name.clone(),
                                            config.target_database.clone(),
                                            table_name.clone(),
                                            before,
                                            after,
                                            op,
                                            binlog_filename.clone(),
                                            timestamp,
                                            next_event_position,
                                        );
                                        runtime_progress::record_read(
                                            &progress_label,
                                            "cdc",
                                            pk_column.as_ref().and_then(|pk| {
                                                pk_value_for_progress(&data_buffer, pk)
                                            }),
                                        )
                                        .await;
                                        let plugin_data =
                                            detail_with_plugin(&plugins, data_buffer).await;
                                        if let Ok(item) = plugin_data {
                                            Self::write_record_with_retry(
                                                &mut sink,
                                                &item,
                                                Some(checkpoint_entity.clone()),
                                            )
                                            .await;
                                            runtime_progress::record_synced(&progress_label).await;
                                        } else {
                                            runtime_progress::record_filtered(&progress_label)
                                                .await;
                                        }
                                    }

                                    if !binlog_filename.is_empty() {
                                        SOURCE_LAG_POSITION
                                            .with_label_values(&["mysql", progress_label.as_str()])
                                            .set(next_event_position as i64);
                                        checkpoint_entity = checkpoint_entity
                                            .update(binlog_filename, next_event_position);
                                        to_modify.insert(
                                            checkpoint_entity.table.clone(),
                                            checkpoint_entity,
                                        );
                                    }
                                }
                                trace!("Checkpoint: {:?}", to_modify);
                                *self.checkpoint_entities.lock().await[source_index]
                                    .lock()
                                    .await = to_modify;
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        let message = e.to_string();
                        if should_ignore_read_error(&message) {
                            continue;
                        }
                        if should_reconnect_read_error(&message) {
                            let group = self.stream_groups[i].clone();
                            let connection_url = group.connection_url.clone();
                            let server_id = group.server_id;
                            let runtime_binlog_filename = self.binlog_filename_list.lock().await[i]
                                .lock()
                                .await
                                .clone();
                            let runtime_binlog_position =
                                *self.binlog_position_list.lock().await[i].lock().await;
                            let mut checkpoints = HashMap::new();
                            {
                                let checkpoints_guard = self.checkpoint_entities.lock().await;
                                for source_index in &group.source_indices {
                                    checkpoints.extend(
                                        checkpoints_guard[*source_index].lock().await.clone(),
                                    );
                                }
                            }
                            let resume_position = compute_resume_position(
                                if runtime_binlog_filename.is_empty() {
                                    None
                                } else {
                                    Some(runtime_binlog_filename.as_str())
                                },
                                if runtime_binlog_position == 0 {
                                    None
                                } else {
                                    Some(runtime_binlog_position)
                                },
                                &checkpoints,
                            );
                            let start_position = match resume_position {
                                ResumePosition::Latest => StartPosition::Latest,
                                ResumePosition::BinlogPosition(f, p) => {
                                    StartPosition::BinlogPosition(f, p)
                                }
                            };
                            match BinlogClient::new(&connection_url, server_id, start_position)
                                .with_master_heartbeat(Duration::from_secs(5))
                                .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
                                .connect()
                                .await
                            {
                                Ok(new_stream) => {
                                    self.streams[i] = Some(new_stream);
                                    continue;
                                }
                                Err(connect_err) => {
                                    error!(
                                        "遇到错误，重连失败，尝试重新开启source，Error: {}, ConnectError: {}",
                                        message, connect_err
                                    );
                                    return Err(CustomError {
                                        message,
                                        error_type: CustomErrorType::Restart,
                                    });
                                }
                            }
                        }
                        error!(
                            "遇到错误，结束内部循环，尝试重新开启source，Error: {}",
                            message
                        );
                        return Err(CustomError {
                            message,
                            error_type: CustomErrorType::Restart,
                        });
                    }
                }
            }
        }
    }

    async fn add_plugins(&mut self, plugin: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>) {
        self.plugins = plugin;
    }

    async fn get_table_info(&mut self) -> Vec<TableInfoVo> {
        self.mysql_source
            .iter()
            .flat_map(|s| s.table_info_list.clone())
            .collect()
    }

    // async fn alter_flush(&mut self) -> Result<(), String> {
    //     let max = self.streams.len();
    //     for i in 0..max {
    //         let checkpoint_entity: &mut MysqlCheckPointDetailEntity =
    //             &mut self.checkpoint_entities.lock().await[i]
    //                 .lock()
    //                 .await
    //                 .clone();
    //         let mut checkpoint_entity = checkpoint_entity.clone();
    //         match checkpoint_entity.save() {
    //             Ok(_) => {
    //                 *self.checkpoint_entities.lock().await[i].lock().await = checkpoint_entity;
    //             }
    //             Err(message) => {
    //                 error!("持久化失败: {}", message);
    //             }
    //         };
    //     }
    //     Ok(())
    // }
}

async fn parse_row(
    row: RowEvent,
    table_name: &str,
    columns_map: &Mutex<CaseInsensitiveHashMapVecString>,
    config: &MysqlSourceConfigDetail,
    pool: &mut Pool<MySql>,
    binlog_columns: Option<&BinlogTableColumnInfo>,
) -> CaseInsensitiveHashMap {
    let mut data: HashMap<String, Value> = HashMap::new();
    let table_key = config.source_table_key(table_name);
    let row_column_count = row.column_values.len();
    let row_column_names = if let Some(binlog_columns) = binlog_columns {
        if binlog_columns.column_count != row_column_count {
            warn!(
                "MySQL TableMap列数与RowEvent列数不一致 table={} table_map_columns={} row_columns={}",
                table_key, binlog_columns.column_count, row_column_count
            );
        }
        if let Some(row_column_names) = binlog_columns.row_column_names.as_ref() {
            reconcile_row_column_names(
                table_key.as_str(),
                row_column_names.clone(),
                row_column_count,
                "binlog TableMap",
            )
        } else {
            let create_table_columns = create_table_columns_for_table(config, table_name);
            if create_table_columns.len() == row_column_count {
                create_table_columns.into_iter().map(Some).collect()
            } else {
                let current_columns = current_table_columns_for_row(
                    table_name,
                    row_column_count,
                    columns_map,
                    config,
                    pool,
                )
                .await;
                align_current_columns_to_row_columns(
                    table_key.as_str(),
                    current_columns,
                    row_column_count,
                    None,
                    &generated_columns_for_table(config, table_name),
                    "当前表结构",
                )
            }
        }
    } else {
        let create_table_columns = create_table_columns_for_table(config, table_name);
        if create_table_columns.len() == row_column_count {
            create_table_columns.into_iter().map(Some).collect()
        } else {
            let current_columns = current_table_columns_for_row(
                table_name,
                row_column_count,
                columns_map,
                config,
                pool,
            )
            .await;
            align_current_columns_to_row_columns(
                table_key.as_str(),
                current_columns,
                row_column_count,
                None,
                &generated_columns_for_table(config, table_name),
                "当前表结构",
            )
        }
    };

    for (index, column_value) in row.column_values.into_iter().enumerate() {
        // TODO 这里可能存在问题，直接用顺序的索引来获取column_name，会导致字段对不上
        let Some(Some(column_name)) = row_column_names.get(index).cloned() else {
            continue;
        };
        match column_value {
            ColumnValue::None => {
                data.insert(column_name, Value::None);
            }
            ColumnValue::Tiny(v) => {
                data.insert(column_name, Value::Int8(v));
            }
            ColumnValue::Short(v) => {
                let value: Value = Value::Int16(v);
                data.insert(column_name, value);
            }
            ColumnValue::Long(v) => {
                let value: Value = Value::Int32(v);
                data.insert(column_name, value);
            }
            ColumnValue::LongLong(v) => {
                let value: Value = Value::Int64(v);
                data.insert(column_name, value);
            }
            ColumnValue::Float(v) => {
                let value: Value = Value::Float(v);
                data.insert(column_name, value);
            }
            ColumnValue::Double(v) => {
                let value: Value = Value::Double(v);
                data.insert(column_name, value);
            }
            ColumnValue::Decimal(v) => {
                let value: Value = Value::Decimal(v);
                data.insert(column_name, value);
            }
            ColumnValue::Time(v) => {
                let value: Value = Value::Time(v);
                data.insert(column_name, value);
            }
            ColumnValue::Date(v) => {
                let value: Value = Value::Date(v);
                data.insert(column_name, value);
            }
            ColumnValue::DateTime(v) => {
                let value: Value = Value::DateTime(v);
                data.insert(column_name, value);
            }
            ColumnValue::Timestamp(v) => {
                let value: Value = Value::Timestamp(v);
                data.insert(column_name, value);
            }
            ColumnValue::Year(v) => {
                let value: Value = Value::Year(v);
                data.insert(column_name, value);
            }
            ColumnValue::String(v) => {
                let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
                data.insert(column_name, value);
            }
            ColumnValue::Blob(v) => {
                let value: Value = Value::Blob(v);
                data.insert(column_name, value);
            }
            ColumnValue::Bit(v) => {
                let value: Value = Value::Bit(v);
                data.insert(column_name, value);
            }
            // ColumnValue::Set(v) => { data.insert(column_name, Value::Int8(v))}
            // ColumnValue::Enum(v) => { data.insert(column_name, Value::Int8(v))}
            ColumnValue::Json(v) => {
                let value: Value = if v.is_empty() {
                    Value::Json("".to_string())
                } else {
                    Value::Json(
                        JsonBinary::parse_as_string(&v).unwrap_or_else(|_| "{}".to_string()),
                    )
                };
                data.insert(column_name, value);
            }
            _ => {
                row_column_names
                    .iter()
                    .flatten()
                    .for_each(|column_name| error!("column: {}", column_name));
                error!("column_name: {:?}", column_name);
                error!("column_value: {:?}", column_value);
                panic!("unsupported column value type")
            }
        }
    }
    CaseInsensitiveHashMap::new(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_binlog_connector_rust::event::table_map::table_metadata::{
        ColumnMetadata, TableMetadata,
    };

    fn mk_entity(
        is_new: bool,
        last_binlog_filename: &str,
        last_binlog_position: u32,
        table: &str,
    ) -> MysqlCheckPointDetailEntity {
        MysqlCheckPointDetailEntity {
            last_binlog_filename: last_binlog_filename.to_string(),
            last_binlog_position,
            retry_times: 0,
            is_new,
            checkpoint_filepath: "x".to_string(),
            table: table.to_string(),
        }
    }

    fn table_map_event_with_column_names(names: Vec<Option<&str>>) -> TableMapEvent {
        let columns = names
            .iter()
            .map(|name| ColumnMetadata {
                column_name: name.map(str::to_string),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        TableMapEvent {
            table_id: 1,
            database_name: "source_db".to_string(),
            table_name: "orders".to_string(),
            column_types: vec![3; columns.len()],
            column_metas: vec![0; columns.len()],
            null_bits: vec![false; columns.len()],
            table_metadata: Some(TableMetadata {
                default_charset: None,
                enum_and_set_default_charset: None,
                columns,
            }),
        }
    }

    fn table_map_event_with_visibility(visibility: Vec<Option<bool>>) -> TableMapEvent {
        let columns = visibility
            .iter()
            .map(|is_visible| ColumnMetadata {
                is_visible: *is_visible,
                ..Default::default()
            })
            .collect::<Vec<_>>();

        TableMapEvent {
            table_id: 1,
            database_name: "source_db".to_string(),
            table_name: "orders".to_string(),
            column_types: vec![3; columns.len()],
            column_metas: vec![0; columns.len()],
            null_bits: vec![false; columns.len()],
            table_metadata: Some(TableMetadata {
                default_charset: None,
                enum_and_set_default_charset: None,
                columns,
            }),
        }
    }

    #[test]
    fn table_map_column_names_reads_complete_metadata() {
        let event = table_map_event_with_column_names(vec![Some("id"), Some("name")]);

        assert_eq!(
            table_map_column_names(&event),
            Some(vec!["id".to_string(), "name".to_string()])
        );
    }

    #[test]
    fn table_map_column_names_requires_complete_names() {
        let event = table_map_event_with_column_names(vec![Some("id"), None]);

        assert_eq!(table_map_column_names(&event), None);
    }

    #[test]
    fn align_current_columns_truncates_extra_columns() {
        let columns = vec![
            "id".to_string(),
            "name".to_string(),
            "created_at".to_string(),
        ];

        assert_eq!(
            align_current_columns_to_row_columns(
                "source_db.orders",
                columns,
                2,
                None,
                &[],
                "当前表结构"
            ),
            vec![Some("id".to_string()), Some("name".to_string())]
        );
    }

    #[test]
    fn align_current_columns_pads_short_columns() {
        let columns = vec!["id".to_string(), "name".to_string()];

        assert_eq!(
            align_current_columns_to_row_columns(
                "source_db.orders",
                columns,
                3,
                None,
                &[],
                "当前表结构"
            ),
            vec![Some("id".to_string()), Some("name".to_string()), None]
        );
    }

    #[test]
    fn align_current_columns_uses_table_map_visibility() {
        let event = table_map_event_with_visibility(vec![Some(true), Some(false), Some(true)]);
        let columns = vec!["id".to_string(), "settleDate".to_string()];

        assert_eq!(
            align_current_columns_to_row_columns(
                "source_db.orders",
                columns,
                3,
                table_map_column_visibility(&event),
                &[],
                "当前表结构"
            ),
            vec![Some("id".to_string()), None, Some("settleDate".to_string())]
        );
    }

    #[test]
    fn align_current_columns_uses_generated_columns_from_show_create_table() {
        let columns = vec![
            "path".to_string(),
            "HouseId".to_string(),
            "settleDate".to_string(),
        ];

        assert_eq!(
            align_current_columns_to_row_columns(
                "source_db.orders",
                columns,
                4,
                None,
                &[(2, "fullPath".to_string())],
                "当前表结构"
            ),
            vec![
                Some("path".to_string()),
                Some("HouseId".to_string()),
                None,
                Some("settleDate".to_string())
            ]
        );
    }

    #[test]
    fn generated_columns_from_create_table_sql_reads_ordinals() {
        let create_table_sql = r#"CREATE TABLE `charge_customerchargedetail` (
  `path` varchar(100) DEFAULT NULL,
  `HouseId` bigint(20) NOT NULL,
  `fullPath` varchar(120) GENERATED ALWAYS AS (concat(`path`,`HouseId`,_utf8mb3'/')) STORED,
  `settleDate` date DEFAULT NULL,
  PRIMARY KEY (`HouseId`)
)"#;

        assert_eq!(
            generated_columns_from_create_table_sql(create_table_sql),
            vec![(2, "fullPath".to_string())]
        );
    }

    #[test]
    fn column_names_from_create_table_sql_keeps_generated_columns() {
        let create_table_sql = r#"CREATE TABLE `charge_customerchargedetail` (
  `path` varchar(100) DEFAULT NULL,
  `HouseId` bigint(20) NOT NULL,
  `fullPath` varchar(120) GENERATED ALWAYS AS (concat(`path`,`HouseId`,_utf8mb3'/')) STORED,
  `bankCollectionLock` int(11) DEFAULT '0',
  `checkTime` datetime DEFAULT NULL,
  PRIMARY KEY (`HouseId`)
)"#;

        assert_eq!(
            column_names_from_create_table_sql(create_table_sql),
            vec![
                "path".to_string(),
                "HouseId".to_string(),
                "fullPath".to_string(),
                "bankCollectionLock".to_string(),
                "checkTime".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn parse_row_uses_binlog_columns_without_schema_lookup() {
        let config = MysqlSourceConfigDetail {
            username: "".to_string(),
            password: "".to_string(),
            host: "".to_string(),
            port: "".to_string(),
            database: "source_db".to_string(),
            target_database: "target_db".to_string(),
            server_id: 1,
            connection_url: "".to_string(),
            table_info_list: vec![],
            batchsize: 100,
            start_binlog_filename: None,
            start_binlog_position: None,
        };
        let mut pool =
            sqlx::Pool::<sqlx::MySql>::connect_lazy("mysql://root:password@localhost/source_db")
                .unwrap();
        let columns_map = Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg());
        let binlog_columns = BinlogTableColumnInfo {
            database_name: "source_db".to_string(),
            table_name: "orders".to_string(),
            column_count: 3,
            row_column_names: Some(vec![
                Some("id".to_string()),
                Some("name".to_string()),
                Some("created_at".to_string()),
            ]),
        };
        let row = RowEvent {
            column_values: vec![
                ColumnValue::Long(7),
                ColumnValue::String(b"order-7".to_vec()),
            ],
        };

        let parsed = parse_row(
            row,
            "orders",
            &columns_map,
            &config,
            &mut pool,
            Some(&binlog_columns),
        )
        .await;

        assert_eq!(parsed.len(), 2);
        assert!(matches!(parsed.get("id"), &Value::Int32(7)));
        match parsed.get("name") {
            Value::String(name) => assert_eq!(name, "order-7"),
            other => panic!("unexpected name value: {:?}", other),
        }
        assert!(parsed.get("created_at").is_none());
    }

    #[tokio::test]
    async fn parse_row_skips_hidden_binlog_column_without_shifting_later_columns() {
        let config = MysqlSourceConfigDetail {
            username: "".to_string(),
            password: "".to_string(),
            host: "".to_string(),
            port: "".to_string(),
            database: "source_db".to_string(),
            target_database: "target_db".to_string(),
            server_id: 1,
            connection_url: "".to_string(),
            table_info_list: vec![],
            batchsize: 100,
            start_binlog_filename: None,
            start_binlog_position: None,
        };
        let mut pool =
            sqlx::Pool::<sqlx::MySql>::connect_lazy("mysql://root:password@localhost/source_db")
                .unwrap();
        let columns_map = Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg());
        let binlog_columns = BinlogTableColumnInfo {
            database_name: "source_db".to_string(),
            table_name: "orders".to_string(),
            column_count: 3,
            row_column_names: Some(vec![
                Some("id".to_string()),
                None,
                Some("settleDate".to_string()),
            ]),
        };
        let row = RowEvent {
            column_values: vec![
                ColumnValue::Long(7),
                ColumnValue::String(b"".to_vec()),
                ColumnValue::Date("2026-06-16".to_string()),
            ],
        };

        let parsed = parse_row(
            row,
            "orders",
            &columns_map,
            &config,
            &mut pool,
            Some(&binlog_columns),
        )
        .await;

        assert_eq!(parsed.len(), 2);
        assert!(matches!(parsed.get("id"), &Value::Int32(7)));
        match parsed.get("settleDate") {
            Value::Date(date) => assert_eq!(date, "2026-06-16"),
            other => panic!("unexpected settleDate value: {:?}", other),
        }
    }

    #[test]
    fn compute_resume_position_prefers_runtime() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert(
            "t".to_string(),
            mk_entity(false, "mysql-bin.000010", 120, "t"),
        );
        let rp = compute_resume_position(Some("mysql-bin.000020"), Some(456), &checkpoints);
        assert_eq!(
            rp,
            ResumePosition::BinlogPosition("mysql-bin.000020".to_string(), 456)
        );
    }

    #[test]
    fn compute_resume_position_latest_when_all_new() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert("a".to_string(), mk_entity(true, "", 0, "a"));
        checkpoints.insert("b".to_string(), mk_entity(true, "", 0, "b"));
        let rp = compute_resume_position(None, None, &checkpoints);
        assert_eq!(rp, ResumePosition::Latest);
    }

    #[test]
    fn compute_resume_position_uses_max_checkpoint() {
        let mut checkpoints = HashMap::new();
        checkpoints.insert(
            "a".to_string(),
            mk_entity(false, "mysql-bin.000010", 120, "a"),
        );
        checkpoints.insert(
            "b".to_string(),
            mk_entity(false, "mysql-bin.000011", 4, "b"),
        );
        let rp = compute_resume_position(None, None, &checkpoints);
        assert_eq!(
            rp,
            ResumePosition::BinlogPosition("mysql-bin.000011".to_string(), 4)
        );
    }

    #[test]
    fn timeout_errors_are_ignored() {
        assert!(should_ignore_read_error(
            "unexpected binlog data: Read binlog header timeout after 60s while waiting for packet header"
        ));
        assert!(should_ignore_read_error("Timed out"));
    }

    #[test]
    fn eof_errors_trigger_reconnect() {
        assert!(should_reconnect_read_error(
            "io error: unexpected end of file"
        ));
        assert!(should_reconnect_read_error("connection reset by peer"));
        assert!(should_reconnect_read_error("Broken pipe"));
    }

    #[test]
    fn mysql_identifier_is_quoted() {
        assert_eq!(quote_mysql_identifier("newsee-system"), "`newsee-system`");
        assert_eq!(quote_mysql_identifier("a`b"), "`a``b`");
    }

    #[test]
    fn mysql_table_name_is_qualified() {
        assert_eq!(
            qualified_mysql_table_name("newsee-system", "ns_core_role_user"),
            "`newsee-system`.`ns_core_role_user`"
        );
    }

    #[test]
    fn init_pk_cursor_supports_unsigned_bigint() {
        let cursor = InitPkCursor::from_value(&Value::UnsignedInt64(42)).unwrap();

        assert_eq!(cursor, InitPkCursor::Unsigned(42));
        assert_eq!(cursor.sql_literal().as_deref(), Some("42"));
    }

    #[test]
    fn init_pk_cursor_advances_forward_only() {
        let mut cursor = InitPkCursor::Start;

        cursor.advance(InitPkCursor::Unsigned(10));
        cursor.advance(InitPkCursor::Unsigned(8));
        cursor.advance(InitPkCursor::Unsigned(12));

        assert_eq!(cursor, InitPkCursor::Unsigned(12));
    }
}
