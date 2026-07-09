extern crate core;

use async_trait::async_trait;
use common::case_insensitive_hash_map::{CaseInsensitiveHashMap, CaseInsensitiveHashMapVecString};
use common::checkpoint_manager::{CheckpointManager, checkpoint_manager_from_config};
use common::custom_error::{CustomError, CustomErrorType};
use common::metrics::{SOURCE_EVENTS_TOTAL, SOURCE_LAG_POSITION};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::runtime_progress;
use common::{
    CdcConfig, DataBuffer, FlushByOperation, ForeignKeyInfo, Operation, Plugin, Sink, Source,
    TableIndexInfo, TableInfoVo, Value, database_table_key,
    get_mysql_pool_by_url_with_max_connections, mysql_connection_url_from_config,
    mysql_row_text_value, mysql_row_to_hashmap, redact_connection_url_password,
};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::column::column_type::ColumnType;
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
use std::collections::{HashMap, HashSet};
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

#[derive(Debug, Clone)]
struct MysqlIndexBuilder {
    table_name: String,
    index_name: String,
    columns: Vec<String>,
    unique: bool,
    skip_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MysqlBinlogTypeCategory {
    Tiny,
    Short,
    Int24,
    Long,
    LongLong,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Timestamp,
    DateTime,
    Year,
    String,
    Blob,
    Json,
    Bit,
    Unknown,
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
    random_check_data_after_init: bool,
    random_check_data_after_init_batch_size_min: usize,
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

        for (i, source_database) in source_databases
            .iter()
            .enumerate()
            .take(logical_source_size)
        {
            let config_index = if config.multi_mode_open() { 0 } else { i };
            let username = config.source("username", config_index);
            let password = config.source("password", config_index);
            let host = config.source("host", config_index);
            let port = config.source("port", config_index);
            let database = if config.multi_mode_open() {
                source_database.clone()
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
            let sync_foreign_key_tables = config.sync_foreign_key_tables_enabled();
            let sync_schema_only_tables = config.sync_no_pk_table_schema_enabled();

            let full_table_discovery = current_source_tables.is_empty()
                || (current_source_tables.len() == 1
                    && (current_source_tables[0].eq_ignore_ascii_case("all")
                        || current_source_tables[0].eq_ignore_ascii_case("*")));
            if full_table_discovery {
                // get all tables
                let show_tables_sql = Self::full_table_discovery_sql(
                    sync_foreign_key_tables,
                    sync_schema_only_tables,
                );
                let tables: Vec<String> = sqlx::query(show_tables_sql.as_str())
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
                if let Some(re) = &exclude_regex
                    && re.is_match(table_name)
                {
                    info!("Exclude table by regex: {}", table_name);
                    return false;
                }
                if let Some(re) = &include_regex
                    && !re.is_match(table_name)
                {
                    info!("Skip table not matching include regex: {}", table_name);
                    return false;
                }
                true
            });

            let selected_table_keys = current_source_tables
                .iter()
                .map(|table_name| table_name.to_ascii_lowercase())
                .collect::<HashSet<_>>();
            let table_comments =
                Self::fetch_table_comments(&pool, database.as_str(), &selected_table_keys).await;
            let secondary_indexes =
                Self::fetch_secondary_indexes(&pool, database.as_str(), &selected_table_keys).await;

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

                let Some(pk_column) = Self::table_cdc_pk_column(
                    table_name.as_str(),
                    &col_list,
                    sync_schema_only_tables,
                ) else {
                    continue;
                };
                let columns: Vec<String> = col_list.iter().map(|c| c.column_name.clone()).collect();

                table_info_list.push(TableInfoVo {
                    source_database: database.clone(),
                    target_database: target_database.clone(),
                    table_name: table_name.clone(),
                    pk_column,
                    create_table_sql,
                    columns,
                    table_comment: table_comments
                        .get(table_name.to_ascii_lowercase().as_str())
                        .cloned()
                        .unwrap_or_default(),
                    indexes: secondary_indexes
                        .get(table_name.to_ascii_lowercase().as_str())
                        .cloned()
                        .unwrap_or_default(),
                    foreign_keys: Vec::new(),
                });
            }
            if sync_foreign_key_tables {
                let selected_tables = table_info_list
                    .iter()
                    .map(|table_info| table_info.table_name.to_ascii_lowercase())
                    .collect::<HashSet<_>>();
                let foreign_keys =
                    Self::fetch_foreign_keys(&pool, database.as_str(), &selected_tables).await;
                for table_info in &mut table_info_list {
                    table_info.foreign_keys = foreign_keys
                        .get(table_info.table_name.to_ascii_lowercase().as_str())
                        .cloned()
                        .unwrap_or_default();
                }
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
                random_check_data_after_init: config.random_check_data_after_init_enabled(),
                random_check_data_after_init_batch_size_min: config
                    .random_check_data_after_init_batch_size_min(),
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

    fn full_table_discovery_sql(
        sync_foreign_key_tables: bool,
        sync_schema_only_tables: bool,
    ) -> String {
        if sync_schema_only_tables {
            let mut sql = r#"
            SELECT DISTINCT t.TABLE_NAME AS table_name
            FROM information_schema.TABLES t
            WHERE t.TABLE_SCHEMA = (SELECT DATABASE())
              AND t.TABLE_TYPE = 'BASE TABLE'
        "#
            .to_string();
            if !sync_foreign_key_tables {
                Self::push_foreign_key_table_exclusion(&mut sql, "t.TABLE_NAME");
            }
            sql.push_str(
                r#"
            ORDER BY t.TABLE_NAME
        "#,
            );
            return sql;
        }

        let mut sql = r#"
            SELECT DISTINCT c.TABLE_NAME AS table_name
            FROM information_schema.COLUMNS c
            JOIN information_schema.TABLES t
              ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
             AND t.TABLE_NAME = c.TABLE_NAME
            WHERE c.TABLE_SCHEMA = (SELECT DATABASE())
              AND t.TABLE_TYPE = 'BASE TABLE'
              AND c.COLUMN_KEY = 'PRI'
              AND c.DATA_TYPE IN ('tinyint', 'smallint', 'mediumint', 'int', 'bigint')
        "#
        .to_string();
        if !sync_foreign_key_tables {
            Self::push_foreign_key_table_exclusion(&mut sql, "c.TABLE_NAME");
        }
        sql.push_str(
            r#"
              AND c.TABLE_NAME NOT IN (
                    SELECT cc.TABLE_NAME AS table_name
                    FROM information_schema.COLUMNS cc
                    WHERE cc.TABLE_SCHEMA = (SELECT DATABASE())
                      AND cc.COLUMN_KEY = 'PRI'
                    GROUP BY cc.TABLE_NAME
                    HAVING COUNT(*) > 1
              )
            ORDER BY c.TABLE_NAME
        "#,
        );
        sql
    }

    fn push_foreign_key_table_exclusion(sql: &mut String, table_expression: &str) {
        sql.push_str(&format!(
            r#"
              AND {table_expression} NOT IN (
                    SELECT TABLE_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = (SELECT DATABASE())
                      AND REFERENCED_TABLE_NAME IS NOT NULL
               )
              AND {table_expression} NOT IN (
                    SELECT REFERENCED_TABLE_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = (SELECT DATABASE())
                      AND REFERENCED_TABLE_NAME IS NOT NULL
               )
            "#
        ));
    }

    fn table_cdc_pk_column(
        table_name: &str,
        col_list: &[ColumnInfoFromMysql],
        sync_schema_only_tables: bool,
    ) -> Option<String> {
        let primary_key_columns: Vec<&ColumnInfoFromMysql> = col_list
            .iter()
            .filter(|c| c.column_key.eq_ignore_ascii_case("PRI"))
            .collect();
        let supported_pk_columns: Vec<String> = primary_key_columns
            .iter()
            .filter(|c| is_supported_mysql_pk_data_type(c.data_type.as_str()))
            .map(|c| c.column_name.clone())
            .collect();
        if primary_key_columns.len() == 1 && supported_pk_columns.len() == 1 {
            return supported_pk_columns.first().cloned();
        }

        if sync_schema_only_tables {
            info!(
                "table {} is schema-only because primary key is missing, unsupported, or composite",
                table_name
            );
            return Some(String::new());
        }

        error!(
            "pk_column is empty, unsupported, or more than one for table {}",
            table_name
        );
        None
    }

    async fn fetch_table_comments(
        pool: &Pool<MySql>,
        database: &str,
        selected_tables: &HashSet<String>,
    ) -> HashMap<String, String> {
        if selected_tables.is_empty() {
            return HashMap::new();
        }
        let sql = r#"
            SELECT
              TABLE_NAME AS table_name,
              COALESCE(TABLE_COMMENT, '') AS table_comment
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = ?
              AND TABLE_TYPE = 'BASE TABLE'
        "#;
        let rows = sqlx::query(sql)
            .bind(database)
            .fetch_all(pool)
            .await
            .expect("query failed");

        rows.into_iter()
            .filter_map(|row| {
                let table_name = mysql_row_text_value(&row, "table_name");
                let table_key = table_name.to_ascii_lowercase();
                if !selected_tables.contains(table_key.as_str()) {
                    return None;
                }
                Some((table_key, mysql_row_text_value(&row, "table_comment")))
            })
            .collect()
    }

    async fn fetch_secondary_indexes(
        pool: &Pool<MySql>,
        database: &str,
        selected_tables: &HashSet<String>,
    ) -> HashMap<String, Vec<TableIndexInfo>> {
        if selected_tables.is_empty() {
            return HashMap::new();
        }
        let sql = r#"
            SELECT
              TABLE_NAME AS table_name,
              INDEX_NAME AS index_name,
              CAST(NON_UNIQUE AS CHAR) AS non_unique,
              CAST(SEQ_IN_INDEX AS CHAR) AS seq_in_index,
              COALESCE(COLUMN_NAME, '') AS column_name,
              COALESCE(CAST(SUB_PART AS CHAR), '') AS sub_part,
              COALESCE(INDEX_TYPE, '') AS index_type
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = ?
              AND INDEX_NAME <> 'PRIMARY'
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        "#;
        let rows = sqlx::query(sql)
            .bind(database)
            .fetch_all(pool)
            .await
            .expect("query failed");

        let mut grouped: HashMap<(String, String), MysqlIndexBuilder> = HashMap::new();
        for row in rows {
            let table_name = mysql_row_text_value(&row, "table_name");
            let table_key = table_name.to_ascii_lowercase();
            if !selected_tables.contains(table_key.as_str()) {
                continue;
            }

            let index_name = mysql_row_text_value(&row, "index_name");
            let index_key = index_name.to_ascii_lowercase();
            let non_unique = mysql_row_text_value(&row, "non_unique");
            let column_name = mysql_row_text_value(&row, "column_name");
            let sub_part = mysql_row_text_value(&row, "sub_part");
            let index_type = mysql_row_text_value(&row, "index_type");
            let unique = non_unique.trim() == "0";
            let entry =
                grouped
                    .entry((table_key, index_key))
                    .or_insert_with(|| MysqlIndexBuilder {
                        table_name: table_name.clone(),
                        index_name: index_name.clone(),
                        columns: Vec::new(),
                        unique,
                        skip_reason: None,
                    });

            if !index_type.eq_ignore_ascii_case("BTREE") {
                entry.skip_reason = Some(format!("unsupported index_type={}", index_type));
                continue;
            }
            if column_name.trim().is_empty() {
                entry.skip_reason = Some("expression index has no COLUMN_NAME".to_string());
                continue;
            }
            if !sub_part.trim().is_empty() {
                entry.skip_reason = Some(format!("prefix index sub_part={}", sub_part));
                continue;
            }
            entry.columns.push(column_name);
        }

        let mut by_table: HashMap<String, Vec<TableIndexInfo>> = HashMap::new();
        for ((table_key, _), index) in grouped {
            if let Some(reason) = index.skip_reason {
                warn!(
                    "Skip unsupported MySQL secondary index: {}.{} {}",
                    index.table_name, index.index_name, reason
                );
                continue;
            }
            if index.columns.is_empty() {
                warn!(
                    "Skip MySQL secondary index without columns: {}.{}",
                    index.table_name, index.index_name
                );
                continue;
            }
            by_table.entry(table_key).or_default().push(TableIndexInfo {
                index_name: index.index_name,
                table_name: index.table_name,
                columns: index.columns,
                unique: index.unique,
            });
        }
        for indexes in by_table.values_mut() {
            indexes.sort_by(|a, b| a.index_name.cmp(&b.index_name));
        }
        by_table
    }

    async fn fetch_foreign_keys(
        pool: &Pool<MySql>,
        database: &str,
        selected_tables: &HashSet<String>,
    ) -> HashMap<String, Vec<ForeignKeyInfo>> {
        if selected_tables.is_empty() {
            return HashMap::new();
        }
        let sql = r#"
            SELECT
              kcu.CONSTRAINT_NAME AS constraint_name,
              kcu.TABLE_NAME AS table_name,
              kcu.COLUMN_NAME AS column_name,
              kcu.REFERENCED_TABLE_NAME AS referenced_table_name,
              kcu.REFERENCED_COLUMN_NAME AS referenced_column_name,
              rc.UPDATE_RULE AS update_rule,
              rc.DELETE_RULE AS delete_rule
            FROM information_schema.KEY_COLUMN_USAGE kcu
            LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
              ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
             AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND rc.TABLE_NAME = kcu.TABLE_NAME
            WHERE kcu.TABLE_SCHEMA = ?
              AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        "#;
        let rows = sqlx::query(sql)
            .bind(database)
            .fetch_all(pool)
            .await
            .expect("query failed");
        let mut grouped: HashMap<(String, String), ForeignKeyInfo> = HashMap::new();
        let mut skipped = HashSet::new();
        for row in rows {
            let table_name = mysql_row_text_value(&row, "table_name");
            let referenced_table_name = mysql_row_text_value(&row, "referenced_table_name");
            let table_key = table_name.to_ascii_lowercase();
            let referenced_key = referenced_table_name.to_ascii_lowercase();
            if !selected_tables.contains(table_key.as_str())
                || !selected_tables.contains(referenced_key.as_str())
            {
                let constraint_name = mysql_row_text_value(&row, "constraint_name");
                if skipped.insert(format!("{}.{}", table_name, constraint_name)) {
                    warn!(
                        "Skip foreign key because child or referenced table is not selected: {}.{} -> {}",
                        table_name, constraint_name, referenced_table_name
                    );
                }
                continue;
            }
            let constraint_name = mysql_row_text_value(&row, "constraint_name");
            let key = (table_key, constraint_name.to_ascii_lowercase());
            let entry = grouped.entry(key).or_insert_with(|| ForeignKeyInfo {
                constraint_name: constraint_name.clone(),
                table_name: table_name.clone(),
                columns: Vec::new(),
                referenced_table_name: referenced_table_name.clone(),
                referenced_columns: Vec::new(),
                update_rule: Some(mysql_row_text_value(&row, "update_rule")),
                delete_rule: Some(mysql_row_text_value(&row, "delete_rule")),
            });
            entry
                .columns
                .push(mysql_row_text_value(&row, "column_name"));
            entry
                .referenced_columns
                .push(mysql_row_text_value(&row, "referenced_column_name"));
        }

        let mut by_table: HashMap<String, Vec<ForeignKeyInfo>> = HashMap::new();
        for (_, foreign_key) in grouped {
            by_table
                .entry(foreign_key.table_name.to_ascii_lowercase())
                .or_default()
                .push(foreign_key);
        }
        for foreign_keys in by_table.values_mut() {
            foreign_keys.sort_by(|a, b| a.constraint_name.cmp(&b.constraint_name));
        }
        by_table
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

fn is_supported_mysql_pk_data_type(data_type: &str) -> bool {
    matches!(
        data_type.to_ascii_lowercase().as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    )
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
    fn is_cdc_table(table_info: &TableInfoVo) -> bool {
        !table_info.pk_column.trim().is_empty()
    }

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
            if !Self::is_cdc_table(table_info) {
                continue;
            }
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
        limit: usize,
        random_sample: bool,
        executor: &mut Transaction<'_, MySql>,
    ) -> Vec<DataBuffer> {
        let limit = limit.max(1);
        let table_ref = qualified_mysql_table_name(&self.database, table_name);
        let pk_ident = quote_mysql_identifier(pk_column);
        let mut random_start = None;
        let sql = if random_sample {
            let start = self
                .random_init_pk_start(table_name, pk_column, executor)
                .await
                .unwrap_or_else(|| "0".to_string());
            random_start = Some(start.clone());
            format!(
                r#"
                    select *
                    FROM {}
                    where {} >= {}
                    order by {}
                    limit {}
                "#,
                table_ref, pk_ident, start, pk_ident, limit
            )
        } else {
            let where_clause = cursor
                .sql_literal()
                .map(|id| format!("where {} > {}", pk_ident, id))
                .unwrap_or_default();
            format!(
                r#"
                    select *
                    FROM {}
                    {}
                    order by {}
                    limit {}
                "#,
                table_ref, where_clause, pk_ident, limit
            )
        };
        debug!(
            "extract_init_data: [{}.{}] {} {} random_sample={}",
            self.database,
            table_name,
            pk_column,
            cursor.display_value(),
            random_sample
        );
        // 查询 Row，而不是 HashMap
        let mut rows: Vec<MySqlRow> = sqlx::query(&sql)
            .fetch_all(&mut **executor)
            .await
            .expect("query failed");
        if random_sample
            && rows.len() < limit
            && let Some(start) = random_start.as_deref()
        {
            let wrap_sql = format!(
                r#"
                        select *
                        FROM {}
                        where {} < {}
                        order by {}
                        limit {}
                    "#,
                table_ref,
                pk_ident,
                start,
                pk_ident,
                limit.saturating_sub(rows.len())
            );
            let wrap_rows: Vec<MySqlRow> = sqlx::query(&wrap_sql)
                .fetch_all(&mut **executor)
                .await
                .expect("query failed");
            rows.extend(wrap_rows);
        }
        info!(
            "extract_init_data: [{}.{}] {} {} {} rows random_sample={}",
            self.database,
            table_name,
            pk_column,
            cursor.display_value(),
            rows.len(),
            random_sample
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

    async fn random_init_pk_start(
        &self,
        table_name: &str,
        pk_column: &str,
        executor: &mut Transaction<'_, MySql>,
    ) -> Option<String> {
        let table_ref = qualified_mysql_table_name(&self.database, table_name);
        let pk_ident = quote_mysql_identifier(pk_column);
        let sql = format!(
            r#"
                select CAST(
                    FLOOR(
                        COALESCE(MIN({pk}), 0)
                        + RAND() * GREATEST(COALESCE(MAX({pk}), 0) - COALESCE(MIN({pk}), 0), 0)
                    ) AS CHAR
                ) AS random_pk
                FROM {table_ref}
            "#,
            pk = pk_ident,
            table_ref = table_ref
        );
        sqlx::query(&sql)
            .fetch_optional(&mut **executor)
            .await
            .ok()
            .flatten()
            .and_then(|row| row.try_get::<Option<String>, _>("random_pk").ok().flatten())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
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
        let checkpoint_manager = checkpoint_manager_from_config(config).await;
        for i in 0..size {
            let connection_url = cfg.mysql_source[i].connection_url.clone();
            let tables: Vec<String> = cfg.mysql_source[i]
                .table_info_list
                .clone()
                .iter()
                .filter(|t| !t.pk_column.trim().is_empty())
                .map(|t| database_table_key(t.source_database.as_str(), t.table_name.as_str()))
                .collect::<Vec<String>>();
            let mut mysql_checkpoint_detail_entity_map = HashMap::new();
            for table in tables {
                let mysql_checkpoint_detail_entity =
                    if config.random_check_data_after_init_enabled() {
                        Self::random_check_checkpoint_entity(table.clone())
                    } else {
                        MysqlCheckPointDetailEntity::from_config_with_manager(
                            config
                                .checkpoint_file_path
                                .clone()
                                .unwrap_or("/checkpoint".to_string()),
                            &connection_url,
                            table.clone(),
                            checkpoint_manager.as_ref(),
                        )
                        .await
                    };
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

    fn random_check_checkpoint_entity(table: String) -> MysqlCheckPointDetailEntity {
        MysqlCheckPointDetailEntity {
            last_binlog_filename: String::new(),
            last_binlog_position: 0,
            retry_times: 0,
            is_new: true,
            checkpoint_filepath: format!("memory://random-check/{}", table),
            table,
        }
    }

    async fn save_pending_checkpoints(
        &self,
        checkpoints: &mut HashMap<String, MysqlCheckPointDetailEntity>,
    ) {
        if checkpoints.is_empty() {
            return;
        }
        let entries = checkpoints
            .iter()
            .map(|(key, cp)| (key.clone(), cp.clone()))
            .collect::<Vec<_>>();
        match self.checkpoint_manager.save_many(&entries).await {
            Ok(_) => {
                trace!("saved pending filtered checkpoints: {}", entries.len());
                checkpoints.clear();
            }
            Err(e) => {
                error!("Failed to save pending filtered checkpoints: {}", e);
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
            if !MysqlSourceConfigDetail::is_cdc_table(table_info) {
                return false;
            }
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
            if !MysqlSourceConfigDetail::is_cdc_table(table_info) {
                continue;
            }
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
            if !MysqlSourceConfigDetail::is_cdc_table(&table_info_vo) {
                continue;
            }
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
            let validation_init_limit = config
                .random_check_data_after_init
                .then_some(config.random_check_data_after_init_batch_size_min);
            if let Some(limit) = validation_init_limit {
                info!(
                    "MySQL数据源初始化验证模式已开启，仅同步样本数据: {}.{} limit={}",
                    config.database, table_name, limit
                );
            }
            loop {
                let extract_limit = validation_init_limit.unwrap_or(config.batchsize);
                let data_buffer_list: Vec<DataBuffer> = config
                    .extract_init_data(
                        &table_name,
                        &pk_column,
                        &cursor,
                        extract_limit,
                        validation_init_limit.is_some(),
                        &mut tx,
                    )
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
                if validation_init_limit.is_some() {
                    break;
                }
                if len != config.batchsize {
                    break;
                }
            }
            if validation_init_limit.is_none() {
                sink.lock()
                    .await
                    .flush_with_retry(&FlushByOperation::Init)
                    .await;
            }
            info!(
                "MySQL数据源初始化完成 {}.{} count: {} cost: {:?}",
                redacted_connection_url,
                table_name,
                count,
                start.elapsed()
            );
            runtime_progress::finish_table_initialization(&progress_label).await;
            if let Some(cp) = checkpoints.get_mut(table_key.to_lowercase().as_str()) {
                if validation_init_limit.is_some() {
                    info!(
                        "MySQL数据源初始化验证模式跳过checkpoint完成标记: {}.{}",
                        config.database, table_name
                    );
                    continue;
                }
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

    let missing_generated_columns =
        generated_columns_missing_from_columns(generated_columns, &columns);
    if columns.len() + missing_generated_columns.len() == row_column_count
        && missing_generated_columns
            .iter()
            .all(|(ordinal, _)| *ordinal < row_column_count)
    {
        warn!(
            "MySQL binlog row列数与{}列数不一致 table={} row_columns={} {}_columns={}; \
             根据当前列集合中缺失的SHOW CREATE TABLE生成列位置跳过: {:?}",
            column_source,
            table_label,
            row_column_count,
            column_source,
            columns.len(),
            missing_generated_columns
        );
        let mut columns = columns.into_iter();
        return (0..row_column_count)
            .map(|ordinal| {
                if missing_generated_columns
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

    if !generated_columns.is_empty() {
        warn!(
            "MySQL binlog列对齐诊断 table={} row_columns={} {}_columns={} \
             generated_columns={:?} missing_generated_columns={:?} focus_positions={:?}",
            table_label,
            row_column_count,
            column_source,
            columns.len(),
            generated_columns,
            missing_generated_columns,
            focus_column_positions(&columns)
        );
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

fn generated_columns_missing_from_columns(
    generated_columns: &[(usize, String)],
    columns: &[String],
) -> Vec<(usize, String)> {
    generated_columns
        .iter()
        .filter(|(_, generated_column)| {
            !columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(generated_column))
        })
        .cloned()
        .collect()
}

fn focus_column_positions(columns: &[String]) -> Vec<(&'static str, Option<usize>)> {
    const FOCUS_COLUMNS: &[&str] = &[
        "path",
        "HouseId",
        "fullPath",
        "bankCollectionLock",
        "checkTime",
        "CreateTime",
        "settleDate",
        "ShouldChargeDate",
        "uuid",
    ];

    FOCUS_COLUMNS
        .iter()
        .map(|focus_column| {
            (
                *focus_column,
                columns
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(focus_column)),
            )
        })
        .collect()
}

fn binlog_type_category(column_type: u8) -> MysqlBinlogTypeCategory {
    match ColumnType::from_code(column_type) {
        ColumnType::Tiny => MysqlBinlogTypeCategory::Tiny,
        ColumnType::Short => MysqlBinlogTypeCategory::Short,
        ColumnType::Int24 => MysqlBinlogTypeCategory::Int24,
        ColumnType::Long => MysqlBinlogTypeCategory::Long,
        ColumnType::LongLong => MysqlBinlogTypeCategory::LongLong,
        ColumnType::Float => MysqlBinlogTypeCategory::Float,
        ColumnType::Double => MysqlBinlogTypeCategory::Double,
        ColumnType::Decimal | ColumnType::NewDecimal => MysqlBinlogTypeCategory::Decimal,
        ColumnType::Date => MysqlBinlogTypeCategory::Date,
        ColumnType::Time | ColumnType::Time2 => MysqlBinlogTypeCategory::Time,
        ColumnType::TimeStamp | ColumnType::TimeStamp2 => MysqlBinlogTypeCategory::Timestamp,
        ColumnType::DateTime | ColumnType::DateTime2 => MysqlBinlogTypeCategory::DateTime,
        ColumnType::Year => MysqlBinlogTypeCategory::Year,
        ColumnType::VarChar | ColumnType::VarString | ColumnType::String => {
            MysqlBinlogTypeCategory::String
        }
        ColumnType::Blob
        | ColumnType::TinyBlob
        | ColumnType::MediumBlob
        | ColumnType::LongBlob
        | ColumnType::Geometry => MysqlBinlogTypeCategory::Blob,
        ColumnType::Json => MysqlBinlogTypeCategory::Json,
        ColumnType::Bit => MysqlBinlogTypeCategory::Bit,
        _ => MysqlBinlogTypeCategory::Unknown,
    }
}

fn create_table_column_type_category(column_definition: &str) -> MysqlBinlogTypeCategory {
    let line = column_definition.trim();
    let Some(second_tick) = line[1..].find('`').map(|i| i + 1) else {
        return MysqlBinlogTypeCategory::Unknown;
    };
    let type_definition = line[(second_tick + 1)..].trim_start();
    let type_name = type_definition
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .trim_matches('`')
        .to_ascii_lowercase();

    match type_name.as_str() {
        "bool" | "boolean" | "tinyint" => MysqlBinlogTypeCategory::Tiny,
        "smallint" => MysqlBinlogTypeCategory::Short,
        "mediumint" => MysqlBinlogTypeCategory::Int24,
        "int" | "integer" => MysqlBinlogTypeCategory::Long,
        "bigint" | "serial" => MysqlBinlogTypeCategory::LongLong,
        "float" => MysqlBinlogTypeCategory::Float,
        "double" | "real" => MysqlBinlogTypeCategory::Double,
        "decimal" | "dec" | "fixed" | "numeric" => MysqlBinlogTypeCategory::Decimal,
        "date" => MysqlBinlogTypeCategory::Date,
        "time" => MysqlBinlogTypeCategory::Time,
        "timestamp" => MysqlBinlogTypeCategory::Timestamp,
        "datetime" => MysqlBinlogTypeCategory::DateTime,
        "year" => MysqlBinlogTypeCategory::Year,
        "char" | "varchar" | "binary" | "varbinary" | "enum" | "set" => {
            MysqlBinlogTypeCategory::String
        }
        "json" => MysqlBinlogTypeCategory::Json,
        name if name.ends_with("text") || name.ends_with("blob") => MysqlBinlogTypeCategory::Blob,
        "bit" => MysqlBinlogTypeCategory::Bit,
        _ => MysqlBinlogTypeCategory::Unknown,
    }
}

fn infer_single_hidden_column_position_by_types(
    visible_type_categories: &[MysqlBinlogTypeCategory],
    row_column_types: &[u8],
) -> Option<usize> {
    if row_column_types.len() != visible_type_categories.len() + 1 {
        return None;
    }

    let mut candidates = vec![];
    for hidden_ordinal in 0..row_column_types.len() {
        let mut matched = 0usize;
        let mut mismatched = false;
        for (row_ordinal, row_column_type) in row_column_types.iter().enumerate() {
            if row_ordinal == hidden_ordinal {
                continue;
            }
            let visible_ordinal = if row_ordinal < hidden_ordinal {
                row_ordinal
            } else {
                row_ordinal - 1
            };
            let expected = visible_type_categories[visible_ordinal];
            let actual = binlog_type_category(*row_column_type);
            if expected == MysqlBinlogTypeCategory::Unknown
                || actual == MysqlBinlogTypeCategory::Unknown
            {
                continue;
            }
            if expected != actual {
                mismatched = true;
                break;
            }
            matched += 1;
        }

        if !mismatched {
            candidates.push((hidden_ordinal, matched));
        }
    }

    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.1));
    match candidates.as_slice() {
        [(hidden_ordinal, _)] => Some(*hidden_ordinal),
        [(hidden_ordinal, best_score), (_, second_score), ..] if best_score > second_score => {
            Some(*hidden_ordinal)
        }
        _ => None,
    }
}

fn hidden_column_position_matches_types(
    hidden_ordinal: usize,
    visible_type_categories: &[MysqlBinlogTypeCategory],
    row_column_types: &[u8],
) -> bool {
    if row_column_types.len() != visible_type_categories.len() + 1
        || hidden_ordinal >= row_column_types.len()
    {
        return false;
    }

    for (row_ordinal, row_column_type) in row_column_types.iter().enumerate() {
        if row_ordinal == hidden_ordinal {
            continue;
        }
        let visible_ordinal = if row_ordinal < hidden_ordinal {
            row_ordinal
        } else {
            row_ordinal - 1
        };
        let expected = visible_type_categories[visible_ordinal];
        let actual = binlog_type_category(*row_column_type);
        if expected == MysqlBinlogTypeCategory::Unknown
            || actual == MysqlBinlogTypeCategory::Unknown
        {
            continue;
        }
        if expected != actual {
            return false;
        }
    }
    true
}

fn functional_index_hidden_column_positions(
    create_table_sql: &str,
    columns: &[String],
) -> Vec<usize> {
    let mut result = vec![];
    for raw_line in create_table_sql.lines() {
        let line = raw_line.trim();
        let line_lower = line.to_ascii_lowercase();
        if line.starts_with('`') || !line_lower.contains("((") {
            continue;
        }
        if !(line_lower.starts_with("key ")
            || line_lower.starts_with("index ")
            || line_lower.starts_with("unique key ")
            || line_lower.starts_with("unique index ")
            || line_lower.starts_with("constraint "))
        {
            continue;
        }

        let Some(expression_start) = line.find("((") else {
            continue;
        };
        let expression = &line[expression_start..];
        let referenced_ordinals = backtick_identifiers(expression)
            .into_iter()
            .filter_map(|identifier| {
                columns
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(identifier.as_str()))
            })
            .collect::<Vec<_>>();
        let Some(max_referenced_ordinal) = referenced_ordinals.into_iter().max() else {
            continue;
        };
        let hidden_ordinal = max_referenced_ordinal + 1;
        if !result.contains(&hidden_ordinal) {
            result.push(hidden_ordinal);
        }
    }
    result
}

fn backtick_identifiers(text: &str) -> Vec<String> {
    let mut result = vec![];
    let mut remaining = text;
    while let Some(start) = remaining.find('`') {
        let after_start = &remaining[(start + 1)..];
        let Some(end) = after_start.find('`') else {
            break;
        };
        result.push(after_start[..end].to_string());
        remaining = &after_start[(end + 1)..];
    }
    result
}

fn row_column_names_with_hidden_position(
    columns: Vec<String>,
    row_column_count: usize,
    hidden_ordinal: usize,
) -> Vec<Option<String>> {
    let mut columns = columns.into_iter();
    (0..row_column_count)
        .map(|ordinal| {
            if ordinal == hidden_ordinal {
                None
            } else {
                columns.next()
            }
        })
        .collect()
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
    for (ordinal, (column_name, line)) in column_lines_from_create_table_sql(create_table_sql)
        .into_iter()
        .enumerate()
    {
        let line_lower = line.to_ascii_lowercase();
        if line_lower.contains("generated always")
            || line_lower.contains("stored generated")
            || line_lower.contains("virtual generated")
        {
            result.push((ordinal, column_name));
        }
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

fn create_table_column_lines_for_table(
    config: &MysqlSourceConfigDetail,
    table_name: &str,
) -> Vec<(String, String)> {
    config
        .table_info_list
        .iter()
        .find(|table_info| table_info.table_name.eq_ignore_ascii_case(table_name))
        .map(|table_info| column_lines_from_create_table_sql(table_info.create_table_sql.as_str()))
        .unwrap_or_default()
}

fn create_table_sql_for_table<'a>(
    config: &'a MysqlSourceConfigDetail,
    table_name: &str,
) -> Option<&'a str> {
    config
        .table_info_list
        .iter()
        .find(|table_info| table_info.table_name.eq_ignore_ascii_case(table_name))
        .map(|table_info| table_info.create_table_sql.as_str())
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

    let create_table_column_lines =
        create_table_column_lines_for_table(config, info.table_name.as_str());
    let create_table_columns = create_table_column_lines
        .iter()
        .map(|(column_name, _)| column_name.clone())
        .collect::<Vec<_>>();
    if create_table_columns.len() == info.column_count {
        let table_label = database_table_key(info.database_name.as_str(), info.table_name.as_str());
        warn!(
            "MySQL binlog TableMap缺少列名元数据 table={} row_columns={}; 使用SHOW CREATE TABLE完整列顺序对齐",
            table_label, info.column_count
        );
        info.row_column_names = Some(create_table_columns.into_iter().map(Some).collect());
        return info;
    }
    if create_table_columns.len() + 1 == info.column_count {
        let visible_type_categories = create_table_column_lines
            .iter()
            .map(|(_, column_definition)| {
                create_table_column_type_category(column_definition.as_str())
            })
            .collect::<Vec<_>>();
        let functional_hidden_ordinals =
            create_table_sql_for_table(config, info.table_name.as_str())
                .map(|create_table_sql| {
                    functional_index_hidden_column_positions(
                        create_table_sql,
                        create_table_columns.as_slice(),
                    )
                    .into_iter()
                    .filter(|hidden_ordinal| {
                        hidden_column_position_matches_types(
                            *hidden_ordinal,
                            &visible_type_categories,
                            &event.column_types,
                        )
                    })
                    .collect::<Vec<_>>()
                })
                .unwrap_or_default();
        let hidden_ordinal = if functional_hidden_ordinals.len() == 1 {
            functional_hidden_ordinals.first().copied()
        } else {
            infer_single_hidden_column_position_by_types(
                &visible_type_categories,
                &event.column_types,
            )
        };
        if let Some(hidden_ordinal) = hidden_ordinal {
            let table_label =
                database_table_key(info.database_name.as_str(), info.table_name.as_str());
            warn!(
                "MySQL binlog TableMap缺少列名元数据 table={} row_columns={} \
                 create_table_columns={}; 根据TableMap列类型推断隐藏列位置 hidden_ordinal={} \
                 functional_hidden_ordinals={:?} focus_positions={:?}",
                table_label,
                info.column_count,
                create_table_columns.len(),
                hidden_ordinal,
                functional_hidden_ordinals,
                focus_column_positions(&create_table_columns)
            );
            info.row_column_names = Some(row_column_names_with_hidden_position(
                create_table_columns,
                info.column_count,
                hidden_ordinal,
            ));
            return info;
        }
    }
    if !create_table_columns.is_empty() {
        let table_label = database_table_key(info.database_name.as_str(), info.table_name.as_str());
        warn!(
            "MySQL binlog TableMap缺少列名元数据且SHOW CREATE TABLE列数不匹配 table={} \
             row_columns={} create_table_columns={} generated_columns={:?} focus_positions={:?}",
            table_label,
            info.column_count,
            create_table_columns.len(),
            generated_columns_for_table(config, info.table_name.as_str()),
            focus_column_positions(&create_table_columns)
        );
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
    if checkpoints.is_empty() {
        return ResumePosition::Latest;
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
        .and_then(|table| (!table.pk_column.trim().is_empty()).then(|| table.pk_column.clone()))
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
            sink.lock()
                .await
                .after_initialization()
                .await
                .map_err(|e| CustomError {
                    message: format!("sink after initialization failed: {}", e),
                    error_type: CustomErrorType::Restart,
                })?;

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
        let columns: Mutex<CaseInsensitiveHashMapVecString> =
            Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg());
        // 这里获取列名
        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        let mut table_column_info_map: HashMap<(usize, u64), BinlogTableColumnInfo> =
            HashMap::new();
        let mut last_checkpoint_save = Instant::now();
        let mut pending_filtered_checkpoints: HashMap<String, MysqlCheckPointDetailEntity> =
            HashMap::new();
        loop {
            if last_checkpoint_save.elapsed().as_secs() >= 5 {
                sink.lock()
                    .await
                    .flush_with_retry(&FlushByOperation::Cdc)
                    .await;
                self.save_pending_checkpoints(&mut pending_filtered_checkpoints)
                    .await;
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
                                    let event_checkpoint_entity = if binlog_filename.is_empty() {
                                        checkpoint_entity.clone()
                                    } else {
                                        checkpoint_entity
                                            .clone()
                                            .update(binlog_filename.clone(), next_event_position)
                                    };
                                    let pk_column = table_pk_column(config, table_name.as_str());
                                    let mut wrote_any = false;
                                    for row in event.rows {
                                        let before: CaseInsensitiveHashMap =
                                            CaseInsensitiveHashMap::new_with_no_arg();
                                        let after: CaseInsensitiveHashMap = parse_row(
                                            row,
                                            table_name.as_str(),
                                            &columns,
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
                                                Some(event_checkpoint_entity.clone()),
                                            )
                                            .await;
                                            wrote_any = true;
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
                                        checkpoint_entity = event_checkpoint_entity.clone();
                                        to_modify.insert(
                                            checkpoint_entity.table.clone(),
                                            checkpoint_entity.clone(),
                                        );
                                        if !wrote_any {
                                            pending_filtered_checkpoints.insert(
                                                checkpoint_entity.table.clone(),
                                                checkpoint_entity,
                                            );
                                        }
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
                                    let event_checkpoint_entity = if binlog_filename.is_empty() {
                                        checkpoint_entity.clone()
                                    } else {
                                        checkpoint_entity
                                            .clone()
                                            .update(binlog_filename.clone(), next_event_position)
                                    };
                                    let pk_column = table_pk_column(config, table_name.as_str());
                                    let mut wrote_any = false;
                                    for row in event.rows {
                                        let before: CaseInsensitiveHashMap = parse_row(
                                            row,
                                            table_name.as_str(),
                                            &columns,
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
                                                Some(event_checkpoint_entity.clone()),
                                            )
                                            .await;
                                            wrote_any = true;
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
                                        checkpoint_entity = event_checkpoint_entity.clone();
                                        to_modify.insert(
                                            checkpoint_entity.table.clone(),
                                            checkpoint_entity.clone(),
                                        );
                                        if !wrote_any {
                                            pending_filtered_checkpoints.insert(
                                                checkpoint_entity.table.clone(),
                                                checkpoint_entity,
                                            );
                                        }
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
                                    let event_checkpoint_entity = if binlog_filename.is_empty() {
                                        checkpoint_entity.clone()
                                    } else {
                                        checkpoint_entity
                                            .clone()
                                            .update(binlog_filename.clone(), next_event_position)
                                    };
                                    let pk_column = table_pk_column(config, table_name.as_str());
                                    let mut wrote_any = false;
                                    for (b, a) in event.rows {
                                        let before: CaseInsensitiveHashMap = parse_row(
                                            b,
                                            table_name.as_str(),
                                            &columns,
                                            config,
                                            pool,
                                            binlog_columns.as_ref(),
                                        )
                                        .await;
                                        let after: CaseInsensitiveHashMap = parse_row(
                                            a,
                                            table_name.as_str(),
                                            &columns,
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
                                                Some(event_checkpoint_entity.clone()),
                                            )
                                            .await;
                                            wrote_any = true;
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
                                        checkpoint_entity = event_checkpoint_entity.clone();
                                        to_modify.insert(
                                            checkpoint_entity.table.clone(),
                                            checkpoint_entity.clone(),
                                        );
                                        if !wrote_any {
                                            pending_filtered_checkpoints.insert(
                                                checkpoint_entity.table.clone(),
                                                checkpoint_entity,
                                            );
                                        }
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

    #[test]
    fn random_check_checkpoint_entity_is_memory_only_and_new() {
        let entity = MySQLSource::random_check_checkpoint_entity("source_db.orders".to_string());

        assert!(entity.is_new);
        assert_eq!(entity.last_binlog_filename, "");
        assert_eq!(entity.last_binlog_position, 0);
        assert_eq!(
            entity.checkpoint_filepath,
            "memory://random-check/source_db.orders"
        );
        assert_eq!(entity.table, "source_db.orders");
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
    fn align_current_columns_keeps_present_generated_column() {
        let columns = vec![
            "path".to_string(),
            "HouseId".to_string(),
            "fullPath".to_string(),
            "bankCollectionLock".to_string(),
        ];

        assert_eq!(
            align_current_columns_to_row_columns(
                "source_db.orders",
                columns,
                5,
                None,
                &[(2, "fullPath".to_string())],
                "当前表结构"
            ),
            vec![
                Some("path".to_string()),
                Some("HouseId".to_string()),
                Some("fullPath".to_string()),
                Some("bankCollectionLock".to_string()),
                None
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

    #[test]
    fn infers_hidden_column_position_by_table_map_types() {
        let create_table_sql = r#"CREATE TABLE `charge_customerchargedetail` (
  `bankCollectionLock` int(11) DEFAULT '0',
  `checkTime` datetime DEFAULT NULL,
  `uuid` varchar(64) DEFAULT NULL,
  `beforePrice` decimal(10,2) DEFAULT NULL,
  `settleDate` date DEFAULT NULL,
  PRIMARY KEY (`uuid`)
)"#;
        let column_lines = column_lines_from_create_table_sql(create_table_sql);
        let visible_type_categories = column_lines
            .iter()
            .map(|(_, column_definition)| {
                create_table_column_type_category(column_definition.as_str())
            })
            .collect::<Vec<_>>();
        let row_column_types = vec![
            ColumnType::Long as u8,
            ColumnType::DateTime2 as u8,
            ColumnType::VarChar as u8,
            ColumnType::Long as u8,
            ColumnType::NewDecimal as u8,
            ColumnType::Date as u8,
        ];

        assert_eq!(
            infer_single_hidden_column_position_by_types(
                &visible_type_categories,
                &row_column_types
            ),
            Some(3)
        );

        let row_column_names = row_column_names_with_hidden_position(
            column_lines
                .into_iter()
                .map(|(column_name, _)| column_name)
                .collect(),
            row_column_types.len(),
            3,
        );
        assert_eq!(
            row_column_names,
            vec![
                Some("bankCollectionLock".to_string()),
                Some("checkTime".to_string()),
                Some("uuid".to_string()),
                None,
                Some("beforePrice".to_string()),
                Some("settleDate".to_string()),
            ]
        );
    }

    #[test]
    fn functional_index_position_disambiguates_hidden_string_column() {
        let create_table_sql = r#"CREATE TABLE `charge_customerchargedetail` (
  `bankCollectionLock` int(11) DEFAULT '0',
  `checkTime` datetime DEFAULT NULL,
  `uuid` varchar(64) DEFAULT NULL,
  `beforePrice` decimal(10,2) DEFAULT NULL,
  `settleDate` date DEFAULT NULL,
  KEY `idx_uuid_lower` ((lower(`uuid`)))
)"#;
        let column_lines = column_lines_from_create_table_sql(create_table_sql);
        let columns = column_lines
            .iter()
            .map(|(column_name, _)| column_name.clone())
            .collect::<Vec<_>>();
        let visible_type_categories = column_lines
            .iter()
            .map(|(_, column_definition)| {
                create_table_column_type_category(column_definition.as_str())
            })
            .collect::<Vec<_>>();
        let row_column_types = vec![
            ColumnType::Long as u8,
            ColumnType::DateTime2 as u8,
            ColumnType::VarChar as u8,
            ColumnType::VarChar as u8,
            ColumnType::NewDecimal as u8,
            ColumnType::Date as u8,
        ];

        let candidates = functional_index_hidden_column_positions(create_table_sql, &columns)
            .into_iter()
            .filter(|hidden_ordinal| {
                hidden_column_position_matches_types(
                    *hidden_ordinal,
                    &visible_type_categories,
                    &row_column_types,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(candidates, vec![3]);
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
            random_check_data_after_init: false,
            random_check_data_after_init_batch_size_min: 10,
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
            random_check_data_after_init: false,
            random_check_data_after_init_batch_size_min: 10,
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
    fn compute_resume_position_latest_when_no_cdc_tables() {
        let checkpoints = HashMap::new();

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
    fn init_pk_cursor_supports_signed_int() {
        let cursor = InitPkCursor::from_value(&Value::Int32(7)).unwrap();

        assert_eq!(cursor, InitPkCursor::Signed(7));
        assert_eq!(cursor.sql_literal().as_deref(), Some("7"));
    }

    #[test]
    fn supported_pk_data_types_include_mysql_integer_types() {
        for data_type in ["tinyint", "smallint", "mediumint", "int", "bigint"] {
            assert!(is_supported_mysql_pk_data_type(data_type));
        }
        assert!(is_supported_mysql_pk_data_type("INT"));
        assert!(!is_supported_mysql_pk_data_type("varchar"));
        assert!(!is_supported_mysql_pk_data_type("decimal"));
    }

    #[test]
    fn full_table_discovery_includes_all_base_tables_when_schema_only_enabled() {
        let sql = MysqlSourceConfig::full_table_discovery_sql(true, true);

        assert!(!sql.contains("REFERENCED_TABLE_NAME IS NOT NULL"));
        assert!(sql.contains("t.TABLE_TYPE = 'BASE TABLE'"));
        assert!(!sql.contains("c.COLUMN_KEY = 'PRI'"));
        assert!(!sql.contains("HAVING COUNT(*) > 1"));
    }

    #[test]
    fn full_table_discovery_filters_integer_pk_when_schema_only_disabled() {
        let sql = MysqlSourceConfig::full_table_discovery_sql(true, false);

        assert!(sql.contains("c.COLUMN_KEY = 'PRI'"));
        assert!(sql.contains("c.DATA_TYPE IN"));
        assert!(sql.contains("HAVING COUNT(*) > 1"));
    }

    #[test]
    fn full_table_discovery_can_keep_legacy_foreign_key_exclusion() {
        let sql = MysqlSourceConfig::full_table_discovery_sql(false, true);

        assert!(sql.contains("REFERENCED_TABLE_NAME IS NOT NULL"));
        assert!(sql.contains("SELECT REFERENCED_TABLE_NAME"));
        assert!(sql.contains("t.TABLE_NAME NOT IN"));
    }

    fn column_info(column_name: &str, column_key: &str, data_type: &str) -> ColumnInfoFromMysql {
        ColumnInfoFromMysql {
            column_name: column_name.to_string(),
            column_key: column_key.to_string(),
            data_type: data_type.to_string(),
        }
    }

    #[test]
    fn table_cdc_pk_column_keeps_only_single_integer_primary_key_for_cdc() {
        let cols = vec![
            column_info("id", "PRI", "bigint"),
            column_info("name", "", "varchar"),
        ];

        assert_eq!(
            MysqlSourceConfig::table_cdc_pk_column("orders", &cols, true),
            Some("id".to_string())
        );
    }

    #[test]
    fn table_cdc_pk_column_marks_unsupported_tables_as_schema_only() {
        let string_pk_cols = vec![column_info("ID_", "PRI", "varchar")];
        let composite_pk_cols = vec![
            column_info("tenant_id", "PRI", "bigint"),
            column_info("code", "PRI", "varchar"),
        ];
        let no_pk_cols = vec![column_info("name", "", "varchar")];

        assert_eq!(
            MysqlSourceConfig::table_cdc_pk_column("string_pk", &string_pk_cols, true),
            Some(String::new())
        );
        assert_eq!(
            MysqlSourceConfig::table_cdc_pk_column("composite_pk", &composite_pk_cols, true),
            Some(String::new())
        );
        assert_eq!(
            MysqlSourceConfig::table_cdc_pk_column("no_pk", &no_pk_cols, true),
            Some(String::new())
        );
        assert_eq!(
            MysqlSourceConfig::table_cdc_pk_column("string_pk", &string_pk_cols, false),
            None
        );
    }

    #[test]
    fn init_pk_cursor_advances_forward_only() {
        let mut cursor = InitPkCursor::Start;

        cursor.advance(InitPkCursor::Unsigned(10));
        cursor.advance(InitPkCursor::Unsigned(8));
        cursor.advance(InitPkCursor::Unsigned(12));

        assert_eq!(cursor, InitPkCursor::Unsigned(12));
    }

    #[test]
    fn schema_only_tables_are_not_cdc_tables() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_db".to_string(),
            table_name: "no_pk_table".to_string(),
            pk_column: "".to_string(),
            create_table_sql: "CREATE TABLE `no_pk_table` (`name` varchar(64))".to_string(),
            columns: vec!["name".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let config = MysqlSourceConfigDetail {
            username: "".to_string(),
            password: "".to_string(),
            host: "".to_string(),
            port: "".to_string(),
            database: "source_db".to_string(),
            target_database: "target_db".to_string(),
            server_id: 1,
            connection_url: "".to_string(),
            table_info_list: vec![table_info],
            batchsize: 100,
            random_check_data_after_init: false,
            random_check_data_after_init_batch_size_min: 10,
            start_binlog_filename: None,
            start_binlog_position: None,
        };

        assert!(!config.is_target_database_and_table("source_db", "no_pk_table"));
        assert_eq!(table_pk_column(&config, "no_pk_table"), None);
    }
}
