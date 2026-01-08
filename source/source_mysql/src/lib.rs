extern crate core;

use async_trait::async_trait;
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::{
    CaseInsensitiveHashMap, CdcConfig, DataBuffer, FlushByOperation, Operation, Plugin, Sink,
    Source, TableInfoVo, Value, get_mysql_pool_by_url, mysql_row_to_hashmap,
};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::column::json::json_binary::JsonBinary;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use serde::Deserialize;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::Row;
use sqlx::mysql::MySqlRow;
use sqlx::{MySql, Pool};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};

pub struct MySQLSource {
    streams: Vec<BinlogStream>, // ✅ 多个流
    mysql_source: Vec<MysqlSourceConfigDetail>,
    pools: Vec<Pool<MySql>>,
    checkpoint_entities: Mutex<Vec<Mutex<HashMap<String, MysqlCheckPointDetailEntity>>>>,
    plugins: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>,
    binlog_filename_list: Mutex<Vec<Mutex<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSourceConfig {
    table_name_list: Vec<String>,
    mysql_source: Vec<MysqlSourceConfigDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MysqlSourceConfigDetail {
    username: String,
    password: String,
    host: String,
    port: String,
    database: String,
    server_id: u64,
    connection_url: String,
    table_info_list: Vec<TableInfoVo>,
    batchsize: usize,
}

impl MysqlSourceConfig {
    pub async fn new(config: &CdcConfig) -> Self {
        let size = config.source_config.len();
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        // TODO 后续要支持多张表
        // split table_name
        let mut table_name_list: Vec<String> = config
            .first_source("table_name")
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let except_table_name_prefix: Vec<String> = config
            .first_source("except_table_name_prefix")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        for i in 0..size {
            let username = config.source("username", i);
            let password = config.source("password", i);
            let host = config.source("host", i);
            let port = config.source("port", i);
            let database = config.source("database", i);
            let server_id: u64 = config.source("server_id", i).parse::<u64>().unwrap_or(0);
            let connection_url = format!(
                "mysql://{}:{}@{}:{}/{}",
                username,
                password,
                host,
                port,
                database.clone(),
            );
            let mut table_info_list: Vec<TableInfoVo> = vec![];
            let pool = get_mysql_pool_by_url(&connection_url, "mysql source 初始化获取数据结构")
                .await
                .unwrap();
            if table_name_list.is_empty()
                || table_name_list.len() == 1
                    && (table_name_list[0].eq_ignore_ascii_case("all")
                        || table_name_list[0].eq_ignore_ascii_case("*"))
            {
                // get all tables
                let show_tables_sql = r#"
                    SELECT distinct c.TABLE_NAME AS table_name
                    FROM information_schema.COLUMNS c
                    WHERE c.TABLE_SCHEMA = (SELECT DATABASE())
                      AND c.COLUMN_KEY = 'PRI'
                      AND c.COLUMN_TYPE = 'bigint'
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
                                    AND cc.COLUMN_TYPE = 'bigint'
                                GROUP BY cc.TABLE_NAME
                                HAVING COUNT(*) > 1
                        )
                "#;
                let tables: Vec<String> = sqlx::query(show_tables_sql)
                    .fetch_all(&pool)
                    .await
                    .expect("query failed")
                    .into_iter()
                    .map(|row| mysql_row_to_hashmap(&row))
                    .map(|row| {
                        row.get("table_name")
                            // .unwrap_or_else(|| panic!("table_name not found"))
                            .resolve_string()
                    })
                    // .map(|row| row.table_name)
                    .collect();
                info!("get all tables: {:?}", tables);
                table_name_list.clear();
                table_name_list.extend(tables);
            }

            for table_name in table_name_list.clone() {
                // fill table_info
                let show_create_table_sql = format!("show create table `{}`", table_name);
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
                    .filter(|c| c.data_type == "bigint")
                    .map(|c| c.column_name.clone())
                    .collect();
                if pk_column.is_empty() || pk_column.len() > 1 {
                    error!("pk_column is empty or more than one");
                    // panic!("pk_column is empty or more than one");
                    continue;
                }
                let pk_column = pk_column[0].clone();
                let columns: Vec<String> = col_list.iter().map(|c| c.column_name.clone()).collect();

                table_info_list.push(TableInfoVo {
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
                server_id,
                connection_url,
                table_info_list,
                batchsize: config.source_batch_size.unwrap_or(8192),
            });
        }
        table_name_list = table_name_list
            .iter()
            .filter(|t| !Self::judge_is_skip(&except_table_name_prefix, t))
            .map(|t| t.to_string())
            .collect();
        if table_name_list.is_empty() {
            error!("no table found");
            panic!("no table found");
        }
        MysqlSourceConfig {
            table_name_list,
            mysql_source,
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

impl MysqlSourceConfigDetail {
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
        id: i64,
        pool: &Pool<MySql>,
    ) -> Vec<DataBuffer> {
        let sql = format!(
            r#"
                select *
                FROM {}
                where {} > {}
                order by {}
                limit {}
            "#,
            table_name, pk_column, id, pk_column, self.batchsize
        );
        debug!(
            "extract_init_data: [{}.{}] {} {}",
            self.database, table_name, pk_column, id
        );
        // 查询 Row，而不是 HashMap
        let rows: Vec<MySqlRow> = sqlx::query(&sql)
            .fetch_all(pool)
            .await
            .expect("query failed");
        info!(
            "extract_init_data: [{}.{}] {} {} {} rows",
            self.database,
            table_name,
            pk_column,
            id,
            rows.len()
        );
        let mut result: Vec<DataBuffer> = vec![];
        for row in rows {
            let before = CaseInsensitiveHashMap::new(HashMap::new());
            let after = mysql_row_to_hashmap(&row);
            let op = Operation::CREATE(true);
            result.push(DataBuffer::new(
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
        let mut streams: Vec<BinlogStream> = vec![];
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        let mut pools: Vec<Pool<MySql>> = vec![];
        // 这里支持多个数据源配置
        let cfg: MysqlSourceConfig = MysqlSourceConfig::new(config).await;
        let size = cfg.mysql_source.len();
        let binlog_filename_list: Mutex<Vec<Mutex<String>>> = Mutex::new(Vec::new());
        let mut checkpoint_entities: Vec<Mutex<HashMap<String, MysqlCheckPointDetailEntity>>> =
            vec![];
        for i in 0..size {
            let server_id: u64 = cfg.mysql_source[i].server_id;
            let connection_url = cfg.mysql_source[i].connection_url.clone();
            let tables: Vec<String> = cfg.mysql_source[i]
                .table_info_list
                .clone()
                .iter()
                .map(|t| t.table_name.to_lowercase().to_string())
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
            checkpoint_entities.push(Mutex::new(mysql_checkpoint_detail_entity_map.clone()));

            let start_position: StartPosition = if mysql_checkpoint_detail_entity_map
                .values()
                .all(|entity| entity.is_new)
            {
                StartPosition::Latest
            } else {
                // 如果部分是新的表，选取最新的一个的binlog开始同步
                let max: MysqlCheckPointDetailEntity = mysql_checkpoint_detail_entity_map
                    .values()
                    .max_by(|a, b| {
                        a.last_binlog_filename
                            .cmp(&b.last_binlog_filename)
                            .then(a.last_binlog_position.cmp(&b.last_binlog_position))
                    })
                    .unwrap()
                    .clone();
                StartPosition::BinlogPosition(max.last_binlog_filename, max.last_binlog_position)
            };

            let client: BinlogStream =
                BinlogClient::new(&connection_url, server_id, start_position)
                    .with_master_heartbeat(Duration::from_secs(5))
                    .with_read_timeout(Duration::from_secs(60))
                    .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
                    .connect()
                    .await
                    .expect("Failed to connect to MySQL server");

            streams.push(client);
            mysql_source.push(cfg.mysql_source[i].clone());
            let pool: Pool<MySql> = get_mysql_pool_by_url(&connection_url, "mysql source 初始化")
                .await
                .unwrap();
            pools.push(pool);
            binlog_filename_list
                .lock()
                .await
                .push(Mutex::new("".to_string()));
        }

        Self {
            streams,
            mysql_source,
            pools,
            checkpoint_entities: Mutex::new(checkpoint_entities),
            plugins: vec![],
            binlog_filename_list,
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
        let after_plugin = p.lock().await.collect(data_buffer.clone()).await;
        if after_plugin.is_err() {
            return Err(());
        }
        data_buffer = after_plugin?;
    }
    Ok(data_buffer)
}
#[async_trait]
impl Source for MySQLSource {
    async fn start(
        &mut self,
        mut sink: Arc<Mutex<dyn Sink + Send + Sync>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let plugins: &Vec<Arc<Mutex<dyn Plugin + Send + Sync>>> = &self.plugins;
        {
            info!("开始MySQL数据源初始化");
            let max = self.pools.len();
            let start_all = Instant::now();
            for i in 0..max {
                let pool: &mut Pool<MySql> = &mut self.pools[i];
                let config: &MysqlSourceConfigDetail = &mut self.mysql_source[i];

                for table_info_vo in config.table_info_list.clone() {
                    let table_name = table_info_vo.table_name.clone();
                    let checkpoint_entity: &mut Mutex<
                        HashMap<String, MysqlCheckPointDetailEntity>,
                    > = &mut self.checkpoint_entities.lock().await[i];
                    let checkpoint_entity = &mut checkpoint_entity
                        .lock()
                        .await
                        .get(&table_name.to_lowercase())
                        .unwrap_or_else(|| panic!("{} not found", table_name))
                        .clone();
                    if !checkpoint_entity.is_new {
                        info!("跳过初始化数据源: {}", config.connection_url);
                        continue;
                    }

                    let pk_column = table_info_vo.pk_column.clone();
                    info!("开始初始化数据源: {}.{}", config.connection_url, table_name);
                    let start = Instant::now();
                    let mut count = 0;
                    // 这里进行循环，一批一批进行数据写入
                    let mut id: i64 = i64::MIN;
                    loop {
                        let data_buffer_list: Vec<DataBuffer> = config
                            .extract_init_data(&table_name, &pk_column, id, pool)
                            .await;
                        let len = data_buffer_list.len();
                        for data_buffer in data_buffer_list.iter().take(len) {
                            let plugin_data =
                                detail_with_plugin(plugins, data_buffer.clone()).await;
                            if let Ok(item) = plugin_data {
                                Self::write_record_with_retry(&mut sink, &item, None).await;
                            }
                            let this_id = data_buffer.after.get(&pk_column);
                            let this_id = match this_id {
                                Value::Int64(x) => x,
                                _ => {
                                    panic!("pk_column not found");
                                }
                            };
                            if this_id > &id {
                                id = *this_id;
                            }
                        }
                        count += len;
                        debug!("当前最大id为 {}", id);
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
                        config.connection_url,
                        table_name,
                        count,
                        start.elapsed()
                    );
                    match checkpoint_entity.save() {
                        Ok(_) => {
                            info!(
                                "alter_flush success {}",
                                checkpoint_entity.checkpoint_filepath
                            )
                        }
                        Err(e) => {
                            error!("alter_flush error: {}", e)
                        }
                    }
                }
            }
            info!("MySQL数据源初始化完成, cost: {:?}", start_all.elapsed());
        }

        info!("Starting MySQL binlog source");
        let mut columns: Mutex<Vec<String>> = Mutex::new(vec![]);
        // 这里获取列名
        let mut table_map = HashMap::new();
        let mut table_database_map = HashMap::new();
        loop {
            let max = self.streams.len();
            for i in 0..max {
                let stream: &mut BinlogStream = &mut self.streams[i];
                let pool: &mut Pool<MySql> = &mut self.pools[i];
                let config: &MysqlSourceConfigDetail = &mut self.mysql_source[i];
                let mut to_modify = self.checkpoint_entities.lock().await[i]
                    .lock()
                    .await
                    .clone();

                match stream.read().await {
                    Ok((header, data)) => match data {
                        EventData::Rotate(event) => {
                            *self.binlog_filename_list.lock().await[i].lock().await =
                                event.binlog_filename.clone();
                        }
                        EventData::TableMap(event) => {
                            let table_name = event.table_name;
                            let table_id = event.table_id;
                            let database_name = event.database_name;
                            table_map.insert(table_id, table_name);
                            table_database_map.insert(table_id, database_name);
                        }
                        EventData::WriteRows(event) => {
                            let table_name = table_map
                                .get(&event.table_id)
                                .unwrap_or_else(|| panic!("Table id {} not found", event.table_id))
                                .as_str();
                            let database_name = table_database_map
                                .get(&event.table_id)
                                .unwrap_or_else(|| panic!("Table id {} not found", event.table_id))
                                .as_str();
                            if config.is_target_database_and_table(database_name, table_name) {
                                debug!("WriteRows: {}.{}", database_name, table_name);

                                let checkpoint_entity: &mut MysqlCheckPointDetailEntity =
                                    &mut self.checkpoint_entities.lock().await[i]
                                        .lock()
                                        .await
                                        .get(&table_name.to_lowercase())
                                        .expect("checkpoint_entity not found")
                                        .clone();
                                let mut checkpoint_entity = checkpoint_entity.clone();

                                let binlog_filename = self.binlog_filename_list.lock().await[i]
                                    .lock()
                                    .await
                                    .clone();
                                let timestamp = header.timestamp;
                                let next_event_position = header.next_event_position;
                                for row in event.rows {
                                    let before: CaseInsensitiveHashMap =
                                        CaseInsensitiveHashMap::new_with_no_arg();
                                    let after: CaseInsensitiveHashMap =
                                        parse_row(row, table_name, &mut columns, config, pool)
                                            .await;
                                    let op = Operation::CREATE(false);
                                    let data_buffer = DataBuffer::new(
                                        table_name.to_string(),
                                        before,
                                        after,
                                        op,
                                        binlog_filename.clone(),
                                        timestamp,
                                        next_event_position,
                                    );
                                    let plugin_data =
                                        detail_with_plugin(plugins, data_buffer).await;
                                    if let Ok(item) = plugin_data {
                                        Self::write_record_with_retry(
                                            &mut sink,
                                            &item,
                                            Some(checkpoint_entity.clone()),
                                        )
                                        .await;
                                    }
                                }
                                if !binlog_filename.is_empty() {
                                    checkpoint_entity = checkpoint_entity
                                        .update(binlog_filename, next_event_position);
                                    to_modify
                                        .insert(checkpoint_entity.table.clone(), checkpoint_entity);
                                }
                            }
                        }
                        EventData::DeleteRows(event) => {
                            let table_name = table_map
                                .get(&event.table_id)
                                .unwrap_or_else(|| panic!("Table id {} not found", event.table_id))
                                .as_str();
                            let database_name = table_database_map
                                .get(&event.table_id)
                                .unwrap_or_else(|| panic!("Table id {} not found", event.table_id))
                                .as_str();
                            if config.is_target_database_and_table(database_name, table_name) {
                                debug!("DeleteRows: {}.{}", database_name, table_name);
                                let checkpoint_entity: &mut MysqlCheckPointDetailEntity =
                                    &mut self.checkpoint_entities.lock().await[i]
                                        .lock()
                                        .await
                                        .get(&table_name.to_lowercase())
                                        .expect("checkpoint_entity not found")
                                        .clone();
                                let mut checkpoint_entity = checkpoint_entity.clone();

                                let binlog_filename = self.binlog_filename_list.lock().await[i]
                                    .lock()
                                    .await
                                    .clone();
                                let timestamp = header.timestamp;
                                let next_event_position = header.next_event_position;
                                for row in event.rows {
                                    let before: CaseInsensitiveHashMap =
                                        parse_row(row, table_name, &mut columns, config, pool)
                                            .await;
                                    let after: CaseInsensitiveHashMap =
                                        CaseInsensitiveHashMap::new_with_no_arg();
                                    let op = Operation::DELETE;
                                    let data_buffer = DataBuffer::new(
                                        table_name.to_string(),
                                        before,
                                        after,
                                        op,
                                        binlog_filename.clone(),
                                        timestamp,
                                        next_event_position,
                                    );
                                    let plugin_data =
                                        detail_with_plugin(plugins, data_buffer).await;
                                    if let Ok(item) = plugin_data {
                                        Self::write_record_with_retry(
                                            &mut sink,
                                            &item,
                                            Some(checkpoint_entity.clone()),
                                        )
                                        .await;
                                    }
                                }
                                if !binlog_filename.is_empty() {
                                    checkpoint_entity = checkpoint_entity
                                        .update(binlog_filename, next_event_position);
                                    to_modify
                                        .insert(checkpoint_entity.table.clone(), checkpoint_entity);
                                }
                            }
                        }
                        EventData::UpdateRows(event) => {
                            let table_name = table_map
                                .get(&event.table_id)
                                .unwrap_or_else(|| panic!("Table id {} not found", event.table_id))
                                .as_str();
                            let database_name = table_database_map
                                .get(&event.table_id)
                                .unwrap_or_else(|| panic!("Table id {} not found", event.table_id))
                                .as_str();
                            if config.is_target_database_and_table(database_name, table_name) {
                                debug!("UpdateRows: {}.{}", database_name, table_name);
                                let checkpoint_entity: &mut MysqlCheckPointDetailEntity =
                                    &mut self.checkpoint_entities.lock().await[i]
                                        .lock()
                                        .await
                                        .get(&table_name.to_lowercase())
                                        .expect("checkpoint_entity not found")
                                        .clone();
                                let mut checkpoint_entity = checkpoint_entity.clone();

                                let binlog_filename = self.binlog_filename_list.lock().await[i]
                                    .lock()
                                    .await
                                    .clone();
                                let timestamp = header.timestamp;
                                let next_event_position = header.next_event_position;
                                for (b, a) in event.rows {
                                    let before: CaseInsensitiveHashMap =
                                        parse_row(b, table_name, &mut columns, config, pool).await;
                                    let after: CaseInsensitiveHashMap =
                                        parse_row(a, table_name, &mut columns, config, pool).await;
                                    let op = Operation::UPDATE;
                                    let data_buffer = DataBuffer::new(
                                        table_name.to_string(),
                                        before,
                                        after,
                                        op,
                                        binlog_filename.clone(),
                                        timestamp,
                                        next_event_position,
                                    );
                                    let plugin_data =
                                        detail_with_plugin(plugins, data_buffer).await;
                                    if let Ok(item) = plugin_data {
                                        Self::write_record_with_retry(
                                            &mut sink,
                                            &item,
                                            Some(checkpoint_entity.clone()),
                                        )
                                        .await;
                                    }
                                }

                                if !binlog_filename.is_empty() {
                                    checkpoint_entity = checkpoint_entity
                                        .update(binlog_filename, next_event_position);
                                    to_modify
                                        .insert(checkpoint_entity.table.clone(), checkpoint_entity);
                                }
                            }
                        }
                        _ => {}
                    },
                    Err(e) => {
                        // 打印错误信息
                        error!("Error: {}", e);
                        panic!("Error: {}", e);
                    }
                }
                trace!("Checkpoint: {:?}", to_modify);
                *self.checkpoint_entities.lock().await[i].lock().await = to_modify;
            }
        }
    }

    async fn add_plugins(&mut self, plugin: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>) {
        self.plugins = plugin;
    }

    async fn get_table_info(&mut self) -> Vec<TableInfoVo> {
        self.mysql_source[0].table_info_list.clone()
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
    columns: &mut Mutex<Vec<String>>,
    config: &MysqlSourceConfigDetail,
    pool: &mut Pool<MySql>,
) -> CaseInsensitiveHashMap {
    let mut data: HashMap<String, Value> = HashMap::new();
    let mut index = 0;
    if columns.lock().await.len() != row.column_values.len() {
        let columns_new = config.fill_table_column(table_name, pool).await;
        columns.lock().await.clear();
        columns.lock().await.extend(columns_new);
    }
    if columns.lock().await.len() != row.column_values.len() {
        panic!("columns length not equal to column_values length");
    }

    for column_value in row.column_values {
        let column_name = columns.lock().await[index].clone();
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
                let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
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
                columns
                    .lock()
                    .await
                    .iter()
                    .for_each(|column_name| error!("column: {}", column_name));
                error!("column_name: {:?}", column_name);
                error!("column_value: {:?}", column_value);
                panic!("unsupported column value type")
            }
        }

        index += 1;
    }
    CaseInsensitiveHashMap::new(data)
}
