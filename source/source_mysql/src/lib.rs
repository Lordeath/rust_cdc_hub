extern crate core;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use common::{
    CdcConfig, DataBuffer, FlushByOperation, Operation, Plugin, Sink, Source, TableInfoVo, Value,
};
use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use mysql_binlog_connector_rust::binlog_stream::BinlogStream;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use serde::Deserialize;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::mysql::MySqlRow;
use sqlx::types::BigDecimal;
use sqlx::{Column, MySqlPool, Row, ValueRef};
use sqlx::{MySql, Pool, TypeInfo};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace};

const LOOP_PACE: usize = 8192;

pub struct MySQLSource {
    streams: Vec<BinlogStream>, // ✅ 多个流
    mysql_source: Vec<MysqlSourceConfigDetail>,
    pools: Vec<Pool<MySql>>,
    plugins: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>,
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
            let pool = MySqlPool::connect(&connection_url).await.unwrap();
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
                let tables: Vec<String> = sqlx::query(&show_tables_sql)
                    .fetch_all(&pool)
                    .await
                    .expect("query failed")
                    .into_iter()
                    .map(|row| mysql_row_to_hashmap(&row))
                    .map(|row| row.get("table_name").unwrap().resolve_string())
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
                let col_list = sqlx::query_as::<_, ColumnInfoFromMysql>(&pk_column_sql)
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
                    panic!("pk_column is empty or more than one");
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
            });
        }
        MysqlSourceConfig {
            table_name_list,
            mysql_source,
        }
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
        self.database.eq_ignore_ascii_case(database_name)
            && table_name.eq_ignore_ascii_case(table_name)
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
            table_name, pk_column, id, pk_column, LOOP_PACE
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
        info!("extract_init_data: {} rows", rows.len());
        let mut result: Vec<DataBuffer> = vec![];
        for row in rows {
            let before = HashMap::new();
            let after = mysql_row_to_hashmap(&row);
            let op = Operation::CREATE;
            result.push(DataBuffer {
                table_name: table_name.to_string(),
                before,
                after,
                op,
            });
        }
        result
    }

}


#[inline]
fn mysql_row_to_hashmap(row: &MySqlRow) -> HashMap<String, Value> {
    let mut result = HashMap::new();
    for row_column in row.columns().iter().enumerate() {
        let column = row_column.1;
        let name = column.name().to_string();
        let is_null = row
            .try_get_raw(name.as_str())
            .expect("mysql_row_to_hashmap")
            .is_null();

        let value = if is_null {
            Value::None
        } else {
            match column.type_info().name() {
                "INT" => match row.try_get::<i32, _>(name.as_str()) {
                    Ok(v) => Value::Int32(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "INT UNSIGNED" => match row.try_get::<u32, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt32(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BIGINT UNSIGNED" => match row.try_get::<u64, _>(name.as_str()) {
                    Ok(v) => Value::UnsignedInt64(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BIGINT" => match row.try_get::<i64, _>(name.as_str()) {
                    Ok(v) => Value::Int64(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "BOOLEAN" => match row.try_get::<bool, _>(name.as_str()) {
                    Ok(v) => {
                        if v {
                            Value::Int8(0)
                        } else {
                            Value::Int8(1)
                        }
                    }
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "TINYINT" => match row.try_get::<i8, _>(name.as_str()) {
                    Ok(v) => Value::Int8(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DATE" => match row.try_get::<NaiveDate, _>(name.as_str()) {
                    Ok(v) => Value::String(v.to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DATETIME" => match row.try_get::<NaiveDateTime, _>(name.as_str()) {
                    Ok(v) => Value::String(v.to_string()),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "TIMESTAMP" => match row.try_get::<DateTime<Utc>, _>(name.as_str()) {
                    Ok(v) => {
                        // 推荐：将带时区的 DateTime<Utc> 转换为 Unix 时间戳（i64 秒数）
                        let ts_ms = v.timestamp_millis();
                        let secs = ts_ms / 1000;
                        let nanos = ((ts_ms % 1000) * 1_000_000) as u32;

                        let dt = DateTime::from_timestamp(secs, nanos)
                            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
                            .naive_utc();

                        let x = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
                        // 使用 DateTime
                        Value::DateTime(x)
                    }
                    Err(e) => {
                        // ... 错误处理保持不变
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "VARCHAR" | "TEXT" => match row.try_get::<String, _>(name.as_str()) {
                    Ok(v) => Value::String(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "FLOAT" | "DOUBLE" => match row.try_get::<f64, _>(name.as_str()) {
                    Ok(v) => Value::Double(v),
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                "DECIMAL" => match row.try_get::<BigDecimal, _>(name.as_str()) {
                    Ok(v) => Value::Decimal(v.to_string()), // 假设您有 Value::Decimal 变体
                    Err(e) => {
                        error!("类型转换失败: {}", column.type_info().name());
                        error!("{}", e);
                        panic!("类型转换失败: {}", column.type_info().name());
                    }
                },
                _ => {
                    error!("Unsupported column type: {}", column.type_info().name());
                    panic!("Unsupported column type: {}", column.type_info().name())
                }
            }
        };

        result.insert(column.name().to_string(), value);
    }
    result
}

impl MySQLSource {
    pub async fn new(config: &CdcConfig) -> Self {
        let mut streams: Vec<BinlogStream> = vec![];
        let mut mysql_source: Vec<MysqlSourceConfigDetail> = vec![];
        let mut pools: Vec<Pool<MySql>> = vec![];
        // 这里支持多个数据源配置
        let cfg: MysqlSourceConfig = MysqlSourceConfig::new(config).await;
        let size = cfg.mysql_source.len();
        for i in 0..size {
            let server_id: u64 = cfg.mysql_source[i].server_id;
            let connection_url = cfg.mysql_source[i].connection_url.clone();
            let client: BinlogStream =
                BinlogClient::new(&connection_url, server_id, StartPosition::Latest)
                    .with_master_heartbeat(Duration::from_secs(5))
                    .with_read_timeout(Duration::from_secs(60))
                    .with_keepalive(Duration::from_secs(60), Duration::from_secs(10))
                    .connect()
                    .await
                    .expect("Failed to connect to MySQL server");

            streams.push(client);
            mysql_source.push(cfg.mysql_source[i].clone());
            let pool: Pool<MySql> = MySqlPool::connect(&connection_url).await.unwrap();
            pools.push(pool);
        }

        Self {
            streams,
            mysql_source,
            pools,
            plugins: vec![],
        }
    }

    async fn write_record_with_retry(
        sink: &mut Arc<Mutex<dyn Sink + Send + Sync>>,
        data_buffer: &DataBuffer,
    ) {
        let mut loop_count = 0;
        loop {
            let sink_result = sink.lock().await.write_record(data_buffer).await;
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
        if !after_plugin.is_ok() {
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
            for i in 0..max {
                let pool: &mut Pool<MySql> = &mut self.pools[i];
                let config: &MysqlSourceConfigDetail = &mut self.mysql_source[i];
                for table_info_vo in config.table_info_list.clone() {
                    let table_name = table_info_vo.table_name.clone();
                    let pk_column = table_info_vo.pk_column.clone();
                    info!("开始初始化数据源: {}.{}", config.connection_url, table_name);
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
                            if plugin_data.is_ok() {
                                Self::write_record_with_retry(&mut sink, &plugin_data.unwrap())
                                    .await;
                            }
                            let this_id = data_buffer.after.get(&pk_column).unwrap_or_else(|| {
                                panic!("pk_column not found: {}", pk_column.as_str())
                            });
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
                        if len != LOOP_PACE {
                            break;
                        }
                    }
                    sink.lock()
                        .await
                        .flush_with_retry(&FlushByOperation::Init)
                        .await;
                    info!(
                        "MySQL数据源初始化完成 {}.{} count: {}",
                        config.connection_url, table_name, count
                    );
                }
            }
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

                match stream.read().await {
                    Ok((_header, data)) => match data {
                        EventData::TableMap(event) => {
                            let table_name = event.table_name;
                            let table_id = event.table_id;
                            let database_name = event.database_name;
                            table_map.insert(table_id, table_name);
                            table_database_map.insert(table_id, database_name);
                        }
                        EventData::WriteRows(event) => {
                            let table_name = table_map.get(&event.table_id).unwrap().as_str();
                            let database_name =
                                table_database_map.get(&event.table_id).unwrap().as_str();
                            if config.is_target_database_and_table(database_name, table_name) {
                                trace!("WriteRows: {}.{}", database_name, table_name);
                                for row in event.rows {
                                    let before: HashMap<String, Value> = HashMap::new();
                                    let after: HashMap<String, Value> =
                                        parse_row(row, &table_name, &mut columns, config, pool)
                                            .await;
                                    let op = Operation::CREATE;
                                    let data_buffer = DataBuffer {
                                        table_name: table_name.to_string(),
                                        before,
                                        after,
                                        op,
                                    };
                                    let plugin_data =
                                        detail_with_plugin(plugins, data_buffer).await;
                                    if plugin_data.is_ok() {
                                        Self::write_record_with_retry(
                                            &mut sink,
                                            &plugin_data.unwrap(),
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                        EventData::DeleteRows(event) => {
                            let table_name = table_map.get(&event.table_id).unwrap().as_str();
                            let database_name =
                                table_database_map.get(&event.table_id).unwrap().as_str();
                            if config.is_target_database_and_table(database_name, table_name) {
                                trace!("DeleteRows: {}.{}", database_name, table_name);
                                for row in event.rows {
                                    let before: HashMap<String, Value> =
                                        parse_row(row, &table_name, &mut columns, config, pool)
                                            .await;
                                    let after: HashMap<String, Value> = HashMap::new();
                                    let op = Operation::DELETE;
                                    let data_buffer = DataBuffer {
                                        table_name: table_name.to_string(),
                                        before,
                                        after,
                                        op,
                                    };
                                    let plugin_data =
                                        detail_with_plugin(plugins, data_buffer).await;
                                    if plugin_data.is_ok() {
                                        Self::write_record_with_retry(
                                            &mut sink,
                                            &plugin_data.unwrap(),
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                        EventData::UpdateRows(event) => {
                            let table_name = table_map.get(&event.table_id).unwrap().as_str();
                            let database_name =
                                table_database_map.get(&event.table_id).unwrap().as_str();
                            if config.is_target_database_and_table(database_name, table_name) {
                                trace!("UpdateRows: {}.{}", database_name, table_name);
                                for (b, a) in event.rows {
                                    let before: HashMap<String, Value> =
                                        parse_row(b, &table_name, &mut columns, config, pool).await;
                                    let after: HashMap<String, Value> =
                                        parse_row(a, &table_name, &mut columns, config, pool).await;
                                    let op = Operation::UPDATE;
                                    let data_buffer = DataBuffer {
                                        table_name: table_name.to_string(),
                                        before,
                                        after,
                                        op,
                                    };
                                    let plugin_data =
                                        detail_with_plugin(plugins, data_buffer).await;
                                    if plugin_data.is_ok() {
                                        Self::write_record_with_retry(
                                            &mut sink,
                                            &plugin_data.unwrap(),
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    Err(e) => {
                        // 打印错误信息，并且继续监听
                        error!("Error: {}", e);
                    }
                }
            }
        }
    }

    async fn add_plugins(&mut self, plugin: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>>) {
        self.plugins = plugin;
    }

    async fn get_table_info(&mut self) -> Vec<TableInfoVo> {
        self.mysql_source[0].table_info_list.clone()
    }
}

async fn parse_row(
    row: RowEvent,
    table_name: &str,
    columns: &mut Mutex<Vec<String>>,
    config: &MysqlSourceConfigDetail,
    pool: &mut Pool<MySql>,
) -> HashMap<String, Value> {
    let mut data: HashMap<String, Value> = HashMap::new();
    let mut index = 0;
    if columns.lock().await.len() != row.column_values.len() {
        let columns_new = config.fill_table_column(&table_name, pool).await;
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
                let value: Value = Value::String(String::from_utf8_lossy(&v).to_string());
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
    data
}
