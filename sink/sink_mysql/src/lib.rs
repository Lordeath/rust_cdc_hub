use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink};
use meilisearch_sdk::macro_helper::async_trait;
use sqlx::{MySql, MySqlPool, Pool};
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

const BATCH_SIZE: usize = 256;

pub struct MySqlSink {
    pool: Pool<MySql>,
    table_name: String,
    pk_column: String,

    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,

    // 缓存所有字段名（第一批数据会取一次）
    columns_cache: Mutex<Vec<String>>,
}

impl MySqlSink {
    pub async fn new(config: CdcConfig) -> Self {
        let table_name = config.first_sink("table_name");
        let pk_column = config.first_sink("pk_column");
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
        let pool: Pool<MySql> = MySqlPool::connect(&connection_url).await.unwrap();

        MySqlSink {
            pool,
            table_name,
            pk_column,
            buffer: Mutex::new(Vec::with_capacity(BATCH_SIZE)),
            initialized: RwLock::new(false),
            columns_cache: Mutex::new(vec![]),
        }
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
            self.flush(FlushByOperation::Signal).await?;
        }

        Ok(())
    }

    async fn flush(&self, flush_by_operation: FlushByOperation) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;

        match flush_by_operation {
            FlushByOperation::Timer => {info!("Flushing MeiliSearch Sink by timer... {}", buf.len());}
            FlushByOperation::Init => {
                if !buf.is_empty() {
                    info!("Flushing MeiliSearch Sink by init... {}", buf.len());
                }
            }
            FlushByOperation::Signal => {
                if !buf.is_empty() {
                    info!("Flushing MeiliSearch Sink by signal... {}", buf.len());
                }
            }
            FlushByOperation::Retry => {info!("Flushing MeiliSearch Sink by retry... {}", buf.len());}
        }
        if buf.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut *buf);
        drop(buf);

        let mut inserts = vec![];
        let mut deletes: Vec<String> = vec![];

        let pk_name = self.pk_column.as_str();

        for r in batch {
            match r.op {
                Operation::CREATE | Operation::UPDATE => inserts.push(r.after),
                Operation::DELETE => {
                    let pk = r.get_pk(pk_name);
                    if !pk.is_none() {
                        deletes.push(pk.resolve_string());
                    }
                }
                _ => {
                    panic!("unexpected operation {:?}", r.op);
                }
            }
        }

        // 初始化字段名（只做一次）
        if !*self.initialized.read().await {
            if let Some(first) = inserts.first() {
                let mut cols = self.columns_cache.lock().await;
                *cols = first.keys().cloned().collect::<Vec<_>>();
                *self.initialized.write().await = true;
            }
        }

        let columns = { self.columns_cache.lock().await.clone() };

        // ======================================
        //       批量 UPSERT（INSERT ... ON DUP）
        // ======================================
        if !inserts.is_empty() {
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
                .filter(|c| c != pk_name)
                .collect::<Vec<_>>()
                .join(",");

            let sql = format!(
                "INSERT INTO `{}` ({})
                 VALUES {}
                 ON DUPLICATE KEY UPDATE {}",
                self.table_name, cols_str, values_sql, updates_sql
            );

            // info!("sql: {}", sql.clone());

            let mut query = sqlx::query(&sql);

            for row in &inserts {
                for col in &columns {
                    // let value = row.get(col);
                    let v = row.get(col).map(|v| v.resolve_string()).unwrap_or_else(|| "null".to_string());
                    if v == "null" {
                        query = query.bind(None::<String>);
                    } else {
                        query = query.bind(v);
                    }
                }
            }

            if let Err(e) = query.execute(&self.pool).await {
                error!("MySQL batch UPSERT error: {:?}", e);
                return Err(Box::new(e));
            }
        }

        // ======================================
        //             批量 DELETE
        // ======================================
        if !deletes.is_empty() {
            let ph = (0..deletes.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");

            let sql = format!(
                "DELETE FROM `{}` WHERE `{}` IN ({})",
                self.table_name, self.pk_column, ph
            );

            let mut query = sqlx::query(&sql);
            for pk in deletes {
                query = query.bind(pk);
            }

            if let Err(e) = query.execute(&self.pool).await {
                error!("MySQL batch delete error: {:?}", e);
                return Err(Box::new(e));
            }
        }

        Ok(())
    }
}
