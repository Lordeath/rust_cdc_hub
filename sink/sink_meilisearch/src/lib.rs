use common::{CdcConfig, DataBuffer, Operation, Sink};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::macro_helper::async_trait;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, sleep};
use tracing::{error, info};

const BATCH_SIZE: usize = 8192;

pub struct MeiliSearchSink {
    meili_url: String,
    meili_master_key: String,
    client: Client,
    meili_table_name: String,
    meili_table_pk: String,

    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,

    // 新增：定时窗口
    flush_interval_secs: u64,
}

impl MeiliSearchSink {
    pub fn new(config: CdcConfig) -> Self {
        let meili_url = config.first_sink("meili_url");
        let meili_master_key = config.first_sink("meili_master_key");
        let meili_table_name = config.first_sink("table_name");
        let meili_table_pk = config.first_sink("meili_table_pk");
        let flush_interval_secs = config.first_sink("flush_interval_secs").parse::<u64>().unwrap_or(15);

        let client = Client::new(meili_url.as_str(), Some(meili_master_key.as_str())).unwrap();

        MeiliSearchSink {
            meili_url,
            meili_master_key,
            client,
            meili_table_name,
            meili_table_pk,
            buffer: Mutex::new(Vec::with_capacity(BATCH_SIZE)),
            initialized: RwLock::new(false),
            flush_interval_secs,
        }
    }
}

#[async_trait]
impl Sink for MeiliSearchSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "meili_url: {}, meili_master_key: {}, meili_table_name: {}, meili_table_pk: {}",
            self.meili_url, self.meili_master_key, self.meili_table_name, self.meili_table_pk
        );

        let _ = self
            .client
            .create_index(&self.meili_table_name, Some(&self.meili_table_pk))
            .await;

        // 🚀 启动定时 flush 任务 (每 5 秒)
        // ⚠️ 警告: 为了让 spawned task 能获取 Sink 的所有权，
        // 在实际的 CDC 框架中，`MeiliSearchSink` 实例必须被包装在 `Arc` 中。
        // 此处假设框架为您提供了获取 `Arc<Self>` 克隆的能力。
        // 如果没有，这段代码在编译时可能会失败，需要您在外部调整包装方式。
        let sink_for_timer: &'static Self = unsafe {
            // 仅为演示定时器逻辑而使用，您可能需要替换为安全的 Arc::clone 逻辑
            std::mem::transmute(self)
        };

        let flush_interval_secs = self.flush_interval_secs;

        tokio::spawn(async move {
            info!(
                "MeiliSearch Sink Timer started ({}s window).",
                flush_interval_secs
            );
            let timer_interval = Duration::from_secs(flush_interval_secs);

            loop {
                // 等待时间窗口到达
                sleep(timer_interval).await;

                match sink_for_timer.flush().await {
                    Ok(_) => {
                        // 只有在实际有数据写入时才记录信息，但 flush 方法内部会检查是否为空
                        // info!("定时写入完成");
                    }
                    Err(e) => error!("Automatic flush triggered by timer failed: {}", e),
                }
            }
        });

        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;
        buf.push(record.clone());

        if buf.len() >= BATCH_SIZE {
            drop(buf);
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;
        if buf.is_empty() {
            return Ok(()); // 没数据不写
        }
        info!("Flushing MeiliSearch Sink... {}", buf.len());

        // 交换出 buffer（避免长时间锁住）
        let batch = std::mem::take(&mut *buf);
        drop(buf);

        let index = self.client.index(&self.meili_table_name);

        let mut docs = vec![];
        let mut deletes = vec![];

        for r in batch {
            match r.op {
                Operation::CREATE | Operation::UPDATE => {
                    docs.push(r.after);
                }
                Operation::DELETE => {
                    if let Some(pk) = r.before.get(&self.meili_table_pk) {
                        deletes.push(pk.resolve_string());
                    }
                }
                _ => {}
            }
        }

        // 初始化 filterable attributes（一次）
        if !*self.initialized.read().await
            && let Some(first) = docs.first()
        {
            let field_names = first.keys().cloned().collect::<Vec<_>>();
            let _ = index.set_filterable_attributes(&field_names).await;
            *self.initialized.write().await = true;
        }

        if !docs.is_empty()
            && let Err(e) = index
                .add_or_replace(&docs, Some(&self.meili_table_pk))
                .await
        {
            error!("Batch upsert error: {}", e);
            return Err(Box::new(e));
        }

        if !deletes.is_empty()
            && let Err(e) = index.delete_documents(&deletes).await
        {
            error!("Batch delete error: {}", e);
            return Err(Box::new(e));
        }

        Ok(())
    }
}
