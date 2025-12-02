use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::macro_helper::async_trait;
use std::error::Error;
use tokio::sync::{Mutex, RwLock};
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
}

impl MeiliSearchSink {
    pub fn new(config: CdcConfig) -> Self {
        let meili_url = config.first_sink("meili_url");
        let meili_master_key = config.first_sink("meili_master_key");
        let meili_table_name = config.first_sink("table_name");
        let meili_table_pk = config.first_sink("meili_table_pk");

        let client = Client::new(meili_url.as_str(), Some(meili_master_key.as_str())).unwrap();

        MeiliSearchSink {
            meili_url,
            meili_master_key,
            client,
            meili_table_name,
            meili_table_pk,
            buffer: Mutex::new(Vec::with_capacity(BATCH_SIZE)),
            initialized: RwLock::new(false),
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
            return Ok(()); // 没数据不写
        }
        // info!("Flushing MeiliSearch Sink... {}", buf.len());

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
