use async_trait::async_trait;
use common::checkpoint_manager::CheckpointServiceHandle;
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::{CdcConfig, DataBuffer, FlushByOperation, Operation, Sink, TableInfoVo};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::task_info::TaskInfo;
use std::collections::HashMap;
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
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
    checkpoint_service: CheckpointServiceHandle,
}

impl MeiliSearchSink {
    pub async fn new(
        config: &CdcConfig,
        _table_info_list: Vec<TableInfoVo>,
        checkpoint_service: CheckpointServiceHandle,
    ) -> Self {
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
            checkpoint: Mutex::new(HashMap::new()),
            checkpoint_service,
        }
    }

    fn connection_summary(&self) -> String {
        format!(
            "meili_url: {}, meili_master_key: {}, meili_table_name: {}, meili_table_pk: {}",
            self.meili_url,
            if self.meili_master_key.is_empty() {
                "not configured"
            } else {
                "configured"
            },
            self.meili_table_name,
            self.meili_table_pk
        )
    }

    async fn wait_for_task_success(&self, task_info: TaskInfo) -> Result<(), String> {
        let task = task_info
            .wait_for_completion(&self.client, None, None)
            .await
            .map_err(|e| e.to_string())?;
        if task.is_success() {
            return Ok(());
        }
        if task.is_failure() {
            return Err(task.unwrap_failure().to_string());
        }
        Err(format!("Meilisearch task did not complete: {:?}", task))
    }

    async fn restore_batch(&self, batch: &[DataBuffer]) {
        self.buffer.lock().await.extend_from_slice(batch);
    }
}

#[async_trait]
impl Sink for MeiliSearchSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("{}", self.connection_summary());

        let _ = self
            .client
            .create_index(&self.meili_table_name, Some(&self.meili_table_pk))
            .await;

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
        if buf.len() >= BATCH_SIZE {
            drop(buf);
            self.flush_with_retry(&FlushByOperation::Signal).await;
        }

        Ok(())
    }

    async fn flush(&self, flush_by_operation: &FlushByOperation) -> Result<(), String> {
        let mut buf = self.buffer.lock().await;
        match flush_by_operation {
            FlushByOperation::Timer => {
                info!("Flushing MeiliSearch Sink by timer... {}", buf.len());
            }
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
            FlushByOperation::Cdc => {
                if !buf.is_empty() {
                    info!("Flushing MeiliSearch Sink by cdc... {}", buf.len());
                }
            }
        }

        if buf.is_empty() {
            return Ok(()); // 没数据不写
        }

        // 交换出 buffer（避免长时间锁住）
        let batch = std::mem::take(&mut *buf);
        drop(buf);

        let index = self.client.index(&self.meili_table_name);

        let mut docs = vec![];
        let mut deletes = vec![];
        let mut cache_for_roll_back: Vec<DataBuffer> = vec![];

        for r in batch {
            cache_for_roll_back.push(r.clone());
            match r.op {
                Operation::CREATE(_) | Operation::UPDATE => {
                    docs.push(r.after);
                }
                Operation::DELETE => {
                    let pk = r.before.get(&self.meili_table_pk);
                    if !pk.is_none() {
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
            let field_names = first.raw_keys().cloned().collect::<Vec<_>>();
            let _ = index.set_filterable_attributes(&field_names).await;
            *self.initialized.write().await = true;
        }

        let upsert_result = if docs.is_empty() {
            Ok(())
        } else {
            match index
                .add_or_replace(&docs, Some(&self.meili_table_pk))
                .await
            {
                Ok(task_info) => self.wait_for_task_success(task_info).await,
                Err(e) => Err(e.to_string()),
            }
        };
        if let Err(e) = upsert_result {
            error!("Batch upsert error: {}", e);
            error!("need to do it again: {}", cache_for_roll_back.len());
            self.restore_batch(&cache_for_roll_back).await;
            return Err(e);
        }

        let delete_result = if deletes.is_empty() {
            Ok(())
        } else {
            match index.delete_documents(&deletes).await {
                Ok(task_info) => self.wait_for_task_success(task_info).await,
                Err(e) => Err(e.to_string()),
            }
        };
        if let Err(e) = delete_result {
            error!("Batch delete error: {}", e);
            error!("need to do it again: {}", cache_for_roll_back.len());
            self.restore_batch(&cache_for_roll_back).await;
            return Err(e);
        }

        Ok(())
    }

    async fn alter_flush(&mut self) -> Result<(), String> {
        let entries = {
            let checkpoint = self.checkpoint.lock().await;
            checkpoint
                .iter()
                .map(|(key, cp)| (key.clone(), cp.clone()))
                .collect::<Vec<_>>()
        };
        if entries.is_empty() {
            return Ok(());
        }
        self.checkpoint_service
            .record_table_applied_many(entries)
            .await?;
        self.checkpoint.lock().await.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::case_insensitive_hash_map::CaseInsensitiveHashMap;
    use common::{Operation, Value};
    use meilisearch_sdk::client::Client;
    use mockito::Matcher;
    use std::sync::Arc;

    fn test_sink(base_url: &str, master_key: &str) -> MeiliSearchSink {
        MeiliSearchSink {
            meili_url: base_url.to_string(),
            meili_master_key: master_key.to_string(),
            client: Client::new(base_url, Some(master_key)).unwrap(),
            meili_table_name: "articles".to_string(),
            meili_table_pk: "id".to_string(),
            buffer: Mutex::new(Vec::new()),
            initialized: RwLock::new(true),
            checkpoint: Mutex::new(HashMap::new()),
            checkpoint_service: CheckpointServiceHandle::disabled_for_tests(),
        }
    }

    fn article_record() -> DataBuffer {
        DataBuffer::new(
            "articles".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            CaseInsensitiveHashMap::new(HashMap::from([
                ("id".to_string(), Value::Int64(7)),
                ("title".to_string(), Value::String("demo".to_string())),
            ])),
            Operation::CREATE(false),
            String::new(),
            0,
            0,
        )
    }

    #[test]
    fn connection_summary_does_not_expose_master_key() {
        let sink = test_sink("http://127.0.0.1:7700", "super-secret-key");

        let summary = sink.connection_summary();

        assert!(!summary.contains("super-secret-key"));
    }

    #[tokio::test]
    async fn flush_rejects_failed_meilisearch_task() {
        let mut server = mockito::Server::new_async().await;
        let enqueue = server
            .mock("POST", "/indexes/articles/documents")
            .match_query(Matcher::UrlEncoded(
                "primaryKey".to_string(),
                "id".to_string(),
            ))
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "taskUid": 1,
                    "indexUid": "articles",
                    "status": "enqueued",
                    "type": "documentAdditionOrUpdate",
                    "enqueuedAt": "2026-08-31T00:00:00.000Z"
                }"#,
            )
            .create_async()
            .await;
        let failed_task = server
            .mock("GET", "/tasks/1")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "uid": 1,
                    "indexUid": "articles",
                    "status": "failed",
                    "type": "documentAdditionOrUpdate",
                    "details": {"receivedDocuments": 1, "indexedDocuments": 0},
                    "canceledBy": null,
                    "error": {
                        "message": "missing document id",
                        "code": "missing_document_id",
                        "type": "invalid_request",
                        "link": "https://example.invalid/errors#missing_document_id"
                    },
                    "duration": "PT0.001S",
                    "enqueuedAt": "2026-08-31T00:00:00.000Z",
                    "startedAt": "2026-08-31T00:00:00.001Z",
                    "finishedAt": "2026-08-31T00:00:00.002Z"
                }"#,
            )
            .expect_at_least(1)
            .create_async()
            .await;
        let sink = Arc::new(test_sink(server.url().as_str(), "test-key"));
        sink.buffer.lock().await.push(article_record());

        let result = sink.flush(&FlushByOperation::Signal).await;

        assert!(result.is_err());
        enqueue.assert_async().await;
        failed_task.assert_async().await;
    }
}
