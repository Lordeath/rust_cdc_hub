use async_trait::async_trait;
use common::checkpoint_manager::CheckpointServiceHandle;
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::{CdcConfig, DataBuffer, FlushByOperation, Sink, TableInfoVo};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::Mutex;
use tracing::info;

pub struct PrintSink {
    config: CdcConfig,
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
    checkpoint_service: CheckpointServiceHandle,
}

impl PrintSink {
    pub async fn new(
        config: &CdcConfig,
        _table_info_list: Vec<TableInfoVo>,
        checkpoint_service: CheckpointServiceHandle,
    ) -> Self {
        PrintSink {
            config: config.clone(),
            checkpoint: Mutex::new(HashMap::new()),
            checkpoint_service,
        }
    }
}

#[async_trait]
impl Sink for PrintSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("{}", self.config.source_config.len());
        Ok(())
    }

    async fn write_record(
        &mut self,
        record: &DataBuffer,
        mysql_check_point_detail_entity: &Option<MysqlCheckPointDetailEntity>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("进入print");
        info!("{:?}", record);
        if let Some(s) = mysql_check_point_detail_entity {
            self.checkpoint
                .lock()
                .await
                .insert(s.checkpoint_filepath.to_string(), s.clone());
        }
        Ok(())
    }

    async fn flush(&self, _from_timer: &FlushByOperation) -> Result<(), String> {
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
