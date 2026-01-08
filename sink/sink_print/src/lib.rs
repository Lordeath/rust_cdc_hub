use async_trait::async_trait;
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::{CdcConfig, DataBuffer, FlushByOperation, Sink, TableInfoVo};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::Mutex;
use tracing::{error, info};

pub struct PrintSink {
    config: CdcConfig,
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
}

impl PrintSink {
    pub fn new(config: &CdcConfig, _table_info_list: Vec<TableInfoVo>) -> Self {
        PrintSink {
            config: config.clone(),
            checkpoint: Mutex::new(HashMap::new()),
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
        let err_messages: Vec<String> = self
            .checkpoint
            .lock()
            .await
            .values()
            .map(|s| {
                match s.save() {
                    Ok(_) => "".to_string(),
                    Err(msg) => {
                        error!("{}", msg);
                        // Err(msg);
                        msg
                    }
                }
            })
            .find(|x| !x.is_empty())
            .into_iter()
            .collect();
        if !err_messages.is_empty() {
            return Err(err_messages.join("\n").to_string());
        }
        Ok(())
    }
}
