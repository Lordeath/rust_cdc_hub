use common::checkpoint_manager::CheckpointServiceHandle;
use common::{CdcConfig, Sink, SinkType, TableInfoVo};
use log::info;
use sink_dameng::DamengSink;
use sink_meilisearch::MeiliSearchSink;
use sink_mysql::MySqlSink;
use sink_print::PrintSink;
use sink_starrocks::StarrocksSink;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SinkFactory;

impl SinkFactory {
    pub async fn create_sink(
        config: &CdcConfig,
        table_info_list: Vec<TableInfoVo>,
        checkpoint_service: CheckpointServiceHandle,
    ) -> Arc<Mutex<dyn Sink + Send + Sync>> {
        info!("create sink: {:?}", config.sink_type);
        match config.sink_type {
            SinkType::Print => Arc::new(Mutex::new(
                PrintSink::new(config, table_info_list, checkpoint_service).await,
            )),
            SinkType::MeiliSearch => Arc::new(Mutex::new(
                MeiliSearchSink::new(config, table_info_list, checkpoint_service).await,
            )),
            SinkType::MySQL => Arc::new(Mutex::new(
                MySqlSink::new(config, table_info_list, checkpoint_service).await,
            )),
            SinkType::Starrocks => Arc::new(Mutex::new(
                StarrocksSink::new(config, table_info_list, checkpoint_service).await,
            )),
            SinkType::Dameng => Arc::new(Mutex::new(
                DamengSink::new(config, table_info_list, checkpoint_service).await,
            )),
        }
    }
}
