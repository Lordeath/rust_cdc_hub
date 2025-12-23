use common::{CdcConfig, Sink, SinkType, TableInfoVo};
use sink_meilisearch::MeiliSearchSink;
use sink_mysql::MySqlSink;
use sink_print::PrintSink;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SinkFactory;

impl SinkFactory {
    pub async fn create_sink(
        config: &CdcConfig,
        table_info_list: Vec<TableInfoVo>,
    ) -> Arc<Mutex<dyn Sink + Send + Sync>> {
        match config.sink_type {
            SinkType::Print => Arc::new(Mutex::new(PrintSink::new(config, table_info_list))),
            SinkType::MeiliSearch => {
                Arc::new(Mutex::new(MeiliSearchSink::new(config, table_info_list)))
            }
            SinkType::MySQL => Arc::new(Mutex::new(MySqlSink::new(config, table_info_list).await)),
        }
    }
}
