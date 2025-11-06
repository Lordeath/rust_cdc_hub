use common::{CdcConfig, Sink, SinkType};
use sink_meilisearch::MeiliSearchSink;
use sink_print::PrintSink;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SinkFactory;

impl SinkFactory {
    pub fn create_sink(config: CdcConfig) -> Arc<Mutex<dyn Sink + Send + Sync>> {
        match config.sink_type {
            SinkType::Print => Arc::new(Mutex::new(PrintSink::new(config))),
            SinkType::MeiliSearch => Arc::new(Mutex::new(MeiliSearchSink::new(config))),
        }
    }
}
