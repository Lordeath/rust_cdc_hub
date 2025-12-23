use common::{CdcConfig, Source, SourceType};
use std::sync::{Arc};
use tokio::sync::Mutex;

pub struct SourceFactory;

impl SourceFactory {
    pub async fn create_source(config: &CdcConfig) -> Arc<Mutex<dyn Source>> {
        match config.source_type {
            SourceType::MySQL => Arc::new(Mutex::new(source_mysql::MySQLSource::new(config).await)),
        }
    }
}
