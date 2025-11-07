use common::{CdcConfig, Source, SourceType};
use std::sync::Arc;

pub struct SourceFactory;

impl SourceFactory {
    pub fn create_source(config: CdcConfig) -> Arc<dyn Source> {
        match config.source_type {
            SourceType::MySQL => Arc::new(source_mysql::MySQLSource::new(config)),
        }
    }
}
