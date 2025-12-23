use common::{CdcConfig, DataBuffer, Sink, TableInfoVo};
use std::error::Error;
use async_trait::async_trait;
use tracing::info;

pub struct PrintSink {
    config: CdcConfig,
}

impl PrintSink {
    pub fn new(config: &CdcConfig, _table_info_list: Vec<TableInfoVo>) -> Self {
        PrintSink { config: config.clone() }
    }
}

#[async_trait]
impl Sink for PrintSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("{}", self.config.source_config.len());
        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("进入print");
        info!("{:?}", record);

        Ok(())
    }
}
