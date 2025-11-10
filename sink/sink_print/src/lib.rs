use common::{CdcConfig, DataBuffer, Sink};
use std::error::Error;
use async_trait::async_trait;

pub struct PrintSink {
    config: CdcConfig,
}

impl PrintSink {
    pub fn new(config: CdcConfig) -> Self {
        PrintSink { config }
    }
}

#[async_trait]
impl Sink for PrintSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("{}", self.config.source_config.len());
        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("进入print");
        println!("{:?}", record);

        Ok(())
    }
}
