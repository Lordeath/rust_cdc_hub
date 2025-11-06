use common::{DataBuffer, Sink};
use std::error::Error;

pub struct PrintSink {}

impl PrintSink {
    pub fn new() -> Self {
        PrintSink {}
    }
}

impl Sink for PrintSink {
    fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn write_record(&mut self, record: &DataBuffer) -> Result<(), Box<dyn Error>> {
        println!("进入print");
        println!("{:?}", record);

        Ok(())
    }
}
