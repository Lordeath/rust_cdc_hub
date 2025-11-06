use common::{CdcConfig, DataBuffer, Sink};
use std::error::Error;
use std::sync::{Arc, Mutex};
use meilisearch_sdk::client::Client;

pub struct MeiliSearchSink {
    meili_url: String,
    meili_master_key: String,
    // client: Arc<Client>,
    client: Client,
    meili_table_name: String,
    meili_table_pk: String,
}

impl MeiliSearchSink {
    pub fn new(config: CdcConfig) -> Self {
        let meili_url = config.get("meili_url");
        let meili_master_key = config.get("meili_master_key");
        let meili_table_name = config.get("meili_table_name");
        let meili_table_pk = config.get("meili_table_pk");

        // Create a client (without sending any request so that can't fail)
        let client = Client::new(meili_url.as_str(), Some(meili_master_key.as_str())).unwrap();

        MeiliSearchSink {
            meili_url,
            meili_master_key,
            // client: Arc::new(client),
            client,
            meili_table_name,
            meili_table_pk,
        }
    }
}

impl Sink for MeiliSearchSink {
    fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        // self.client.health().await
        // async {
        //     self.client.health().await.unwrap().status;
        // }.
        Ok(())
    }

    fn write_record(&mut self, record: &DataBuffer) -> Result<(), Box<dyn Error>> {
        println!("进入print");
        println!("{:?}", record);

        Ok(())
    }
}
