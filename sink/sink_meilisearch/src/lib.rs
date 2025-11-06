use common::{CdcConfig, DataBuffer, Sink};
use meilisearch_sdk::client::Client;
use std::error::Error;
use meilisearch_sdk::macro_helper::async_trait;

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

#[async_trait]
impl Sink for MeiliSearchSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!(
            "meili_url: {}, meili_master_key: {}, meili_table_name: {}, meili_table_pk: {}",
            self.meili_url, self.meili_master_key, self.meili_table_name, self.meili_table_pk
        );
        println!("meili_url: {}", self.client.health().await.unwrap().status);
        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("进入print");
        println!("{:?}", record);

        Ok(())
    }
}
