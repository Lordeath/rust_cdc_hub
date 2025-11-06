use common::{CdcConfig, DataBuffer, Operation, Sink};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::macro_helper::async_trait;
use std::error::Error;

pub struct MeiliSearchSink {
    meili_url: String,
    meili_master_key: String,
    client: Client,
    meili_table_name: String,
    meili_table_pk: String,
}

impl MeiliSearchSink {
    pub fn new(config: CdcConfig) -> Self {
        let meili_url = config.get("meili_url");
        let meili_master_key = config.get("meili_master_key");
        let meili_table_name = config.get("table_name");
        let meili_table_pk = config.get("meili_table_pk");

        // Create a client (without sending any request so that can't fail)
        let client = Client::new(meili_url.as_str(), Some(meili_master_key.as_str())).unwrap();

        MeiliSearchSink {
            meili_url,
            meili_master_key,
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
        let _ = self
            .client
            .create_index(
                self.meili_table_name.as_str(),
                Some(self.meili_table_pk.as_str()),
            )
            .await;
        let _ = self
            .client
            .index(self.meili_table_name.as_str())
            .set_primary_key(self.meili_table_pk.as_str());
        println!("初始化完毕");
        Ok(())
    }

    async fn write_record(&self, record: &DataBuffer) -> Result<(), Box<dyn Error + Send + Sync>> {
        match record.op {
            Operation::CREATE => {
                let docs = vec![&record.after];
                let result = self
                    .client
                    .index(self.meili_table_name.as_str())
                    .add_or_replace(&docs, Some(self.meili_table_pk.as_str()))
                    .await;
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            Operation::UPDATE => {
                // ✅ 取主键字符串
                let pk = record.after.get(&self.meili_table_pk);
                match pk {
                    None => {}
                    Some(pk_value) => {
                        let pk_str = pk_value.to_raw_string();
                        self.client
                            .index(self.meili_table_name.as_str())
                            .delete_document(pk_str)
                            .await?;
                    }
                }

                let docs = vec![&record.after];
                let result = self
                    .client
                    .index(self.meili_table_name.as_str())
                    .add_or_replace(&docs, None)
                    .await;
                match result {
                    Ok(r) => {
                        r.wait_for_completion(&self.client, None, None).await?;
                        // println!("task_id: {}", r.task_uid);
                        // println!("result json: {}", serde_json::to_string(&r));
                        // println!("update_type: {}", r.update_type.to);
                        // println!("status: {}", r.status);
                        // println!("index_uid: {}", r.index_uid.unwrap_or_else(-1).to_string());

                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            Operation::DELETE => {
                // ✅ 取主键字符串
                let pk = record.before.get(&self.meili_table_pk);
                match pk {
                    None => {}
                    Some(pk_value) => {
                        let pk_str = pk_value.to_raw_string();
                        self.client
                            .index(self.meili_table_name.as_str())
                            .delete_document(pk_str)
                            .await?;
                    }
                }
            }
            _ => {}
        };

        Ok(())
    }

    async fn flush(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // todo!()
        Ok(())
    }
}
