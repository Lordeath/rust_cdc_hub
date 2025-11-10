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
    // ✅ 新增两个字段
    initialized: tokio::sync::RwLock<bool>, // 判断是否第一次 flush（用锁保护并发安全）
}

impl MeiliSearchSink {
    pub fn new(config: CdcConfig) -> Self {
        let meili_url = config.first_sink("meili_url");
        let meili_master_key = config.first_sink("meili_master_key");
        let meili_table_name = config.first_sink("table_name");
        let meili_table_pk = config.first_sink("meili_table_pk");

        // Create a client (without sending any request so that can't fail)
        let client = Client::new(meili_url.as_str(), Some(meili_master_key.as_str())).unwrap();

        MeiliSearchSink {
            meili_url,
            meili_master_key,
            client,
            meili_table_name,
            meili_table_pk,
            initialized: tokio::sync::RwLock::new(false),
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
                    Ok(_) => {
                        if !*self.initialized.read().await {
                            // ✅ 获取字段名
                            let field_names = record.after.keys().cloned().collect::<Vec<_>>();
                            let _ = self
                                .client
                                .index(self.meili_table_name.as_str())
                                .set_filterable_attributes(&field_names)
                                .await;
                            *self.initialized.write().await = true;
                        }
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            Operation::UPDATE => {
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
            Operation::DELETE => {
                // ✅ 取主键字符串
                let pk = record.before.get(&self.meili_table_pk);
                match pk {
                    None => {}
                    Some(pk_value) => {
                        let pk_str = pk_value.to_string();
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
