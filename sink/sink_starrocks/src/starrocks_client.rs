use serde_json::json;
use std::error::Error;
use tracing::{error, info, trace};

#[derive(Debug, Clone)]
pub struct StarrocksClient {
    pub client: reqwest::Client,
    pub base_url: String,
    pub username: String,
    pub password: String,
}
impl StarrocksClient {
    pub async fn new(base_url: &str, username: &str, password: &str) -> Self {
        StarrocksClient {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Failed to create reqwest client"),
            base_url: base_url.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    /// 核心的 Stream Load 调用方法
    pub async fn stream_load(
        &self,
        database: &str,
        table: &str,
        body: String,
        columns: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 构造请求 URL
        let url = format!("{}{}/{}/_stream_load", self.base_url, database, table);
        trace!(
            "curl -L -X PUT {}  -u {}:{}  -H 'format: JSON' -H 'Expect: 100-continue' -H 'strip_outer_array: true' -H 'columns: {}' -d '{}'",
            url, &self.username, &self.password, columns, body
        );

        // 发起 PUT 请求
        let resp = self
            .client
            .put(&url)
            // Basic Auth
            .basic_auth(self.username.clone(), Some(self.password.clone()))
            // .header("Authorization", &self.auth_header)
            // 告诉 StarRocks 数据格式是 JSON
            .header("format", "JSON")
            .header("columns", columns.clone())
            // 如果发送的是 JSON 数组则需要 strip outer array
            .header("strip_outer_array", "true")
            // 推荐这个 Header，避免不必要的数据传输
            .header("Expect", "100-continue")
            // 发送请求体
            .body(body.clone())
            .send()
            .await?;

        if resp.status().is_redirection() {
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .ok_or("redirect without location")?
                .to_str()?;

            let resp = self
                .client
                .put(location)
                .basic_auth(self.username.clone(), Some(self.password.clone()))
                .header("format", "JSON")
                .header("strip_outer_array", "true")
                .header("columns", columns)
                .body(body)
                .send()
                .await?;

            // 读取响应文本
            let text = resp.text().await?;

            // 检查是否成功
            return if text.contains(r#""Status":"Success""#) || text.contains(r#""Status": "Success""#) {
                Ok(())
            } else {
                Err(format!("StarRocks Stream Load failed: {}", text).into())
            }
        }

        // 读取响应文本
        let text = resp.text().await?;

        // 检查是否成功
        if text.contains(r#""Status":"Success""#) {
            Ok(())
        } else {
            Err(format!("StarRocks Stream Load failed: {}", text).into())
        }
    }

    pub async fn execute_sql(&self, database: &str, sql: &str) -> String {
        let url = format!(
            "{}v1/catalogs/default_catalog/databases/{}/sql",
            self.base_url, database,
        );
        let body = json!({
            "query": sql
        });
        info!("url: {}", url);
        info!("body: {}", &body);
        let resp = self
            .client
            .post(url)
            .basic_auth(self.username.clone(), Some(self.password.clone()))
            // .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .expect("Failed to execute SQL");
        // 读取响应文本
        if resp.status() != 200 {
            error!(
                "Failed to execute SQL: {}",
                resp.text()
                    .await
                    .unwrap_or("Failed to read response text".to_string())
            );
            return "".to_string();
        }
        let text = resp.text().await.expect("Failed to read response text");
        info!("text: {}", text);
        text
    }
}
