extern crate core;

use common::{CdcConfig, Source};
use sink::SinkFactory;
use source::SourceFactory;
use std::error::Error;
use std::sync::Arc;
use std::{env, fs, process};

#[tokio::main]
async fn main() {
    let config_path = get_env("CONFIG_PATH");
    let config: CdcConfig = load_config(&config_path).expect("Failed to load config");

    let source: Arc<dyn Source> = SourceFactory::create_source(config.clone()).await;
    let sink = SinkFactory::create_sink(config.clone()).await;
    let _ = sink.lock().await.connect().await;
    let _ = source.start(sink).await;
}

fn get_env(key: &str) -> String {
    match env::var(key) {
        Ok(val) => val,
        Err(_) => {
            eprintln!("缺少环境变量 {}", key);
            process::exit(1); // 优雅退出，返回码1
        }
    }
}

pub fn load_config(path: &str) -> Result<CdcConfig, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;

    if path.ends_with(".yaml") || path.ends_with(".yml") {
        let cfg: CdcConfig = serde_yaml::from_str(&content)?;
        Ok(cfg)
    } else if path.ends_with(".json") {
        let cfg: CdcConfig = serde_json::from_str(&content)?;
        Ok(cfg)
    } else {
        Err("Unsupported config file format".into())
    }
}
