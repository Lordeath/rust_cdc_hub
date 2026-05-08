use common::{Plugin, PluginConfig, PluginType};
use plugin_column_in::PluginColumnIn;
use plugin_column_plus::PluginPlus;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct PluginFactory;

impl PluginFactory {
    pub async fn create_plugin(config: &PluginConfig) -> Arc<Mutex<dyn Plugin + Send + Sync>> {
        match config.plugin_type {
            PluginType::ColumnIn => Arc::new(Mutex::new(PluginColumnIn::new(config))),
            PluginType::Plus => Arc::new(Mutex::new(PluginPlus::new(config))),
            // Modified By Codex 20260508 DatabaseSplit 是控制插件，由主程序读取配置，不进入事件处理链
            PluginType::DatabaseSplit => panic!("DatabaseSplit is not an event plugin"),
        }
    }
}
