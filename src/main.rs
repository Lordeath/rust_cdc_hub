extern crate core;

use common::{CdcConfig, FlushByOperation, Plugin, Sink, Source};
use sink::SinkFactory;
use source::SourceFactory;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

use chrono::Local;
use plugin::PluginFactory;
use std::time::Duration;
use std::{env, fs, process};
use tokio::time::sleep;
use tracing::subscriber::set_global_default;
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

#[tokio::main]
async fn main() {
    // 设置 tracing 日志格式，自动输出文件名、行号和函数名
    let subscriber = FmtSubscriber::builder()
        .with_env_filter("info")
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_timer(CustomTime)
        .finish();

    tracing_log::LogTracer::init().expect("Failed to set logger");
    set_global_default(subscriber).expect("setting default subscriber failed");

    trace!("App 启动");
    debug!("App 启动");
    info!("App 启动");
    warn!("App 启动");
    error!("App 启动");

    let config_path = get_env("CONFIG_PATH");
    let config: CdcConfig = load_config(&config_path).expect("Failed to load config");
    info!("Config Loaded");
    let source: Arc<Mutex<dyn Source>> = SourceFactory::create_source(&config).await;
    add_plugin(&config, &source).await;
    info!("成功创建source");
    let table_info_list = source.lock().await.get_table_info().await;
    let sink = SinkFactory::create_sink(&config, table_info_list).await;
    info!("成功创建sink");
    let _ = sink.lock().await.connect().await.expect("Failed to connect to sink");
    info!("成功连接到sink");
    add_flush_timer(&config, &sink);
    info!("成功增加flush timer");
    let _ = source.lock().await.start(sink.clone()).await.expect("Failed to start source");
    info!("程序结束");
}

async fn add_plugin(config: &CdcConfig, source: &Arc<Mutex<dyn Source>>) {
    if config.plugins.is_some() && !config.clone().plugins.unwrap().is_empty() {
        info!("正在加载插件");
        let mut plugins: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>> = vec![];
        for plugin in config.clone().plugins.unwrap().iter() {
            // info!("正在加载插件 {}", plugin.plugin_type);
            let plugin = PluginFactory::create_plugin(plugin).await;
            plugins.push(plugin);
        }
        source.lock().await.add_plugins(plugins).await;
        info!("成功加载插件");
    } else {
        info!("没有插件需要加载");
    }
}

fn add_flush_timer(config: &CdcConfig, sink: &Arc<Mutex<dyn Sink + Send + Sync>>) {
    let flush_interval_secs = config
        .first_sink("flush_interval_secs")
        .parse::<u64>()
        .unwrap_or(15);
    let sink_for_timer = sink.clone();
    tokio::spawn(async move {
        info!(
            "MeiliSearch Sink Timer started ({}s window).",
            flush_interval_secs
        );
        let timer_interval = Duration::from_secs(flush_interval_secs);

        loop {
            // 等待时间窗口到达
            sleep(timer_interval).await;

            if let Err(e) = sink_for_timer
                .lock()
                .await
                .flush(&FlushByOperation::Timer)
                .await
            {
                error!("Automatic flush triggered by timer failed: {}", e)
            }
        }
    });
}

fn get_env(key: &str) -> String {
    match env::var(key) {
        Ok(val) => val,
        Err(_) => {
            error!("缺少环境变量 {}", key);
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

struct CustomTime;

impl FormatTime for CustomTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        // 将时间格式化为所需的日期和时间格式
        let datetime = Local::now().format("%Y-%m-%d %H:%M:%S%.6f");
        write!(w, "{}", datetime)
    }
}
