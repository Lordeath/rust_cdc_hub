extern crate core;

use common::{CdcConfig, FlushByOperation, Plugin, Sink, Source};
use sink::SinkFactory;
use source::SourceFactory;
use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use serde_json::json;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::Mutex;

use chrono::Local;
use chrono::Utc;
use plugin::PluginFactory;
use std::time::Duration;
use std::{env, fs, process};
use tokio::time::sleep;
use tracing::subscriber::set_global_default;
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

fn parse_port(s: &str) -> Option<u16> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    t.parse::<u16>().ok()
}

fn ui_port_from_env() -> Option<u16> {
    env::var("UI_PORT")
        .ok()
        .and_then(|s| parse_port(&s))
        .filter(|p| *p > 0)
        .or_else(|| {
            env::var("PORT")
                .ok()
                .and_then(|s| parse_port(&s))
                .filter(|p| *p > 0)
        })
}

fn resolve_ui_port(config: &CdcConfig, env_port: Option<u16>) -> u16 {
    if let Some(p) = env_port {
        return p;
    }
    config.ui_port.unwrap_or(0)
}

#[derive(Clone)]
struct UiState {
    started_at: i64,
    config_summary: serde_json::Value,
    last_timer_flush_at: Arc<AtomicI64>,
    last_source_error: Arc<Mutex<Option<String>>>,
    last_source_restart_at: Arc<AtomicI64>,
}

impl UiState {
    fn new(config: &CdcConfig) -> Self {
        Self::new_with_started_at(config, Utc::now().timestamp())
    }
    fn new_with_started_at(config: &CdcConfig, started_at: i64) -> Self {
        let config_summary = json!({
            "source_type": format!("{}", config.source_type),
            "sink_type": format!("{}", config.sink_type),
            "source_count": config.source_config.len(),
            "sink_count": config.sink_config.len(),
        });
        UiState {
            started_at,
            config_summary,
            last_timer_flush_at: Arc::new(AtomicI64::new(0)),
            last_source_error: Arc::new(Mutex::new(None)),
            last_source_restart_at: Arc::new(AtomicI64::new(0)),
        }
    }
}

async fn ui_health() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

async fn ui_status(state: web::Data<UiState>) -> impl Responder {
    let last_source_error = state.last_source_error.lock().await.clone();
    HttpResponse::Ok().json(json!({
        "started_at": state.started_at,
        "config": state.config_summary,
        "last_timer_flush_at": state.last_timer_flush_at.load(Ordering::Relaxed),
        "last_source_restart_at": state.last_source_restart_at.load(Ordering::Relaxed),
        "last_source_error": last_source_error,
    }))
}

async fn ui_root(state: web::Data<UiState>) -> impl Responder {
    let v = json!({
        "started_at": state.started_at,
        "config": state.config_summary,
        "last_timer_flush_at": state.last_timer_flush_at.load(Ordering::Relaxed),
        "last_source_restart_at": state.last_source_restart_at.load(Ordering::Relaxed),
        "last_source_error": state.last_source_error.lock().await.clone(),
    });
    let body = format!(
        "<html><head><meta charset=\"utf-8\"></head><body><pre>{}</pre></body></html>",
        serde_json::to_string_pretty(&v).unwrap_or_else(|_| "{}".to_string())
    );
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(body)
}

async fn start_ui(ui_state: UiState, bind: String, port: u16) -> Result<(), Box<dyn Error>> {
    let addr = format!("{}:{}", bind, port);
    let listener = std::net::TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    info!("UI listening on http://{}", local_addr);
    let local_port = local_addr.port();
    info!("curl -sS http://127.0.0.1:{}/health", local_port);
    info!("curl -sS http://127.0.0.1:{}/status", local_port);
    info!("curl -sS http://127.0.0.1:{}/", local_port);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(ui_state.clone()))
            .route("/", web::get().to(ui_root))
            .route("/health", web::get().to(ui_health))
            .route("/status", web::get().to(ui_status))
    })
    .listen(listener)?
    .run()
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let config_path = get_env("CONFIG_PATH");
    let config: CdcConfig = load_config(&config_path).expect("Failed to load config");
    let log_level = config.log_level.clone().unwrap_or("info".to_string());

    // 设置 tracing 日志格式，自动输出文件名、行号和函数名
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(log_level)
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_names(true)
        .with_timer(CustomTime)
        .finish();

    tracing_log::LogTracer::init().expect("Failed to set logger");
    set_global_default(subscriber).expect("setting default subscriber failed");

    trace!("App 启动");
    debug!("App 启动");
    info!("App 启动");
    warn!("App 启动");
    error!("App 启动");

    info!("Config Loaded");
    let ui_state = UiState::new(&config);
    if config.enable_ui.unwrap_or(true) {
        let bind = config.ui_bind.clone().unwrap_or("0.0.0.0".to_string());
        let port = resolve_ui_port(&config, ui_port_from_env());
        let ui_state_for_server = ui_state.clone();
        std::thread::Builder::new()
            .name("ui-server".to_string())
            .spawn(move || {
            let result = actix_web::rt::System::new().block_on(async move {
                start_ui(ui_state_for_server, bind, port).await
            });
            if let Err(e) = result {
                error!("UI server error: {}", e);
            }
        })
        .expect("spawn ui-server thread failed");
    }
    let mut source: Arc<Mutex<dyn Source>> = SourceFactory::create_source(&config).await;
    add_plugin(&config, &source).await;
    info!("成功创建source");
    let table_info_list = source.lock().await.get_table_info().await;
    let sink = SinkFactory::create_sink(&config, table_info_list).await;
    info!("成功创建sink");
    sink.lock()
        .await
        .connect()
        .await
        .expect("Failed to connect to sink");
    info!("成功连接到sink");
    add_flush_timer(&config, &sink, ui_state.clone());
    info!("成功增加flush timer");
    let mut retry_times = 0;
    loop {
        let start_result = source.lock().await.start(sink.clone()).await;
        if let Err(e) = start_result {
            retry_times += 1;
            if retry_times >= 30 {
                error!("重试次数过多，程序退出: {}", retry_times);
                break;
            }
            *ui_state.last_source_error.lock().await = Some(e.message.clone());
            ui_state
                .last_source_restart_at
                .store(Utc::now().timestamp(), Ordering::Relaxed);
            error!("尝试进行重试 {}: {}", retry_times, e.message);
            source = SourceFactory::create_source(&config).await;
            add_plugin(&config, &source).await;
        } else {
            break;
        }
    }
    info!("程序结束");
}

async fn add_plugin(config: &CdcConfig, source: &Arc<Mutex<dyn Source>>) {
    if config.plugins.is_some()
        && !config
            .clone()
            .plugins
            .unwrap_or_else(|| panic!("plugins not found"))
            .is_empty()
    {
        info!("正在加载插件");
        let mut plugins: Vec<Arc<Mutex<dyn Plugin + Send + Sync>>> = vec![];
        for plugin in config
            .clone()
            .plugins
            .unwrap_or_else(|| panic!("plugins not found"))
            .iter()
        {
            info!("正在加载插件 {}", plugin.plugin_type);
            let plugin = PluginFactory::create_plugin(plugin).await;
            plugins.push(plugin);
        }
        source.lock().await.add_plugins(plugins).await;
        info!("成功加载插件");
    } else {
        info!("没有插件需要加载");
    }
}

fn add_flush_timer(
    config: &CdcConfig,
    sink: &Arc<Mutex<dyn Sink + Send + Sync>>,
    ui_state: UiState,
) {
    let flush_interval_secs = config
        .first_sink("flush_interval_secs")
        .parse::<u64>()
        .unwrap_or(1);
    let sink_for_timer = sink.clone();
    let ui_state_for_timer = ui_state.clone();
    tokio::spawn(async move {
        info!("Sink Timer started ({}s window).", flush_interval_secs);
        let timer_interval = Duration::from_secs(flush_interval_secs);

        loop {
            // 等待时间窗口到达
            sleep(timer_interval).await;
            ui_state_for_timer
                .last_timer_flush_at
                .store(Utc::now().timestamp(), Ordering::Relaxed);
            sink_for_timer
                .lock()
                .await
                .flush_with_retry(&FlushByOperation::Timer)
                .await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{body::to_bytes, http::StatusCode, test as actix_test};

    fn test_config() -> CdcConfig {
        serde_yaml::from_str(
            r#"
source_type: MySQL
sink_type: Print
source_config:
  - {}
sink_config:
  - {}
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_resolve_ui_port_env_overrides() {
        let cfg = test_config();
        assert_eq!(resolve_ui_port(&cfg, Some(18080)), 18080);
    }

    #[test]
    fn test_resolve_ui_port_default_is_ephemeral() {
        let cfg = test_config();
        assert_eq!(resolve_ui_port(&cfg, None), 0);
    }

    #[actix_web::test]
    async fn test_ui_health_ok() {
        let cfg = test_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/health", web::get().to(ui_health)),
        )
        .await;

        let req = actix_test::TestRequest::get().uri("/health").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(body, "ok");
    }

    #[actix_web::test]
    async fn test_ui_status_contains_fields() {
        let cfg = test_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/status", web::get().to(ui_status)),
        )
        .await;

        let req = actix_test::TestRequest::get().uri("/status").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.get("started_at").unwrap().as_i64().unwrap(), 123);
        assert!(v.get("config").is_some());
        assert!(v.get("last_timer_flush_at").is_some());
        assert!(v.get("last_source_restart_at").is_some());
        assert!(v.get("last_source_error").is_some());
    }

    #[actix_web::test]
    async fn test_ui_root_html() {
        let cfg = test_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/", web::get().to(ui_root)),
        )
        .await;

        let req = actix_test::TestRequest::get().uri("/").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("<pre>"));
        assert!(s.contains("\"started_at\": 123"));
    }
}
