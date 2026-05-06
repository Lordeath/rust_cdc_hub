extern crate core;

use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use common::metrics::APP_RESTART_COUNT;
use common::{CdcConfig, FlushByOperation, Plugin, Sink, Source};
use prometheus::{Encoder, TextEncoder, gather};
use serde_json::json;
use sink::SinkFactory;
use source::SourceFactory;
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

fn ensure_default_backtrace() {
    if env::var_os("RUST_BACKTRACE").is_none() {
        unsafe {
            env::set_var("RUST_BACKTRACE", "1");
        }
    }
}

#[derive(Clone)]
struct UiState {
    started_at: i64,
    config_summary: serde_json::Value,
    last_timer_flush_at: Arc<AtomicI64>,
    timer_flush_count: Arc<AtomicI64>,
    last_source_error: Arc<Mutex<Option<String>>>,
    last_source_restart_at: Arc<AtomicI64>,
    source_restart_count: Arc<AtomicI64>,
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
            "plugin_count": config.plugins.as_ref().map(|plugins| plugins.len()).unwrap_or(0),
            "source_batch_size": config.source_batch_size,
            "sink_batch_size": config.sink_batch_size,
            "checkpoint_file_path": config.checkpoint_file_path.clone(),
            "auto_create_database": config.auto_create_database.unwrap_or(true),
            "auto_create_table": config.auto_create_table.unwrap_or(true),
            "auto_add_column": config.auto_add_column.unwrap_or(true),
            "auto_modify_column": config.auto_modify_column.unwrap_or(true),
            "ui_bind": config.ui_bind.clone(),
            "ui_port": config.ui_port,
        });
        UiState {
            started_at,
            config_summary,
            last_timer_flush_at: Arc::new(AtomicI64::new(0)),
            timer_flush_count: Arc::new(AtomicI64::new(0)),
            last_source_error: Arc::new(Mutex::new(None)),
            last_source_restart_at: Arc::new(AtomicI64::new(0)),
            source_restart_count: Arc::new(AtomicI64::new(0)),
        }
    }
}

async fn ui_health() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

impl UiState {
    async fn snapshot(&self) -> serde_json::Value {
        let now = Utc::now().timestamp();
        let last_source_error = self.last_source_error.lock().await.clone();
        let source_restart_count = self.source_restart_count.load(Ordering::Relaxed);
        let last_timer_flush_at = self.last_timer_flush_at.load(Ordering::Relaxed);
        let timer_flush_count = self.timer_flush_count.load(Ordering::Relaxed);
        let last_source_restart_at = self.last_source_restart_at.load(Ordering::Relaxed);
        let health_status = if last_source_error.is_some() {
            "degraded"
        } else {
            "running"
        };

        json!({
            "status": health_status,
            "now": now,
            "started_at": self.started_at,
            "uptime_seconds": now.saturating_sub(self.started_at),
            "config": self.config_summary,
            "last_timer_flush_at": last_timer_flush_at,
            "timer_flush_count": timer_flush_count,
            "last_source_restart_at": last_source_restart_at,
            "source_restart_count": source_restart_count,
            "last_source_error": last_source_error,
            "links": {
                "dashboard": "/",
                "status": "/status",
                "metrics": "/metrics",
                "health": "/health"
            }
        })
    }
}

async fn ui_status(state: web::Data<UiState>) -> impl Responder {
    HttpResponse::Ok().json(state.snapshot().await)
}

async fn ui_metrics() -> impl Responder {
    let encoder = TextEncoder::new();
    let metric_families = gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(buffer)
}

async fn ui_root() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rust CDC Hub Dashboard</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #07111f;
      --panel: rgba(15, 23, 42, 0.82);
      --panel-strong: rgba(17, 24, 39, 0.96);
      --line: rgba(148, 163, 184, 0.22);
      --muted: #94a3b8;
      --text: #e5eefb;
      --accent: #38bdf8;
      --accent-2: #22c55e;
      --warning: #f97316;
      --danger: #ef4444;
      --shadow: 0 24px 80px rgba(0, 0, 0, 0.34);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.24), transparent 36rem),
        radial-gradient(circle at 80% 20%, rgba(34, 197, 94, 0.16), transparent 28rem),
        linear-gradient(145deg, #020617 0%, var(--bg) 55%, #0f172a 100%);
    }
    a { color: inherit; text-decoration: none; }
    .shell { width: min(1180px, calc(100% - 32px)); margin: 0 auto; padding: 32px 0 42px; }
    .hero {
      display: grid;
      grid-template-columns: 1.5fr 0.9fr;
      gap: 20px;
      align-items: stretch;
      margin-bottom: 20px;
    }
    .card {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(18px);
    }
    .hero-main { padding: 28px; position: relative; overflow: hidden; }
    .hero-main::after {
      content: "";
      position: absolute;
      inset: auto -90px -120px auto;
      width: 260px;
      height: 260px;
      border-radius: 999px;
      background: rgba(56, 189, 248, 0.14);
      filter: blur(4px);
    }
    .eyebrow { color: var(--accent); font-size: 13px; font-weight: 700; letter-spacing: 0.18em; text-transform: uppercase; }
    h1 { margin: 12px 0 8px; font-size: clamp(30px, 5vw, 54px); line-height: 1; }
    .subtitle { margin: 0; max-width: 720px; color: var(--muted); font-size: 16px; line-height: 1.7; }
    .status-pill {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 8px 12px;
      margin-top: 20px;
      border: 1px solid rgba(34, 197, 94, 0.36);
      color: #bbf7d0;
      background: rgba(34, 197, 94, 0.11);
      border-radius: 999px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .06em;
      font-size: 12px;
    }
    .status-pill.degraded { border-color: rgba(249, 115, 22, 0.44); background: rgba(249, 115, 22, 0.14); color: #fed7aa; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--accent-2); box-shadow: 0 0 0 6px rgba(34, 197, 94, 0.12); }
    .degraded .dot { background: var(--warning); box-shadow: 0 0 0 6px rgba(249, 115, 22, 0.12); }
    .hero-side { padding: 22px; display: grid; gap: 14px; }
    .route { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 14px; border: 1px solid var(--line); border-radius: 18px; background: rgba(15, 23, 42, 0.52); }
    .route span { color: var(--muted); font-size: 13px; }
    .route strong { font-size: 15px; }
    .metrics-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 16px; }
    .metric { padding: 20px; }
    .metric .label { color: var(--muted); font-size: 13px; margin-bottom: 10px; }
    .metric .value { font-size: 30px; font-weight: 800; letter-spacing: -0.04em; }
    .metric .hint { margin-top: 8px; color: var(--muted); font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .content-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .section { padding: 22px; }
    .section h2 { margin: 0 0 16px; font-size: 18px; }
    .kv { display: grid; gap: 10px; }
    .kv-row { display: flex; align-items: center; justify-content: space-between; gap: 18px; padding: 11px 0; border-bottom: 1px solid rgba(148, 163, 184, 0.13); }
    .kv-row:last-child { border-bottom: 0; }
    .kv-key { color: var(--muted); }
    .kv-value { text-align: right; font-weight: 700; word-break: break-word; }
    .error-box { min-height: 92px; padding: 14px; border-radius: 16px; background: rgba(2, 6, 23, 0.45); border: 1px solid var(--line); color: #fecaca; white-space: pre-wrap; overflow-wrap: anywhere; }
    .error-box.empty { color: #bbf7d0; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 18px; }
    .btn { display: inline-flex; align-items: center; gap: 8px; border: 1px solid var(--line); border-radius: 999px; padding: 10px 14px; background: rgba(15, 23, 42, 0.8); color: var(--text); font-weight: 700; }
    .btn.primary { border-color: rgba(56, 189, 248, 0.5); color: #bae6fd; }
    pre { margin: 0; max-height: 360px; overflow: auto; padding: 14px; border-radius: 16px; background: #020617; border: 1px solid var(--line); color: #c4b5fd; font-size: 12px; line-height: 1.6; }
    .footer { margin-top: 16px; color: var(--muted); text-align: center; font-size: 12px; }
    @media (max-width: 900px) { .hero, .content-grid { grid-template-columns: 1fr; } .metrics-grid { grid-template-columns: repeat(2, 1fr); } }
    @media (max-width: 560px) { .shell { width: min(100% - 20px, 1180px); padding-top: 18px; } .metrics-grid { grid-template-columns: 1fr; } .hero-main, .hero-side, .section, .metric { padding: 16px; } }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="card hero-main">
        <div class="eyebrow">Change Data Capture</div>
        <h1>Rust CDC Hub</h1>
        <p class="subtitle">实时查看 CDC 作业状态、source/sink 配置摘要、flush 与重启信息。页面每 5 秒自动刷新，也可以手动刷新。</p>
        <div id="statusPill" class="status-pill"><span class="dot"></span><span id="statusText">loading</span></div>
        <div class="actions">
          <button class="btn primary" type="button" onclick="loadStatus()">立即刷新</button>
          <a class="btn" href="/status" target="_blank" rel="noreferrer">JSON 状态</a>
          <a class="btn" href="/metrics" target="_blank" rel="noreferrer">Prometheus 指标</a>
          <a class="btn" href="/health" target="_blank" rel="noreferrer">健康检查</a>
        </div>
      </div>
      <aside class="card hero-side">
        <div class="route"><span>Source</span><strong id="sourceType">-</strong></div>
        <div class="route"><span>Sink</span><strong id="sinkType">-</strong></div>
        <div class="route"><span>自动刷新</span><strong>5s</strong></div>
      </aside>
    </section>

    <section class="metrics-grid">
      <div class="card metric"><div class="label">运行时间</div><div class="value" id="uptime">-</div><div class="hint" id="startedAt">started: -</div></div>
      <div class="card metric"><div class="label">定时 flush</div><div class="value" id="flushCount">0</div><div class="hint" id="lastFlush">last flush: -</div></div>
      <div class="card metric"><div class="label">Source 重启</div><div class="value" id="restartCount">0</div><div class="hint" id="lastRestart">last restart: -</div></div>
      <div class="card metric"><div class="label">配置规模</div><div class="value" id="configScale">-</div><div class="hint" id="pluginCount">plugins: -</div></div>
    </section>

    <section class="content-grid">
      <div class="card section">
        <h2>配置摘要</h2>
        <div class="kv" id="configKv"></div>
      </div>
      <div class="card section">
        <h2>最近错误</h2>
        <div id="errorBox" class="error-box empty">暂无 source 错误</div>
        <div class="actions"><span class="btn">最后更新：<span id="lastUpdated">-</span></span></div>
      </div>
    </section>

    <section class="card section" style="margin-top:16px">
      <h2>原始状态 JSON</h2>
      <pre id="rawJson">loading...</pre>
    </section>
    <div class="footer">Powered by actix-web · rust_cdc_hub</div>
  </main>

<script>
const el = (id) => document.getElementById(id);
function fmtTs(ts) {
  if (!ts) return '-';
  return new Date(ts * 1000).toLocaleString();
}
function fmtDuration(seconds) {
  seconds = Number(seconds || 0);
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}
function setText(id, value) { el(id).textContent = value ?? '-'; }
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[ch]));
}
function kvRow(key, value) {
  const safe = value === null || value === undefined || value === '' ? '-' : value;
  return `<div class="kv-row"><span class="kv-key">${escapeHtml(key)}</span><span class="kv-value">${escapeHtml(safe)}</span></div>`;
}
async function loadStatus() {
  try {
    const response = await fetch('/status', { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const cfg = data.config || {};
    const degraded = data.status !== 'running';
    el('statusPill').classList.toggle('degraded', degraded);
    setText('statusText', data.status || 'unknown');
    setText('sourceType', cfg.source_type);
    setText('sinkType', cfg.sink_type);
    setText('uptime', fmtDuration(data.uptime_seconds));
    setText('startedAt', `started: ${fmtTs(data.started_at)}`);
    setText('flushCount', data.timer_flush_count ?? 0);
    setText('lastFlush', `last flush: ${fmtTs(data.last_timer_flush_at)}`);
    setText('restartCount', data.source_restart_count ?? 0);
    setText('lastRestart', `last restart: ${fmtTs(data.last_source_restart_at)}`);
    setText('configScale', `${cfg.source_count ?? 0} → ${cfg.sink_count ?? 0}`);
    setText('pluginCount', `plugins: ${cfg.plugin_count ?? 0}`);
    el('configKv').innerHTML = [
      kvRow('Source 类型', cfg.source_type),
      kvRow('Sink 类型', cfg.sink_type),
      kvRow('Source 配置数量', cfg.source_count),
      kvRow('Sink 配置数量', cfg.sink_count),
      kvRow('插件数量', cfg.plugin_count),
      kvRow('Source batch size', cfg.source_batch_size),
      kvRow('Sink batch size', cfg.sink_batch_size),
      kvRow('Checkpoint 路径', cfg.checkpoint_file_path),
      kvRow('自动建库', cfg.auto_create_database),
      kvRow('自动建表', cfg.auto_create_table),
      kvRow('自动加字段', cfg.auto_add_column),
      kvRow('自动改字段', cfg.auto_modify_column),
      kvRow('UI bind', cfg.ui_bind),
      kvRow('UI port', cfg.ui_port),
    ].join('');
    const errBox = el('errorBox');
    errBox.textContent = data.last_source_error || '暂无 source 错误';
    errBox.classList.toggle('empty', !data.last_source_error);
    setText('lastUpdated', fmtTs(data.now));
    el('rawJson').textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    el('statusPill').classList.add('degraded');
    setText('statusText', 'ui-error');
    el('errorBox').textContent = `无法加载 /status: ${err.message}`;
    el('errorBox').classList.remove('empty');
  }
}
loadStatus();
setInterval(loadStatus, 5000);
</script>
</body>
</html>"#;

async fn start_ui(ui_state: UiState, bind: String, port: u16) -> Result<(), Box<dyn Error>> {
    let addr = format!("{}:{}", bind, port);
    let listener = std::net::TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    info!("UI listening on http://{}", local_addr);
    let local_port = local_addr.port();
    info!("curl -sS http://127.0.0.1:{}/health", local_port);
    info!("curl -sS http://127.0.0.1:{}/status", local_port);
    info!("curl -sS http://127.0.0.1:{}/metrics", local_port);
    info!("curl -sS http://127.0.0.1:{}/", local_port);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(ui_state.clone()))
            .route("/", web::get().to(ui_root))
            .route("/health", web::get().to(ui_health))
            .route("/status", web::get().to(ui_status))
            .route("/api/status", web::get().to(ui_status))
            .route("/metrics", web::get().to(ui_metrics))
    })
    .listen(listener)?
    .run()
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    ensure_default_backtrace();
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
                let result = actix_web::rt::System::new()
                    .block_on(async move { start_ui(ui_state_for_server, bind, port).await });
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
    let mut sink = SinkFactory::create_sink(&config, table_info_list).await;
    info!("成功创建sink");
    if let Err(e) = sink.lock().await.connect().await {
        error!("Failed to connect to sink: {}", e);
        panic!("Failed to connect to sink: {}", e);
    }
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
            APP_RESTART_COUNT.with_label_values(&["source"]).inc();
            ui_state
                .source_restart_count
                .fetch_add(1, Ordering::Relaxed);
            error!("尝试进行重试 {}: {}", retry_times, e.message);
            // 先关闭旧source释放资源
            source.lock().await.close().await;
            // 关闭sink确保事务被释放
            sink.lock().await.close().await;
            // 添加重试间隔，避免立即重试导致资源耗尽
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            // 重新创建source获取表信息
            source = SourceFactory::create_source(&config).await;
            add_plugin(&config, &source).await;
            let table_info_list = source.lock().await.get_table_info().await;
            // 重新创建sink并连接
            sink = SinkFactory::create_sink(&config, table_info_list).await;
            if let Err(e) = sink.lock().await.connect().await {
                error!("Failed to connect to sink: {}", e);
                panic!("Failed to connect to sink: {}", e);
            }
            add_flush_timer(&config, &sink, ui_state.clone());
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
            ui_state_for_timer
                .timer_flush_count
                .fetch_add(1, Ordering::Relaxed);
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
        assert_eq!(v.get("status").unwrap().as_str().unwrap(), "running");
        assert!(v.get("uptime_seconds").is_some());
        assert!(v.get("last_timer_flush_at").is_some());
        assert!(v.get("timer_flush_count").is_some());
        assert!(v.get("last_source_restart_at").is_some());
        assert!(v.get("source_restart_count").is_some());
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
        assert!(s.contains("Rust CDC Hub"));
        assert!(s.contains("/status"));
        assert!(s.contains("Prometheus 指标"));
        assert!(s.contains("rawJson"));
    }
}
