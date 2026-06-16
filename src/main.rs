extern crate core;

use actix_web::{App, HttpResponse, HttpServer, Responder, http::header, web};
use common::metrics::APP_RESTART_COUNT;
use common::runtime_progress;
use common::{
    CdcConfig, FlushByOperation, Plugin, PluginConfig, PluginType, Sink, Source, TableInfoVo,
    get_mysql_pool_by_url, mysql_connection_url_from_config,
};
use prometheus::{Encoder, TextEncoder, gather};
use serde_json::json;
use sink::SinkFactory;
use source::SourceFactory;
use sqlx::{MySql, Pool, Row};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, Pid, ProcessRefreshKind, RefreshKind, System};
use tokio::sync::Mutex;

use chrono::Local;
use chrono::Utc;
use plugin::PluginFactory;
use std::time::Duration;
use std::{env, fs, io, process};
use tokio::time::sleep;
use tracing::subscriber::set_global_default;
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

const DEFAULT_UI_PORT: u16 = 18088;

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

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
    config.ui_port.unwrap_or(DEFAULT_UI_PORT)
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
    config: CdcConfig,
    table_info_list: Arc<Mutex<Vec<TableInfoVo>>>,
    config_summary: serde_json::Value,
    database_split: serde_json::Value,
    column_in_enabled: bool,
    last_timer_flush_at: Arc<AtomicI64>,
    timer_flush_count: Arc<AtomicI64>,
    last_source_error: Arc<Mutex<Option<String>>>,
    last_source_restart_at: Arc<AtomicI64>,
    source_restart_count: Arc<AtomicI64>,
    system_monitor: Arc<Mutex<System>>,
    process_pid: u32,
}

impl UiState {
    fn new(config: &CdcConfig) -> Self {
        Self::new_with_started_at(config, Utc::now().timestamp())
    }
    fn new_with_started_at(config: &CdcConfig, started_at: i64) -> Self {
        let database_split = database_split_summary(config);
        let column_in_enabled = plugin_config(config, PluginType::ColumnIn).is_some();
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
            "database_split": database_split.clone(),
            "plugins": plugin_summary(config),
        });
        UiState {
            started_at,
            config: config.clone(),
            table_info_list: Arc::new(Mutex::new(Vec::new())),
            config_summary,
            database_split,
            column_in_enabled,
            last_timer_flush_at: Arc::new(AtomicI64::new(0)),
            timer_flush_count: Arc::new(AtomicI64::new(0)),
            last_source_error: Arc::new(Mutex::new(None)),
            last_source_restart_at: Arc::new(AtomicI64::new(0)),
            source_restart_count: Arc::new(AtomicI64::new(0)),
            system_monitor: Arc::new(Mutex::new(System::new_with_specifics(
                RefreshKind::new()
                    .with_memory(MemoryRefreshKind::everything())
                    .with_cpu(CpuRefreshKind::everything()),
            ))),
            process_pid: process::id(),
        }
    }

    async fn set_table_info_list(&self, table_info_list: Vec<TableInfoVo>) {
        *self.table_info_list.lock().await = table_info_list;
    }

    async fn resource_snapshot(&self) -> serde_json::Value {
        let mut system = self.system_monitor.lock().await;
        system.refresh_memory();
        system.refresh_cpu();
        let pid = Pid::from_u32(self.process_pid);
        system.refresh_process_specifics(pid, ProcessRefreshKind::new().with_cpu().with_memory());
        let process = system.process(pid);
        let total_memory_bytes = system.total_memory();
        let used_memory_bytes = system.used_memory();
        let total_swap_bytes = system.total_swap();
        let used_swap_bytes = system.used_swap();
        let system_cpu_percent = system.global_cpu_info().cpu_usage();
        let memory_percent = if total_memory_bytes == 0 {
            0.0
        } else {
            used_memory_bytes as f64 * 100.0 / total_memory_bytes as f64
        };
        json!({
            "system": {
                "cpu_percent": system_cpu_percent,
                "memory_total_bytes": total_memory_bytes,
                "memory_used_bytes": used_memory_bytes,
                "memory_percent": memory_percent,
                "swap_total_bytes": total_swap_bytes,
                "swap_used_bytes": used_swap_bytes,
            },
            "process": {
                "pid": self.process_pid,
                "cpu_percent": process.map(|p| p.cpu_usage()).unwrap_or(0.0),
                "memory_bytes": process.map(|p| p.memory()).unwrap_or(0),
                "virtual_memory_bytes": process.map(|p| p.virtual_memory()).unwrap_or(0),
                "run_time_seconds": process.map(|p| p.run_time()).unwrap_or(0),
            }
        })
    }
}

#[cfg(target_os = "linux")]
fn trim_allocator_memory() -> serde_json::Value {
    let result = unsafe { malloc_trim(0) };
    json!({
        "supported": true,
        "allocator": "glibc",
        "result": result,
        "trimmed": result == 1,
    })
}

#[cfg(not(target_os = "linux"))]
fn trim_allocator_memory() -> serde_json::Value {
    json!({
        "supported": false,
        "allocator": "unsupported",
        "result": 0,
        "trimmed": false,
    })
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
        let runtime_progress = runtime_progress::snapshot().await;
        let resources = self.resource_snapshot().await;
        let health_status = if last_source_error.is_some() {
            "degraded"
        } else if runtime_progress.initializing {
            "initializing"
        } else {
            "running"
        };
        let split_snapshot = self
            .split_snapshot_with_progress(runtime_progress.clone())
            .await;

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
            "runtime_progress": runtime_progress,
            "resources": resources,
            "split_mode": split_snapshot.clone(),
            "database_split": split_snapshot.clone(),
            "links": {
                "dashboard": ".",
                "status": "status",
                "metrics": "metrics",
                "health": "health",
                "split": if self.database_split_enabled() { "split" } else { "" }
            }
        })
    }

    fn database_split_enabled(&self) -> bool {
        self.database_split
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    async fn split_snapshot(&self) -> serde_json::Value {
        self.split_snapshot_with_progress(runtime_progress::snapshot().await)
            .await
    }

    async fn split_snapshot_with_progress(
        &self,
        runtime_progress: runtime_progress::RuntimeProgress,
    ) -> serde_json::Value {
        let mut split = self.database_split.clone();
        if let Some(obj) = split.as_object_mut() {
            obj.insert("runtime_progress".to_string(), json!(runtime_progress));
            obj.insert("resources".to_string(), self.resource_snapshot().await);
            obj.insert(
                "cleanup".to_string(),
                cleanup_summary_json(&self.config, &self.table_info_list.lock().await),
            );
        }
        split
    }
}

async fn ui_status(state: web::Data<UiState>) -> impl Responder {
    HttpResponse::Ok().json(state.snapshot().await)
}

async fn ui_memory_trim(state: web::Data<UiState>) -> impl Responder {
    let before = state.resource_snapshot().await;
    let trim = trim_allocator_memory();
    let after = state.resource_snapshot().await;
    HttpResponse::Ok().json(json!({
        "status": "ok",
        "allocator_trim": trim,
        "before": before,
        "after": after,
    }))
}

async fn ui_split_status(state: web::Data<UiState>) -> impl Responder {
    if !state.database_split_enabled() {
        return HttpResponse::NotFound().json(json!({
            "enabled": false,
            "message": "DatabaseSplit plugin is not enabled"
        }));
    }
    HttpResponse::Ok().json(state.split_snapshot().await)
}

async fn mysql_pool_for_cleanup(config: &CdcConfig, source: bool) -> Result<Pool<MySql>, String> {
    get_mysql_pool_by_url(
        &mysql_connection_url(config, source),
        if source {
            "database split cleanup source"
        } else {
            "database split cleanup sink"
        },
    )
    .await
}

async fn cleanup_count(pool: &Pool<MySql>, target: &CleanupTarget) -> Result<i64, String> {
    let placeholders = vec!["?"; target.filter_values.len()].join(", ");
    let sql = format!(
        "SELECT COUNT(*) AS cnt FROM {} WHERE {} IN ({})",
        quoted_identifier(&target.table_name),
        quoted_identifier(&target.filter_column),
        placeholders
    );
    let mut query = sqlx::query(&sql);
    for value in &target.filter_values {
        query = query.bind(value);
    }
    let row = query.fetch_one(pool).await.map_err(|e| e.to_string())?;
    row.try_get::<i64, _>("cnt").map_err(|e| e.to_string())
}

async fn cleanup_pk_values(
    pool: &Pool<MySql>,
    target: &CleanupTarget,
) -> Result<Vec<String>, String> {
    let placeholders = vec!["?"; target.filter_values.len()].join(", ");
    let sql = format!(
        "SELECT {} AS pk_value FROM {} WHERE {} IN ({}) ORDER BY {}",
        quoted_identifier(&target.pk_column),
        quoted_identifier(&target.table_name),
        quoted_identifier(&target.filter_column),
        placeholders,
        quoted_identifier(&target.pk_column)
    );
    let mut query = sqlx::query(&sql);
    for value in &target.filter_values {
        query = query.bind(value);
    }
    let rows = query.fetch_all(pool).await.map_err(|e| e.to_string())?;
    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("pk_value")
                .or_else(|_| row.try_get::<i64, _>("pk_value").map(|v| v.to_string()))
                .or_else(|_| row.try_get::<u64, _>("pk_value").map(|v| v.to_string()))
                .or_else(|_| row.try_get::<i32, _>("pk_value").map(|v| v.to_string()))
                .or_else(|_| row.try_get::<u32, _>("pk_value").map(|v| v.to_string()))
                .map_err(|e| e.to_string())
        })
        .collect()
}

async fn ui_split_cleanup_dry_run(
    state: web::Data<UiState>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    if !state.database_split_enabled() {
        return HttpResponse::NotFound().json(json!({
            "enabled": false,
            "message": "DatabaseSplit plugin is not enabled"
        }));
    }
    let (targets, invalid_tables) =
        cleanup_targets(&state.config, &state.table_info_list.lock().await);
    let targets = filter_targets_by_request(targets, &request_tables(&body));
    if targets.is_empty() {
        return HttpResponse::BadRequest().json(json!({
            "message": "no cleanup tables available",
            "invalid_tables": invalid_tables.iter().map(|item| json!({
                "table_name": item.table_name,
                "reason": item.reason,
                "matched_columns": item.matched_columns,
            })).collect::<Vec<_>>()
        }));
    }
    let pool = match mysql_pool_for_cleanup(&state.config, true).await {
        Ok(pool) => pool,
        Err(e) => {
            return HttpResponse::InternalServerError().json(json!({ "message": e }));
        }
    };
    let mut results = Vec::new();
    let mut total_count = 0i64;
    for target in targets {
        match cleanup_count(&pool, &target).await {
            Ok(count) => {
                total_count += count;
                results.push(json!({
                    "table_name": target.table_name,
                    "pk_column": target.pk_column,
                    "filter_column": target.filter_column,
                    "filter_values": target.filter_values,
                    "count": count,
                }));
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(json!({
                    "message": e,
                    "table_name": target.table_name
                }));
            }
        }
    }
    HttpResponse::Ok().json(json!({
        "total_count": total_count,
        "tables": results,
        "invalid_tables": invalid_tables.iter().map(|item| json!({
            "table_name": item.table_name,
            "reason": item.reason,
            "matched_columns": item.matched_columns,
        })).collect::<Vec<_>>()
    }))
}

async fn ui_split_cleanup_sql(
    state: web::Data<UiState>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    if !state.database_split_enabled() {
        return HttpResponse::NotFound().json(json!({
            "enabled": false,
            "message": "DatabaseSplit plugin is not enabled"
        }));
    }
    if state.config.sink_type.to_string() != "MySQL" {
        return HttpResponse::BadRequest().json(json!({
            "message": "cleanup SQL generation only supports MySQL sink"
        }));
    }
    let (targets, invalid_tables) =
        cleanup_targets(&state.config, &state.table_info_list.lock().await);
    let targets = filter_targets_by_request(targets, &request_tables(&body));
    if targets.is_empty() {
        return HttpResponse::BadRequest().json(json!({
            "message": "no cleanup tables available",
            "invalid_tables": invalid_tables.iter().map(|item| json!({
                "table_name": item.table_name,
                "reason": item.reason,
                "matched_columns": item.matched_columns,
            })).collect::<Vec<_>>()
        }));
    }
    let pool = match mysql_pool_for_cleanup(&state.config, false).await {
        Ok(pool) => pool,
        Err(e) => {
            return HttpResponse::InternalServerError().json(json!({ "message": e }));
        }
    };
    let batch_size = request_batch_size(&body);
    let mut sql = String::from(
        "-- Generated by rust_cdc_hub DatabaseSplit.\n-- Review before execution. This tool does not execute DELETE.\n\n",
    );
    let mut total_rows = 0usize;
    for target in targets {
        match cleanup_pk_values(&pool, &target).await {
            Ok(pk_values) => {
                total_rows += pk_values.len();
                sql.push_str(&build_delete_sql_batches(&target, &pk_values, batch_size));
            }
            Err(e) => {
                return HttpResponse::InternalServerError().json(json!({
                    "message": e,
                    "table_name": target.table_name
                }));
            }
        }
    }
    if total_rows == 0 {
        sql.push_str("-- No rows matched cleanup filters in sink.\n");
    }
    let filename = format!(
        "database_split_cleanup_{}.sql",
        Local::now().format("%Y%m%d_%H%M%S")
    );
    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, "text/sql; charset=utf-8"))
        .insert_header((
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        ))
        .body(sql)
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

async fn ui_root(state: web::Data<UiState>) -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(render_dashboard_html(
            state.database_split_enabled(),
            state.column_in_enabled,
        ))
}

async fn ui_split(state: web::Data<UiState>) -> impl Responder {
    if !state.database_split_enabled() {
        return HttpResponse::NotFound()
            .content_type("text/plain; charset=utf-8")
            .body("DatabaseSplit plugin is not enabled");
    }
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(SPLIT_HTML)
}

fn plugin_config<'a>(config: &'a CdcConfig, plugin_type: PluginType) -> Option<&'a PluginConfig> {
    config
        .plugins
        .as_ref()?
        .iter()
        .find(|plugin| plugin.plugin_type.to_string() == plugin_type.to_string())
}

fn validate_database_split_plugins(config: &CdcConfig) -> Result<(), String> {
    if plugin_config(config, PluginType::DatabaseSplit).is_some()
        && plugin_config(config, PluginType::Plus).is_some()
    {
        return Err(
            "DatabaseSplit cannot be used with Plus because cleanup SQL depends on original primary keys and filter columns"
                .to_string(),
        );
    }
    Ok(())
}

fn configured_plugin_values(config: &CdcConfig, plugin_type: PluginType, key: &str) -> Vec<String> {
    plugin_config(config, plugin_type)
        .map(|plugin| {
            plugin
                .get_config(key)
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CleanupTarget {
    table_name: String,
    pk_column: String,
    filter_column: String,
    filter_values: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct InvalidCleanupTarget {
    table_name: String,
    reason: String,
    matched_columns: Vec<String>,
}

fn cleanup_targets(
    config: &CdcConfig,
    table_info_list: &[TableInfoVo],
) -> (Vec<CleanupTarget>, Vec<InvalidCleanupTarget>) {
    let filter_columns = configured_plugin_values(config, PluginType::ColumnIn, "columns");
    let filter_values = configured_plugin_values(config, PluginType::ColumnIn, "values");
    if filter_columns.is_empty() || filter_values.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut targets = Vec::new();
    let mut invalid = Vec::new();
    for table_info in table_info_list {
        let matched_columns: Vec<String> = filter_columns
            .iter()
            .filter(|filter_column| {
                table_info
                    .columns
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(filter_column))
            })
            .cloned()
            .collect();
        if matched_columns.is_empty() {
            continue;
        }
        if table_info.pk_column.trim().is_empty() {
            invalid.push(InvalidCleanupTarget {
                table_name: table_info.table_name.clone(),
                reason: "missing primary key".to_string(),
                matched_columns,
            });
            continue;
        }
        if matched_columns.len() > 1 {
            invalid.push(InvalidCleanupTarget {
                table_name: table_info.table_name.clone(),
                reason: "matched multiple ColumnIn columns".to_string(),
                matched_columns,
            });
            continue;
        }
        targets.push(CleanupTarget {
            table_name: table_info.table_name.clone(),
            pk_column: table_info.pk_column.clone(),
            filter_column: matched_columns[0].clone(),
            filter_values: filter_values.clone(),
        });
    }
    targets.sort_by(|a, b| a.table_name.cmp(&b.table_name));
    invalid.sort_by(|a, b| a.table_name.cmp(&b.table_name));
    (targets, invalid)
}

fn cleanup_summary_json(config: &CdcConfig, table_info_list: &[TableInfoVo]) -> serde_json::Value {
    let (targets, invalid) = cleanup_targets(config, table_info_list);
    json!({
        "supported": config.sink_type.to_string() == "MySQL",
        "tables": targets.iter().map(|target| json!({
            "table_name": target.table_name,
            "pk_column": target.pk_column,
            "filter_column": target.filter_column,
            "filter_values": target.filter_values,
        })).collect::<Vec<_>>(),
        "invalid_tables": invalid.iter().map(|item| json!({
            "table_name": item.table_name,
            "reason": item.reason,
            "matched_columns": item.matched_columns,
        })).collect::<Vec<_>>(),
    })
}

fn mysql_connection_url(config: &CdcConfig, source: bool) -> String {
    let config_item = if source {
        &config.source_config[0]
    } else {
        &config.sink_config[0]
    };
    mysql_connection_url_from_config(config_item, config_item.get("database").map(String::as_str))
}

fn quoted_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn mysql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn filter_targets_by_request(
    targets: Vec<CleanupTarget>,
    requested_tables: &[String],
) -> Vec<CleanupTarget> {
    if requested_tables.is_empty() {
        return targets;
    }
    targets
        .into_iter()
        .filter(|target| {
            requested_tables
                .iter()
                .any(|table| table.eq_ignore_ascii_case(&target.table_name))
        })
        .collect()
}

fn request_tables(body: &serde_json::Value) -> Vec<String> {
    body.get("tables")
        .and_then(|v| v.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn request_batch_size(body: &serde_json::Value) -> usize {
    body.get("batch_size")
        .and_then(|v| v.as_u64())
        .map(|v| v.clamp(1, 10000) as usize)
        .unwrap_or(1000)
}

fn build_delete_sql_batches(
    target: &CleanupTarget,
    pk_values: &[String],
    batch_size: usize,
) -> String {
    let mut sql = String::new();
    if pk_values.is_empty() {
        return sql;
    }
    let filter_values = target
        .filter_values
        .iter()
        .map(|value| mysql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ");
    sql.push_str(&format!(
        "-- table: {}, filter: {} IN ({})\n",
        target.table_name, target.filter_column, filter_values
    ));
    for chunk in pk_values.chunks(batch_size.max(1)) {
        let pk_list = chunk
            .iter()
            .map(|value| mysql_string_literal(value))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(
            "DELETE FROM {} WHERE {} IN ({});\n",
            quoted_identifier(&target.table_name),
            quoted_identifier(&target.pk_column),
            pk_list
        ));
    }
    sql.push('\n');
    sql
}

fn plugin_summary(config: &CdcConfig) -> serde_json::Value {
    let plugins: Vec<serde_json::Value> = config
        .plugins
        .as_ref()
        .map(|plugins| {
            plugins
                .iter()
                .map(|plugin| match &plugin.plugin_type {
                    PluginType::ColumnIn => {
                        // Expose only safe ColumnIn config summary so the Dashboard can explain filter stats.
                        json!({
                            "plugin_type": "ColumnIn",
                            "columns": configured_plugin_values(config, PluginType::ColumnIn, "columns"),
                            "values": configured_plugin_values(config, PluginType::ColumnIn, "values")
                        })
                    }
                    PluginType::Plus => json!({
                        "plugin_type": "Plus"
                    }),
                    PluginType::DatabaseSplit => json!({
                        "plugin_type": "DatabaseSplit"
                    }),
                })
                .collect()
        })
        .unwrap_or_default();
    json!(plugins)
}

fn database_split_summary(config: &CdcConfig) -> serde_json::Value {
    let split_plugin = plugin_config(config, PluginType::DatabaseSplit);
    let enabled = split_plugin.is_some();
    let split_config = split_plugin.map(|p| &p.config);
    let get_split_value = |key: &str, default: &str| -> String {
        split_config
            .and_then(|m| m.get(key))
            .cloned()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| default.to_string())
    };
    // DatabaseSplit only exposes control summary fields; never return the raw confirm phrase.
    json!({
        "enabled": enabled,
        "task_name": if enabled { get_split_value("task_name", "database-split-task") } else { "".to_string() },
        "mode": if enabled { get_split_value("mode", "shard_data_split") } else { "".to_string() },
        "cleanup_strategy": if enabled { get_split_value("cleanup_strategy", "generate_sql") } else { "".to_string() },
        "cleanup_confirm_phrase": {
            "configured": split_config
                .and_then(|m| m.get("cleanup_confirm_phrase"))
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
        },
        "filter": {
            "plugin": "ColumnIn",
            "columns": configured_plugin_values(config, PluginType::ColumnIn, "columns"),
            "values": configured_plugin_values(config, PluginType::ColumnIn, "values")
        },
        "source": summarize_config_item(config.source_config.first()),
        "sink": summarize_config_item(config.sink_config.first()),
        "capabilities": {
            "dry_run_count": false,
            "delete_sql_generation": false,
            "delete_execution": false,
            "external_command": false
        }
    })
}

fn summarize_config_item(
    item: Option<&std::collections::HashMap<String, String>>,
) -> serde_json::Value {
    let value =
        |key: &str| -> String { item.and_then(|m| m.get(key)).cloned().unwrap_or_default() };
    json!({
        "host": value("host"),
        "port": value("port"),
        "database": value("database"),
        "table_name": value("table_name"),
        "except_table_name_prefix": value("except_table_name_prefix"),
        "server_id": value("server_id")
    })
}

fn render_dashboard_html(split_enabled: bool, column_in_enabled: bool) -> String {
    DASHBOARD_HTML
        .replace(
            "{{SPLIT_ENTRY}}",
            if split_enabled { SPLIT_ENTRY_HTML } else { "" },
        )
        .replace(
            "{{PLUGIN_FILTER_SECTION}}",
            if column_in_enabled {
                PLUGIN_FILTER_SECTION_HTML
            } else {
                ""
            },
        )
}

const SPLIT_ENTRY_HTML: &str = r#"<a class="btn primary" href="split" target="_blank" rel="noreferrer" data-i18n="database_split">数据库拆分</a>"#;

const PLUGIN_FILTER_SECTION_HTML: &str = r#"
    <section class="card section plugin-filter-section" id="pluginFilterSection">
      <div class="section-title-row">
        <h2 data-i18n="plugin_filter">插件过滤</h2>
        <span class="tag">ColumnIn</span>
      </div>
      <div class="plugin-filter-grid">
        <div class="progress-box"><div class="label" data-i18n="filter_columns">过滤字段</div><div class="value compact" id="columnInColumns">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="filter_values">过滤值</div><div class="value compact" id="columnInValues">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="filtered_total">过滤总数</div><div class="value" id="columnInFilteredTotal">0</div></div>
      </div>
      <div id="columnInFilterTable" class="table-wrap"></div>
    </section>
"#;

const SPLIT_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Database Split - Rust CDC Hub</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #06131d;
      --panel: rgba(17, 24, 39, 0.92);
      --line: rgba(148, 163, 184, 0.22);
      --muted: #9ca3af;
      --text: #eef2ff;
      --accent: #38bdf8;
      --ok: #22c55e;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; color: var(--text); background: linear-gradient(145deg, #020617 0%, var(--bg) 100%); }
    a { color: inherit; text-decoration: none; }
    .shell { width: min(1120px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 40px; }
    .topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 30px; line-height: 1.15; }
    .subtitle { margin: 8px 0 0; color: var(--muted); line-height: 1.7; }
    .btn { display: inline-flex; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: 10px 14px; background: rgba(15, 23, 42, 0.84); color: var(--text); font-weight: 700; cursor: pointer; }
    .btn.primary { border-color: rgba(56, 189, 248, 0.56); color: #bae6fd; }
    .btn.danger { border-color: rgba(248, 113, 113, 0.5); color: #fecaca; }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .card { border: 1px solid var(--line); background: var(--panel); border-radius: 18px; padding: 20px; }
    .card h2 { margin: 0 0 14px; font-size: 17px; }
    .kv { display: grid; gap: 10px; }
    .row { display: flex; justify-content: space-between; gap: 18px; padding: 9px 0; border-bottom: 1px solid rgba(148, 163, 184, 0.13); }
    .row:last-child { border-bottom: 0; }
    .key { color: var(--muted); }
    .value { text-align: right; font-weight: 700; overflow-wrap: anywhere; }
    .pill { display: inline-flex; align-items: center; border: 1px solid rgba(34, 197, 94, 0.35); color: #bbf7d0; background: rgba(34, 197, 94, 0.1); border-radius: 999px; padding: 7px 10px; font-size: 12px; font-weight: 800; text-transform: uppercase; }
    .wide { grid-column: 1 / -1; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 10px 8px; border-bottom: 1px solid rgba(148, 163, 184, 0.14); text-align: left; }
    th { color: var(--muted); font-size: 12px; font-weight: 700; }
    td { font-size: 13px; font-weight: 700; }
    .empty { color: var(--muted); padding: 16px 0; }
    .notice { color: #fde68a; background: rgba(234, 179, 8, 0.1); border: 1px solid rgba(234, 179, 8, 0.28); border-radius: 14px; padding: 12px 14px; line-height: 1.65; }
    .result { margin-top: 14px; color: var(--muted); }
    .summary-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 14px; }
    .summary-box { border: 1px solid rgba(148, 163, 184, 0.18); border-radius: 14px; padding: 12px; background: rgba(2, 6, 23, 0.26); }
    .summary-box .label { color: var(--muted); font-size: 12px; margin-bottom: 6px; }
    .summary-box .value { font-size: 22px; font-weight: 800; color: var(--text); }
    .resource-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
    pre { margin: 16px 0 0; max-height: 360px; overflow: auto; padding: 14px; border-radius: 14px; background: #020617; border: 1px solid var(--line); color: #bae6fd; font-size: 12px; line-height: 1.6; }
    @media (max-width: 780px) { .grid, .topbar { grid-template-columns: 1fr; display: grid; } .resource-grid { grid-template-columns: repeat(2, 1fr); } .topbar { align-items: start; } }
  </style>
</head>
<body>
  <main class="shell">
    <div class="topbar">
      <div>
        <h1>数据库拆分</h1>
        <p class="subtitle">查看拆分任务配置、同步范围和后续清理策略。V1 仅提供状态与入口，不执行删除或外部命令。</p>
      </div>
      <a class="btn" href=".">返回 Dashboard</a>
    </div>
    <section class="grid">
      <div class="card">
        <h2>任务</h2>
        <div class="kv" id="taskKv"></div>
      </div>
      <div class="card">
        <h2>同步范围</h2>
        <div class="kv" id="filterKv"></div>
      </div>
      <div class="card">
        <h2>Source</h2>
        <div class="kv" id="sourceKv"></div>
      </div>
      <div class="card">
        <h2>Sink</h2>
        <div class="kv" id="sinkKv"></div>
      </div>
      <div class="card wide">
        <h2>同步进度</h2>
        <div id="progressSummary" class="kv"></div>
        <div id="progressTableWrap" style="margin-top:14px"></div>
      </div>
      <div class="card wide">
        <h2>资源占用</h2>
        <div class="resource-grid">
          <div class="summary-box"><div class="label">机器 CPU</div><div class="value" id="splitSystemCpu">-</div></div>
          <div class="summary-box"><div class="label">机器内存</div><div class="value" id="splitSystemMemory">-</div></div>
          <div class="summary-box"><div class="label">进程 CPU</div><div class="value" id="splitProcessCpu">-</div></div>
          <div class="summary-box"><div class="label">进程内存</div><div class="value" id="splitProcessMemory">-</div></div>
        </div>
      </div>
      <div class="card wide">
        <h2>清理 SQL</h2>
        <div class="notice">
          工具只生成 SQL，不执行删除。SQL 基于 sink 中已同步数据的主键生成，请下载后人工检查，并在业务低峰执行。数据库拆分模式不允许和会修改数据的插件混用。
        </div>
        <div id="cleanupTableWrap" style="margin-top:14px"></div>
        <div id="cleanupInvalidWrap" class="result"></div>
        <div class="actions">
          <button class="btn primary" type="button" onclick="runCleanupDryRun()">Dry-run 统计</button>
          <button class="btn danger" type="button" onclick="downloadCleanupSql()">下载删除 SQL</button>
        </div>
        <div id="cleanupResult" class="result"></div>
      </div>
    </section>
    <pre id="rawJson">loading...</pre>
  </main>
<script>
const el = (id) => document.getElementById(id);
function escapeHtml(value) {
  return String(value ?? '-').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[ch]));
}
function row(key, value) {
  const safe = value === null || value === undefined || value === '' ? '-' : value;
  return `<div class="row"><span class="key">${escapeHtml(key)}</span><span class="value">${escapeHtml(safe)}</span></div>`;
}
function rows(obj, keys) {
  return keys.map(([label, key]) => row(label, obj?.[key])).join('');
}
function fmtPercent(value) {
  const n = Number(value || 0);
  return `${n.toFixed(1)}%`;
}
function fmtBytes(bytes) {
  let value = Number(bytes || 0);
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}
function renderProgressTable(tables) {
  if (!tables.length) return '<div class="empty">暂无运行态统计</div>';
  const body = tables.map((table) => `<tr>
    <td>${escapeHtml(table.table_name)}</td>
    <td>${escapeHtml(table.phase)}</td>
    <td>${escapeHtml(table.read_total ?? 0)}</td>
    <td>${escapeHtml(table.synced_total ?? 0)}</td>
    <td>${escapeHtml(table.filtered_total ?? 0)}</td>
    <td>${escapeHtml(table.last_pk)}</td>
  </tr>`).join('');
  return `<table><thead><tr><th>表名</th><th>阶段</th><th>读取</th><th>同步</th><th>过滤</th><th>最后主键</th></tr></thead><tbody>${body}</tbody></table>`;
}
function renderCleanupTables(tables) {
  if (!tables.length) return '<div class="empty">暂无可生成清理 SQL 的表。等待初始化表结构加载，或检查 ColumnIn 配置。</div>';
  const body = tables.map((table) => `<tr>
    <td>${escapeHtml(table.table_name)}</td>
    <td>${escapeHtml(table.pk_column)}</td>
    <td>${escapeHtml(table.filter_column)}</td>
    <td>${escapeHtml((table.filter_values || []).join(', '))}</td>
  </tr>`).join('');
  return `<table><thead><tr><th>表名</th><th>主键</th><th>过滤字段</th><th>过滤值</th></tr></thead><tbody>${body}</tbody></table>`;
}
function renderInvalidCleanupTables(tables) {
  if (!tables.length) return '';
  const body = tables.map((table) => `<tr>
    <td>${escapeHtml(table.table_name)}</td>
    <td>${escapeHtml(table.reason)}</td>
    <td>${escapeHtml((table.matched_columns || []).join(', '))}</td>
  </tr>`).join('');
  return `<div style="margin-top:14px"><strong>不能生成 SQL 的表</strong><table><thead><tr><th>表名</th><th>原因</th><th>命中字段</th></tr></thead><tbody>${body}</tbody></table></div>`;
}
function renderDryRunResult(data) {
  const tables = data.tables || [];
  const body = tables.map((table) => `<tr>
    <td>${escapeHtml(table.table_name)}</td>
    <td>${escapeHtml(table.pk_column)}</td>
    <td>${escapeHtml(table.filter_column)}</td>
    <td>${escapeHtml((table.filter_values || []).join(', '))}</td>
    <td>${escapeHtml(table.count ?? 0)}</td>
  </tr>`).join('');
  const tableHtml = tables.length
    ? `<table><thead><tr><th>表名</th><th>主键</th><th>过滤字段</th><th>过滤值</th><th>待删除条数</th></tr></thead><tbody>${body}</tbody></table>`
    : '<div class="empty">Dry-run 未返回表级统计</div>';
  return `
    <div class="summary-grid">
      <div class="summary-box"><div class="label">待删除总数</div><div class="value">${escapeHtml(data.total_count ?? 0)}</div></div>
      <div class="summary-box"><div class="label">统计表数量</div><div class="value">${escapeHtml(tables.length)}</div></div>
      <div class="summary-box"><div class="label">异常表数量</div><div class="value">${escapeHtml((data.invalid_tables || []).length)}</div></div>
    </div>
    <div style="margin-top:14px">${tableHtml}</div>
    ${renderInvalidCleanupTables(data.invalid_tables || [])}
  `;
}
function renderSplitResources(resources) {
  const system = resources?.system || {};
  const processInfo = resources?.process || {};
  el('splitSystemCpu').textContent = fmtPercent(system.cpu_percent);
  el('splitSystemMemory').textContent = `${fmtBytes(system.memory_used_bytes)} / ${fmtBytes(system.memory_total_bytes)}`;
  el('splitProcessCpu').textContent = fmtPercent(processInfo.cpu_percent);
  el('splitProcessMemory').textContent = `${fmtBytes(processInfo.memory_bytes)} · pid ${processInfo.pid ?? '-'}`;
}
function cleanupRequestBody() {
  return JSON.stringify({ tables: [] });
}
async function runCleanupDryRun() {
  el('cleanupResult').innerHTML = '<div class="empty">Dry-run 统计中...</div>';
  const response = await fetch('api/split/cleanup/dry-run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: cleanupRequestBody(),
  });
  const text = await response.text();
  if (!response.ok) {
    el('cleanupResult').textContent = text;
    return;
  }
  const data = JSON.parse(text);
  el('cleanupResult').innerHTML = renderDryRunResult(data);
}
async function downloadCleanupSql() {
  el('cleanupResult').textContent = '正在生成 SQL...';
  const response = await fetch('api/split/cleanup/sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tables: [], batch_size: 1000 }),
  });
  const blob = await response.blob();
  if (!response.ok) {
    el('cleanupResult').textContent = await blob.text();
    return;
  }
  const disposition = response.headers.get('Content-Disposition') || '';
  const match = disposition.match(/filename="([^"]+)"/);
  const filename = match ? match[1] : 'database_split_cleanup.sql';
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  el('cleanupResult').textContent = `SQL 已生成: ${filename}`;
}
async function loadSplitStatus() {
  const response = await fetch('api/split/status', { cache: 'no-store' });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const data = await response.json();
  el('taskKv').innerHTML = [
    row('状态', data.enabled ? 'enabled' : 'disabled'),
    row('任务名称', data.task_name),
    row('模式', data.mode),
    row('清理策略', data.cleanup_strategy),
    row('确认口令', data.cleanup_confirm_phrase?.configured ? 'configured' : 'not configured'),
  ].join('');
  el('filterKv').innerHTML = [
    row('过滤插件', data.filter?.plugin),
    row('过滤列', (data.filter?.columns || []).join(', ')),
    row('过滤值', (data.filter?.values || []).join(', ')),
  ].join('');
  el('sourceKv').innerHTML = rows(data.source, [['Host', 'host'], ['Port', 'port'], ['Database', 'database'], ['Table', 'table_name'], ['Exclude Prefix', 'except_table_name_prefix'], ['Server ID', 'server_id']]);
  el('sinkKv').innerHTML = rows(data.sink, [['Host', 'host'], ['Port', 'port'], ['Database', 'database']]);
  const progress = data.runtime_progress || {};
  const tables = Object.values(progress.tables || {});
  const readTotal = tables.reduce((sum, table) => sum + Number(table.read_total || 0), 0);
  const syncedTotal = tables.reduce((sum, table) => sum + Number(table.synced_total || 0), 0);
  const filteredTotal = tables.reduce((sum, table) => sum + Number(table.filtered_total || 0), 0);
  el('progressSummary').innerHTML = [
    row('初始化中', progress.initializing ? 'yes' : 'no'),
    row('当前表', progress.current_table),
    row('读取 / 同步', `${readTotal} / ${syncedTotal}`),
    row('过滤', filteredTotal),
  ].join('');
  el('progressTableWrap').innerHTML = renderProgressTable(tables);
  renderSplitResources(data.resources);
  el('cleanupTableWrap').innerHTML = renderCleanupTables(data.cleanup?.tables || []);
  el('cleanupInvalidWrap').innerHTML = renderInvalidCleanupTables(data.cleanup?.invalid_tables || []);
  el('rawJson').textContent = JSON.stringify(data, null, 2);
}
loadSplitStatus().catch((err) => { el('rawJson').textContent = `无法加载拆分状态: ${err.message}`; });
</script>
</body>
</html>"#;

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
    .status-pill.initializing { border-color: rgba(56, 189, 248, 0.5); background: rgba(56, 189, 248, 0.14); color: #bae6fd; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--accent-2); box-shadow: 0 0 0 6px rgba(34, 197, 94, 0.12); }
    .degraded .dot { background: var(--warning); box-shadow: 0 0 0 6px rgba(249, 115, 22, 0.12); }
    .initializing .dot { background: var(--accent); box-shadow: 0 0 0 6px rgba(56, 189, 248, 0.12); }
    .hero-side { padding: 22px; display: grid; gap: 14px; }
    .route { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 14px; border: 1px solid var(--line); border-radius: 18px; background: rgba(15, 23, 42, 0.52); }
    .route span { color: var(--muted); font-size: 13px; }
    .route strong { font-size: 15px; }
    .metrics-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 16px; }
    .metric { padding: 20px; }
    .metric .label { color: var(--muted); font-size: 13px; margin-bottom: 10px; }
    .metric .value { font-size: 30px; font-weight: 800; letter-spacing: -0.04em; }
    .metric .hint { margin-top: 8px; color: var(--muted); font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .resource-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
    .content-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .plugin-filter-section { margin-bottom: 16px; }
    .section-title-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 16px; }
    .section-title-row h2 { margin: 0; }
    .tag { display: inline-flex; align-items: center; border: 1px solid rgba(56, 189, 248, 0.4); color: #bae6fd; background: rgba(56, 189, 248, 0.1); border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: 800; }
    .plugin-filter-grid { display: grid; grid-template-columns: 1.2fr 1.2fr 0.7fr; gap: 12px; }
    .progress-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
    .progress-grid[hidden] { display: none; }
    .progress-timing-grid { grid-template-columns: repeat(3, 1fr); margin-top: 12px; }
    .progress-box { border: 1px solid rgba(148, 163, 184, 0.18); border-radius: 14px; padding: 14px; background: rgba(2, 6, 23, 0.26); }
    .progress-box .label { color: var(--muted); font-size: 12px; margin-bottom: 8px; }
    .progress-box .value { font-size: 22px; font-weight: 800; overflow-wrap: anywhere; }
    .progress-box .value.compact { font-size: 16px; line-height: 1.45; letter-spacing: 0; }
    .table-wrap { margin-top: 14px; overflow: auto; max-height: 560px; border: 1px solid rgba(148, 163, 184, 0.18); border-radius: 14px; }
    table { width: 100%; border-collapse: collapse; min-width: 620px; }
    th, td { padding: 12px 14px; border-bottom: 1px solid rgba(148, 163, 184, 0.13); text-align: left; font-size: 13px; }
    th { position: sticky; top: 0; z-index: 2; color: var(--muted); font-weight: 700; background: #101a2c; box-shadow: 0 1px 0 rgba(148, 163, 184, 0.18); }
    th.sortable { cursor: pointer; user-select: none; }
    th.sortable:hover { color: var(--text); background: #14213a; }
    .sort-indicator { margin-left: 6px; color: var(--accent); font-size: 11px; }
    tr:last-child td { border-bottom: 0; }
    tr.phase-initializing td { background: rgba(14, 165, 233, 0.26); color: #e0f2fe; font-weight: 800; }
    tr.phase-initializing td:first-child { border-left: 4px solid #38bdf8; }
    .empty { color: var(--muted); padding: 14px; }
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
    .top-actions { display: flex; justify-content: flex-end; margin-bottom: 12px; }
    .lang-toggle { border: 1px solid rgba(56, 189, 248, 0.42); color: #bae6fd; background: rgba(15, 23, 42, 0.86); border-radius: 999px; padding: 8px 12px; font-weight: 800; cursor: pointer; }
    @media (max-width: 900px) { .hero, .content-grid { grid-template-columns: 1fr; } .metrics-grid, .progress-grid, .plugin-filter-grid, .resource-grid { grid-template-columns: repeat(2, 1fr); } }
    @media (max-width: 560px) { .shell { width: min(100% - 20px, 1180px); padding-top: 18px; } .metrics-grid, .progress-grid, .plugin-filter-grid, .resource-grid { grid-template-columns: 1fr; } .hero-main, .hero-side, .section, .metric { padding: 16px; } }
  </style>
</head>
<body>
  <main class="shell">
    <div class="top-actions">
      <button class="lang-toggle" type="button" id="langToggle">English</button>
    </div>
    <section class="hero">
      <div class="card hero-main">
        <div class="eyebrow">Change Data Capture</div>
        <h1>Rust CDC Hub</h1>
        <p class="subtitle" data-i18n="subtitle">实时查看 CDC 作业状态、source/sink 配置摘要、flush 与重启信息。页面每 5 秒自动刷新，也可以手动刷新。</p>
        <div id="statusPill" class="status-pill"><span class="dot"></span><span id="statusText">loading</span></div>
        <div class="actions">
          <button class="btn primary" type="button" onclick="loadStatus()" data-i18n="refresh_now">立即刷新</button>
          <a class="btn" href="status" target="_blank" rel="noreferrer" data-i18n="json_status">JSON 状态</a>
          <a class="btn" href="metrics" target="_blank" rel="noreferrer" data-i18n="metrics">Prometheus 指标</a>
          <a class="btn" href="health" target="_blank" rel="noreferrer" data-i18n="health">健康检查</a>
          {{SPLIT_ENTRY}}
        </div>
      </div>
      <aside class="card hero-side">
        <div class="route"><span>Source</span><strong id="sourceType">-</strong></div>
        <div class="route"><span>Sink</span><strong id="sinkType">-</strong></div>
        <div class="route"><span data-i18n="auto_refresh">自动刷新</span><strong>5s</strong></div>
      </aside>
    </section>

    {{PLUGIN_FILTER_SECTION}}

    <section class="metrics-grid">
      <div class="card metric"><div class="label" data-i18n="uptime">运行时间</div><div class="value" id="uptime">-</div><div class="hint" id="startedAt">started: -</div></div>
      <div class="card metric"><div class="label" data-i18n="timer_flush">定时 flush</div><div class="value" id="flushCount">0</div><div class="hint" id="lastFlush">last flush: -</div></div>
      <div class="card metric"><div class="label" data-i18n="source_restart">Source 重启</div><div class="value" id="restartCount">0</div><div class="hint" id="lastRestart">last restart: -</div></div>
      <div class="card metric"><div class="label" data-i18n="config_scale">配置规模</div><div class="value" id="configScale">-</div><div class="hint" id="pluginCount">plugins: -</div></div>
    </section>

    <section class="content-grid">
      <div class="card section">
        <h2 data-i18n="config_summary">配置摘要</h2>
        <div class="kv" id="configKv"></div>
      </div>
      <div class="card section">
        <h2 data-i18n="recent_error">最近错误</h2>
        <div id="errorBox" class="error-box empty">暂无 source 错误</div>
        <div class="actions"><span class="btn"><span data-i18n="last_updated">最后更新</span>：<span id="lastUpdated">-</span></span></div>
      </div>
    </section>

    <section class="card section" style="margin-top:16px">
      <h2 data-i18n="resource_usage">资源占用</h2>
      <div class="resource-grid">
        <div class="progress-box"><div class="label" data-i18n="system_cpu">机器 CPU</div><div class="value" id="systemCpu">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="system_memory">机器内存</div><div class="value compact" id="systemMemory">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="process_cpu">进程 CPU</div><div class="value" id="processCpu">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="process_memory">进程内存</div><div class="value compact" id="processMemory">-</div><div class="hint" id="processPid">pid: -</div></div>
      </div>
      <div class="actions">
        <button class="btn primary" type="button" id="memoryTrimButton" onclick="trimMemory()" data-i18n="trim_memory">触发内存回收</button>
        <span class="btn" id="memoryTrimResult" data-i18n="trim_memory_idle">等待手动触发</span>
      </div>
    </section>

    <section class="card section" style="margin-top:16px">
      <h2 data-i18n="sync_progress">数据同步进度</h2>
      <div class="progress-grid">
        <div class="progress-box"><div class="label" data-i18n="current_status">当前状态</div><div class="value" id="progressState">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="current_table">当前表</div><div class="value" id="progressTable">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="read_sink">读取 / 写入 Sink</div><div class="value" id="progressReadSynced">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="filtered_rows">过滤条数</div><div class="value" id="progressFiltered">-</div></div>
      </div>
      <div id="initializationTimingGrid" class="progress-grid progress-timing-grid" hidden>
        <div class="progress-box"><div class="label" data-i18n="initialization_started_at">初始化开始时间</div><div class="value compact" id="initializationStartedAt">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="initialization_finished_at">初始化结束时间</div><div class="value compact" id="initializationFinishedAt">-</div></div>
        <div class="progress-box"><div class="label" data-i18n="initialization_duration">初始化耗时</div><div class="value compact" id="initializationDuration">-</div></div>
      </div>
      <div id="syncProgressTable" class="table-wrap"></div>
    </section>

    <section class="card section" style="margin-top:16px">
      <h2 data-i18n="raw_json">原始状态 JSON</h2>
      <pre id="rawJson">loading...</pre>
    </section>
    <div class="footer">Powered by actix-web · rust_cdc_hub</div>
  </main>

<script>
const el = (id) => document.getElementById(id);
const translations = {
  zh: {
    lang_toggle: 'English',
    subtitle: '实时查看 CDC 作业状态、source/sink 配置摘要、flush 与重启信息。页面每 5 秒自动刷新，也可以手动刷新。',
    refresh_now: '立即刷新',
    json_status: 'JSON 状态',
    metrics: 'Prometheus 指标',
    health: '健康检查',
    database_split: '数据库拆分',
    auto_refresh: '自动刷新',
    plugin_filter: '插件过滤',
    filter_columns: '过滤字段',
    filter_values: '过滤值',
    filtered_total: '过滤总数',
    uptime: '运行时间',
    timer_flush: '定时 flush',
    source_restart: 'Source 重启',
    config_scale: '配置规模',
    config_summary: '配置摘要',
    recent_error: '最近错误',
    last_updated: '最后更新',
    resource_usage: '资源占用',
    system_cpu: '机器 CPU',
    system_memory: '机器内存',
    process_cpu: '进程 CPU',
    process_memory: '进程内存',
    trim_memory: '触发内存回收',
    trim_memory_idle: '等待手动触发',
    trim_memory_running: '正在尝试回收...',
    trim_memory_done: '回收完成',
    trim_memory_unsupported: '当前平台不支持',
    trim_memory_failed: '内存回收失败',
    sync_progress: '数据同步进度',
    current_status: '当前状态',
    current_table: '当前表',
    read_sink: '总计读取 / 写入 Sink',
    filtered_rows: '总计过滤条数',
    initialization_started_at: '初始化开始时间',
    initialization_finished_at: '初始化结束时间',
    initialization_duration: '初始化耗时',
    initializing_running: '进行中',
    raw_json: '原始状态 JSON',
    no_column_in_hits: '暂无 ColumnIn 过滤命中',
    no_table_sync: '暂无表级同步记录',
    no_source_error: '暂无 source 错误',
    no_matched_filter_column: '未命中过滤字段',
    table_name: '表名',
    matched_column: '命中字段',
    input_rows: '入条数',
    output_rows: '出条数',
    filtered_count: '过滤条数',
    last_event_at: '最后更新时间',
    phase: '阶段',
    read: '读取',
    sink_written: '写入 Sink',
    filtered: '过滤',
    last_pk: '最后主键',
    source_type: 'Source 类型',
    sink_type: 'Sink 类型',
    split_mode: '数据库拆分模式',
    split_task: '拆分任务',
    source_count: 'Source 配置数量',
    sink_count: 'Sink 配置数量',
    plugin_count: '插件数量',
    checkpoint_path: 'Checkpoint 路径',
    auto_create_database: '自动建库',
    auto_create_table: '自动建表',
    auto_add_column: '自动加字段',
    auto_modify_column: '自动改字段',
    status_running: '运行中',
    status_initializing: '初始化中',
    status_degraded: '异常',
    status_unknown: '未知',
    status_ui_error: '界面错误',
    phase_running: '运行中',
    phase_initializing: '初始化中',
    phase_cdc: '增量同步',
    phase_done: '已完成',
    started: 'started',
    last_flush: 'last flush',
    last_restart: 'last restart',
    plugins: 'plugins',
    load_status_failed: '无法加载 status',
  },
  en: {
    lang_toggle: '中文',
    subtitle: 'Monitor CDC job status, source/sink config summary, flush status, and restart information in real time. This page refreshes every 5 seconds.',
    refresh_now: 'Refresh',
    json_status: 'JSON Status',
    metrics: 'Prometheus Metrics',
    health: 'Health Check',
    database_split: 'Database Split',
    auto_refresh: 'Auto Refresh',
    plugin_filter: 'Plugin Filter',
    filter_columns: 'Filter Columns',
    filter_values: 'Allowed Values',
    filtered_total: 'Filtered Total',
    uptime: 'Uptime',
    timer_flush: 'Timer Flush',
    source_restart: 'Source Restarts',
    config_scale: 'Config Scale',
    config_summary: 'Config Summary',
    recent_error: 'Recent Error',
    last_updated: 'Last Updated',
    resource_usage: 'Resource Usage',
    system_cpu: 'System CPU',
    system_memory: 'System Memory',
    process_cpu: 'Process CPU',
    process_memory: 'Process Memory',
    trim_memory: 'Trim Memory',
    trim_memory_idle: 'Waiting for manual trigger',
    trim_memory_running: 'Trimming...',
    trim_memory_done: 'Trim completed',
    trim_memory_unsupported: 'Unsupported on this platform',
    trim_memory_failed: 'Memory trim failed',
    sync_progress: 'Data Sync Progress',
    current_status: 'Current Status',
    current_table: 'Current Table',
    read_sink: 'Total Read / Written to Sink',
    filtered_rows: 'Total Filtered Rows',
    initialization_started_at: 'Initialization Started',
    initialization_finished_at: 'Initialization Finished',
    initialization_duration: 'Initialization Duration',
    initializing_running: 'Running',
    raw_json: 'Raw Status JSON',
    no_column_in_hits: 'No ColumnIn filter hits',
    no_table_sync: 'No table-level sync records',
    no_source_error: 'No source errors',
    no_matched_filter_column: 'No matched filter column',
    table_name: 'Table',
    matched_column: 'Matched Column',
    input_rows: 'Input Rows',
    output_rows: 'Output Rows',
    filtered_count: 'Filtered Rows',
    last_event_at: 'Last Updated',
    phase: 'Phase',
    read: 'Read',
    sink_written: 'Written to Sink',
    filtered: 'Filtered',
    last_pk: 'Last PK',
    source_type: 'Source Type',
    sink_type: 'Sink Type',
    split_mode: 'Database Split Mode',
    split_task: 'Split Task',
    source_count: 'Source Config Count',
    sink_count: 'Sink Config Count',
    plugin_count: 'Plugin Count',
    checkpoint_path: 'Checkpoint Path',
    auto_create_database: 'Auto Create Database',
    auto_create_table: 'Auto Create Table',
    auto_add_column: 'Auto Add Column',
    auto_modify_column: 'Auto Modify Column',
    status_running: 'Running',
    status_initializing: 'Initializing',
    status_degraded: 'Degraded',
    status_unknown: 'Unknown',
    status_ui_error: 'UI Error',
    phase_running: 'Running',
    phase_initializing: 'Initializing',
    phase_cdc: 'CDC',
    phase_done: 'Done',
    started: 'started',
    last_flush: 'last flush',
    last_restart: 'last restart',
    plugins: 'plugins',
    load_status_failed: 'Failed to load status',
  },
};
let currentLang = localStorage.getItem('rustCdcHubLang') || 'zh';
let latestStatus = null;
const tableSortState = {
  pluginFilters: { key: 'last_event_at', dir: 'desc' },
  syncProgress: { key: 'last_event_at', dir: 'desc' },
};
function t(key) {
  return translations[currentLang]?.[key] || translations.zh[key] || key;
}
function applyLanguage() {
  document.documentElement.lang = currentLang === 'en' ? 'en' : 'zh-CN';
  document.querySelectorAll('[data-i18n]').forEach((node) => {
    node.textContent = t(node.dataset.i18n);
  });
  const langToggle = el('langToggle');
  if (langToggle) langToggle.textContent = t('lang_toggle');
}
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
function fmtPercent(value) {
  const n = Number(value || 0);
  return `${n.toFixed(1)}%`;
}
function fmtBytes(bytes) {
  let value = Number(bytes || 0);
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
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
function displayFilterColumn(columnName) {
  return columnName === '__no_matched_filter_column__' ? t('no_matched_filter_column') : (columnName || '-');
}
function displayStatus(status) {
  const value = status || 'unknown';
  const key = `status_${String(value).replace(/[^a-zA-Z0-9]+/g, '_').toLowerCase()}`;
  return translations[currentLang]?.[key] || translations.zh[key] || value;
}
function displayPhase(phase) {
  const value = phase || '-';
  const key = `phase_${String(value).replace(/[^a-zA-Z0-9]+/g, '_').toLowerCase()}`;
  return translations[currentLang]?.[key] || translations.zh[key] || value;
}
function sortValue(item, key) {
  if (key === 'column_name') return displayFilterColumn(item.column_name);
  const value = item?.[key];
  if (typeof value === 'number') return value;
  if (value === null || value === undefined) return '';
  return String(value);
}
function compareRows(a, b, key, dir) {
  const av = sortValue(a, key);
  const bv = sortValue(b, key);
  let result;
  if (typeof av === 'number' && typeof bv === 'number') {
    result = av - bv;
  } else {
    result = String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: 'base' });
  }
  if (result === 0 && key !== 'table_name') {
    result = String(a.table_name || '').localeCompare(String(b.table_name || ''), undefined, { numeric: true, sensitivity: 'base' });
  }
  return dir === 'asc' ? result : -result;
}
function sortRows(rows, tableKey) {
  const state = tableSortState[tableKey] || { key: 'last_event_at', dir: 'desc' };
  return [...rows].sort((a, b) => compareRows(a, b, state.key, state.dir));
}
function sortHeader(tableKey, columnKey, label) {
  const state = tableSortState[tableKey] || {};
  const active = state.key === columnKey;
  const indicator = active ? `<span class="sort-indicator">${state.dir === 'asc' ? '▲' : '▼'}</span>` : '';
  return `<th class="sortable" onclick="setTableSort('${tableKey}', '${columnKey}')">${escapeHtml(label)}${indicator}</th>`;
}
function setTableSort(tableKey, columnKey) {
  const state = tableSortState[tableKey] || { key: 'last_event_at', dir: 'desc' };
  tableSortState[tableKey] = {
    key: columnKey,
    dir: state.key === columnKey && state.dir === 'desc' ? 'asc' : 'desc',
  };
  if (latestStatus) renderStatusData(latestStatus);
}
function groupPluginFiltersByTableAndColumn(filters) {
  const grouped = new Map();
  for (const item of filters) {
    const tableName = item.table_name || '-';
    const columnName = item.column_name || '-';
    const key = `${tableName}\u0000${columnName}`;
    const current = grouped.get(key) || {
      plugin_name: item.plugin_name || 'ColumnIn',
      table_name: tableName,
      column_name: columnName,
      input_total: 0,
      output_total: 0,
      filtered_total: 0,
      last_event_at: 0,
    };
    current.input_total += Number(item.input_total || 0);
    current.output_total += Number(item.output_total || 0);
    current.filtered_total += Number(item.filtered_total || 0);
    current.last_event_at = Math.max(Number(current.last_event_at || 0), Number(item.last_event_at || 0));
    grouped.set(key, current);
  }
  return Array.from(grouped.values());
}
function tableInitializationDurationSeconds(table, now) {
  const startedAt = Number(table.initialization_started_at || 0);
  if (startedAt <= 0) return null;
  const finishedAt = table.phase === 'initializing' ? 0 : Number(table.initialization_finished_at || 0);
  const effectiveEnd = finishedAt > 0 ? finishedAt : Number(now || 0);
  return Math.max(0, effectiveEnd - startedAt);
}
function renderColumnInFilters(cfg, progress) {
  const section = el('pluginFilterSection');
  if (!section) return;
  const plugins = Array.isArray(cfg.plugins) ? cfg.plugins : [];
  const columnIn = plugins.find((plugin) => plugin.plugin_type === 'ColumnIn') || {};
  const columns = Array.isArray(columnIn.columns) ? columnIn.columns : [];
  const values = Array.isArray(columnIn.values) ? columnIn.values : [];
  const filters = groupPluginFiltersByTableAndColumn(
    Object.values(progress.plugin_filters || {})
      .filter((item) => item.plugin_name === 'ColumnIn')
  );
  const total = filters.reduce((sum, item) => sum + Number(item.filtered_total || 0), 0);
  setText('columnInColumns', columns.join(', ') || '-');
  setText('columnInValues', values.join(', ') || '-');
  setText('columnInFilteredTotal', total);
  if (!filters.length) {
    el('columnInFilterTable').innerHTML = `<div class="empty">${escapeHtml(t('no_column_in_hits'))}</div>`;
    return;
  }
  const sortedFilters = sortRows(filters, 'pluginFilters');
  const body = sortedFilters.map((item) => `<tr>
    <td>${escapeHtml(item.table_name || '-')}</td>
    <td>${escapeHtml(displayFilterColumn(item.column_name))}</td>
    <td>${escapeHtml(item.input_total ?? 0)}</td>
    <td>${escapeHtml(item.output_total ?? 0)}</td>
    <td>${escapeHtml(item.filtered_total ?? 0)}</td>
    <td>${escapeHtml(fmtTs(item.last_event_at))}</td>
  </tr>`).join('');
  el('columnInFilterTable').innerHTML = `<table><thead><tr>${sortHeader('pluginFilters', 'table_name', t('table_name'))}${sortHeader('pluginFilters', 'column_name', t('matched_column'))}${sortHeader('pluginFilters', 'input_total', t('input_rows'))}${sortHeader('pluginFilters', 'output_total', t('output_rows'))}${sortHeader('pluginFilters', 'filtered_total', t('filtered_count'))}${sortHeader('pluginFilters', 'last_event_at', t('last_event_at'))}</tr></thead><tbody>${body}</tbody></table>`;
}
function renderSyncProgressTable(tables, now) {
  const target = el('syncProgressTable');
  if (!target) return;
  if (!tables.length) {
    target.innerHTML = `<div class="empty">${escapeHtml(t('no_table_sync'))}</div>`;
    return;
  }
  const rowsWithDurations = tables.map((table) => {
    const duration = tableInitializationDurationSeconds(table, now);
    return {
      ...table,
      initialization_duration: duration ?? -1,
      initialization_duration_label: duration === null ? '-' : fmtDuration(duration),
    };
  });
  const rows = sortRows(rowsWithDurations, 'syncProgress')
    .map((table) => `<tr class="${table.phase === 'initializing' ? 'phase-initializing' : ''}">
      <td>${escapeHtml(table.table_name || '-')}</td>
      <td>${escapeHtml(displayPhase(table.phase))}</td>
      <td>${escapeHtml(table.initialization_duration_label)}</td>
      <td>${escapeHtml(table.read_total ?? 0)}</td>
      <td>${escapeHtml(table.synced_total ?? 0)}</td>
      <td>${escapeHtml(table.filtered_total ?? 0)}</td>
      <td>${escapeHtml(table.last_pk || '-')}</td>
      <td>${escapeHtml(fmtTs(table.last_event_at))}</td>
    </tr>`)
    .join('');
  target.innerHTML = `<table><thead><tr>${sortHeader('syncProgress', 'table_name', t('table_name'))}${sortHeader('syncProgress', 'phase', t('phase'))}${sortHeader('syncProgress', 'initialization_duration', t('initialization_duration'))}${sortHeader('syncProgress', 'read_total', t('read'))}${sortHeader('syncProgress', 'synced_total', t('sink_written'))}${sortHeader('syncProgress', 'filtered_total', t('filtered'))}${sortHeader('syncProgress', 'last_pk', t('last_pk'))}${sortHeader('syncProgress', 'last_event_at', t('last_event_at'))}</tr></thead><tbody>${rows}</tbody></table>`;
}
function renderInitializationTiming(progress, now) {
  const timingGrid = el('initializationTimingGrid');
  if (!timingGrid) return;
  const startedAt = Number(progress.initialization_started_at || 0);
  const finishedAt = progress.initializing ? 0 : Number(progress.initialization_finished_at || 0);
  timingGrid.hidden = startedAt <= 0;
  if (startedAt <= 0) {
    setText('initializationStartedAt', '-');
    setText('initializationFinishedAt', '-');
    setText('initializationDuration', '-');
    return;
  }
  const effectiveEnd = finishedAt > 0 ? finishedAt : Number(now || 0);
  const duration = Math.max(0, effectiveEnd - startedAt);
  setText('initializationStartedAt', fmtTs(startedAt));
  setText('initializationFinishedAt', finishedAt > 0 ? fmtTs(finishedAt) : t('initializing_running'));
  setText('initializationDuration', fmtDuration(duration));
}
function renderResources(resources) {
  const system = resources?.system || {};
  const processInfo = resources?.process || {};
  setText('systemCpu', fmtPercent(system.cpu_percent));
  setText('systemMemory', `${fmtBytes(system.memory_used_bytes)} / ${fmtBytes(system.memory_total_bytes)} (${fmtPercent(system.memory_percent)})`);
  setText('processCpu', fmtPercent(processInfo.cpu_percent));
  setText('processMemory', fmtBytes(processInfo.memory_bytes));
  setText('processPid', `pid: ${processInfo.pid ?? '-'}`);
}
async function trimMemory() {
  const button = el('memoryTrimButton');
  const resultBox = el('memoryTrimResult');
  button.disabled = true;
  resultBox.textContent = t('trim_memory_running');
  try {
    const response = await fetch('api/admin/memory/trim', { method: 'POST', cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const before = Number(data.before?.process?.memory_bytes || 0);
    const after = Number(data.after?.process?.memory_bytes || 0);
    const diff = before - after;
    if (data.allocator_trim?.supported === false) {
      resultBox.textContent = t('trim_memory_unsupported');
    } else {
      const delta = diff >= 0 ? `-${fmtBytes(diff)}` : `+${fmtBytes(Math.abs(diff))}`;
      resultBox.textContent = `${t('trim_memory_done')}: ${fmtBytes(before)} → ${fmtBytes(after)} (${delta})`;
    }
    renderResources(data.after);
    await loadStatus();
  } catch (err) {
    resultBox.textContent = `${t('trim_memory_failed')}: ${err.message}`;
  } finally {
    button.disabled = false;
  }
}
function renderStatusData(data) {
    latestStatus = data;
    const cfg = data.config || {};
    const progress = data.runtime_progress || {};
    const progressTables = Object.values(progress.tables || {});
    const progressRead = progressTables.reduce((sum, table) => sum + Number(table.read_total || 0), 0);
    const progressSynced = progressTables.reduce((sum, table) => sum + Number(table.synced_total || 0), 0);
    const progressFiltered = progressTables.reduce((sum, table) => sum + Number(table.filtered_total || 0), 0);
    const degraded = data.status === 'degraded';
    const initializing = data.status === 'initializing';
    el('statusPill').classList.toggle('degraded', degraded);
    el('statusPill').classList.toggle('initializing', initializing);
    setText('statusText', displayStatus(data.status));
    setText('sourceType', cfg.source_type);
    setText('sinkType', cfg.sink_type);
    setText('uptime', fmtDuration(data.uptime_seconds));
    setText('startedAt', `${t('started')}: ${fmtTs(data.started_at)}`);
    setText('flushCount', data.timer_flush_count ?? 0);
    setText('lastFlush', `${t('last_flush')}: ${fmtTs(data.last_timer_flush_at)}`);
    setText('restartCount', data.source_restart_count ?? 0);
    setText('lastRestart', `${t('last_restart')}: ${fmtTs(data.last_source_restart_at)}`);
    setText('configScale', `${cfg.source_count ?? 0} → ${cfg.sink_count ?? 0}`);
    setText('pluginCount', `${t('plugins')}: ${cfg.plugin_count ?? 0}`);
    setText('progressState', displayPhase(progress.initializing ? 'initializing' : (progressTables.length > 0 ? 'cdc' : 'running')));
    setText('progressTable', progress.current_table || '-');
    setText('progressReadSynced', `${progressRead} / ${progressSynced}`);
    setText('progressFiltered', progressFiltered);
    renderResources(data.resources);
    renderInitializationTiming(progress, data.now);
    renderColumnInFilters(cfg, progress);
    renderSyncProgressTable(progressTables, data.now);
    el('configKv').innerHTML = [
      kvRow(t('source_type'), cfg.source_type),
      kvRow(t('sink_type'), cfg.sink_type),
      kvRow(t('split_mode'), data.database_split?.enabled ? 'enabled' : 'disabled'),
      kvRow(t('split_task'), data.database_split?.task_name),
      kvRow(t('source_count'), cfg.source_count),
      kvRow(t('sink_count'), cfg.sink_count),
      kvRow(t('plugin_count'), cfg.plugin_count),
      kvRow('Source batch size', cfg.source_batch_size),
      kvRow('Sink batch size', cfg.sink_batch_size),
      kvRow(t('checkpoint_path'), cfg.checkpoint_file_path),
      kvRow(t('auto_create_database'), cfg.auto_create_database),
      kvRow(t('auto_create_table'), cfg.auto_create_table),
      kvRow(t('auto_add_column'), cfg.auto_add_column),
      kvRow(t('auto_modify_column'), cfg.auto_modify_column),
      kvRow('UI bind', cfg.ui_bind),
      kvRow('UI port', cfg.ui_port),
    ].join('');
    const errBox = el('errorBox');
    errBox.textContent = data.last_source_error || t('no_source_error');
    errBox.classList.toggle('empty', !data.last_source_error);
    setText('lastUpdated', fmtTs(data.now));
    el('rawJson').textContent = JSON.stringify(data, null, 2);
}
async function loadStatus() {
  try {
    // Use relative paths so the UI works behind a subpath reverse proxy.
    const response = await fetch('status', { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    renderStatusData(data);
  } catch (err) {
    el('statusPill').classList.add('degraded');
    setText('statusText', t('status_ui_error'));
    el('errorBox').textContent = `${t('load_status_failed')}: ${err.message}`;
    el('errorBox').classList.remove('empty');
  }
}
applyLanguage();
el('langToggle').addEventListener('click', () => {
  currentLang = currentLang === 'zh' ? 'en' : 'zh';
  localStorage.setItem('rustCdcHubLang', currentLang);
  applyLanguage();
  if (latestStatus) renderStatusData(latestStatus);
});
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
            .route("/api/admin/memory/trim", web::post().to(ui_memory_trim))
            .route("/split", web::get().to(ui_split))
            .route("/api/split/status", web::get().to(ui_split_status))
            .route(
                "/api/split/cleanup/dry-run",
                web::post().to(ui_split_cleanup_dry_run),
            )
            .route(
                "/api/split/cleanup/sql",
                web::post().to(ui_split_cleanup_sql),
            )
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
    let config: CdcConfig = load_config(&config_path).unwrap_or_else(|e| {
        panic!(
            "Failed to load config from CONFIG_PATH={}: {}",
            config_path, e
        )
    });
    let log_level = config.log_level.clone().unwrap_or("info".to_string());

    // 设置 tracing 日志格式，自动输出文件名、行号和函数名
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(log_level)
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_names(false)
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
    if let Err(e) = validate_database_split_plugins(&config) {
        error!("{}", e);
        panic!("{}", e);
    }
    if let Err(e) = config.validate_multi_mode() {
        error!("{}", e);
        panic!("{}", e);
    }
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
    ui_state.set_table_info_list(table_info_list.clone()).await;
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
            ui_state.set_table_info_list(table_info_list.clone()).await;
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
            // Control plugins drive UI/mode state and must not enter the CDC event chain.
            if matches!(plugin.plugin_type, PluginType::DatabaseSplit) {
                info!("DatabaseSplit 是控制插件，跳过事件处理链加载");
                continue;
            }
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
    let cwd = env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|e| format!("读取当前目录失败: {}", e));
    let content = fs::read_to_string(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "读取配置文件失败: CONFIG_PATH={}, cwd={}, error={}",
                path, cwd, e
            ),
        )
    })?;

    if path.ends_with(".yaml") || path.ends_with(".yml") {
        let cfg: CdcConfig = serde_yaml::from_str(&content).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("解析 YAML 配置失败: CONFIG_PATH={}, error={}", path, e),
            )
        })?;
        Ok(cfg)
    } else if path.ends_with(".json") {
        let cfg: CdcConfig = serde_json::from_str(&content).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("解析 JSON 配置失败: CONFIG_PATH={}, error={}", path, e),
            )
        })?;
        Ok(cfg)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "不支持的配置文件格式: CONFIG_PATH={}, 只支持 .yaml/.yml/.json",
                path
            ),
        )
        .into())
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

    fn test_split_config() -> CdcConfig {
        serde_yaml::from_str(
            r#"
source_type: MySQL
sink_type: Print
source_config:
  - host: 127.0.0.1
    port: 3306
    database: source_shard_01
    table_name: "*"
    except_table_name_prefix: tmp_
    server_id: 10010
sink_config:
  - host: 127.0.0.2
    port: 3306
    database: target_shard_08
plugins:
  - plugin_type: ColumnIn
    config:
      columns: project_id,tenant_id
      values: 10001,10002
  - plugin_type: DatabaseSplit
    config:
      task_name: split-project-10001-10002
      mode: shard_data_split
      cleanup_strategy: generate_sql
      cleanup_confirm_phrase: I_UNDERSTAND_THE_RISK
"#,
        )
        .unwrap()
    }

    fn test_split_plus_config() -> CdcConfig {
        serde_yaml::from_str(
            r#"
source_type: MySQL
sink_type: MySQL
source_config:
  - {}
sink_config:
  - {}
plugins:
  - plugin_type: ColumnIn
    config:
      columns: project_id
      values: 10001
  - plugin_type: Plus
    config:
      columns: orders.id
      plus: 1
  - plugin_type: DatabaseSplit
    config:
      task_name: split-project
"#,
        )
        .unwrap()
    }

    fn test_table_info_list() -> Vec<TableInfoVo> {
        vec![
            TableInfoVo {
                source_database: "source_shard_01".to_string(),
                target_database: "target_shard_08".to_string(),
                table_name: "orders".to_string(),
                pk_column: "id".to_string(),
                create_table_sql: "".to_string(),
                columns: vec!["id".to_string(), "project_id".to_string()],
            },
            TableInfoVo {
                source_database: "source_shard_01".to_string(),
                target_database: "target_shard_08".to_string(),
                table_name: "tenant_map".to_string(),
                pk_column: "id".to_string(),
                create_table_sql: "".to_string(),
                columns: vec![
                    "id".to_string(),
                    "project_id".to_string(),
                    "tenant_id".to_string(),
                ],
            },
        ]
    }

    #[test]
    fn test_resolve_ui_port_env_overrides() {
        let cfg = test_config();
        assert_eq!(resolve_ui_port(&cfg, Some(18080)), 18080);
    }

    #[test]
    fn test_resolve_ui_port_default_is_18088() {
        let cfg = test_config();
        assert_eq!(resolve_ui_port(&cfg, None), 18088);
    }

    #[test]
    fn test_resolve_ui_port_config_overrides_default() {
        let mut cfg = test_config();
        cfg.ui_port = Some(19000);
        assert_eq!(resolve_ui_port(&cfg, None), 19000);
    }

    #[test]
    fn test_database_split_config_is_sanitized() {
        let cfg = test_split_config();
        let split = database_split_summary(&cfg);
        assert_eq!(split["enabled"].as_bool().unwrap(), true);
        assert_eq!(
            split["task_name"].as_str().unwrap(),
            "split-project-10001-10002"
        );
        assert_eq!(split["mode"].as_str().unwrap(), "shard_data_split");
        assert_eq!(split["cleanup_strategy"].as_str().unwrap(), "generate_sql");
        assert_eq!(
            split["cleanup_confirm_phrase"]["configured"]
                .as_bool()
                .unwrap(),
            true
        );
        let serialized = serde_json::to_string(&split).unwrap();
        assert!(!serialized.contains("I_UNDERSTAND_THE_RISK"));
    }

    #[test]
    fn test_database_split_disabled_by_default() {
        let cfg = test_config();
        let split = database_split_summary(&cfg);
        assert_eq!(split["enabled"].as_bool().unwrap(), false);
    }

    #[test]
    fn test_plugin_summary_contains_column_in_safe_config() {
        let cfg = test_split_config();
        let plugins = plugin_summary(&cfg);
        let column_in = plugins
            .as_array()
            .unwrap()
            .iter()
            .find(|plugin| plugin["plugin_type"].as_str().unwrap() == "ColumnIn")
            .unwrap();
        assert_eq!(column_in["columns"].as_array().unwrap().len(), 2);
        assert_eq!(column_in["values"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_cleanup_targets_identify_tables_and_invalid_multiple_columns() {
        let cfg = test_split_config();
        let (targets, invalid) = cleanup_targets(&cfg, &test_table_info_list());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].table_name, "orders");
        assert_eq!(targets[0].pk_column, "id");
        assert_eq!(targets[0].filter_column, "project_id");
        assert_eq!(targets[0].filter_values, vec!["10001", "10002"]);
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].table_name, "tenant_map");
        assert_eq!(invalid[0].reason, "matched multiple ColumnIn columns");
    }

    #[test]
    fn test_build_delete_sql_batches_escapes_values() {
        let target = CleanupTarget {
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            filter_column: "project_id".to_string(),
            filter_values: vec!["10001".to_string(), "x'y".to_string()],
        };
        let sql = build_delete_sql_batches(
            &target,
            &["1".to_string(), "2".to_string(), "3".to_string()],
            2,
        );
        assert!(sql.contains("-- table: orders, filter: project_id IN ('10001', 'x\\'y')"));
        assert!(sql.contains("DELETE FROM `orders` WHERE `id` IN ('1', '2');"));
        assert!(sql.contains("DELETE FROM `orders` WHERE `id` IN ('3');"));
    }

    #[test]
    fn test_database_split_plus_is_rejected() {
        let cfg = test_split_plus_config();
        assert!(validate_database_split_plugins(&cfg).is_err());
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
        assert!(v.get("runtime_progress").is_some());
        let resources = v.get("resources").unwrap();
        assert!(resources.get("system").is_some());
        assert!(resources.get("process").is_some());
        assert!(
            resources
                .get("system")
                .unwrap()
                .get("cpu_percent")
                .unwrap()
                .is_number()
        );
        assert!(
            resources
                .get("system")
                .unwrap()
                .get("memory_total_bytes")
                .unwrap()
                .is_number()
        );
        assert_eq!(
            resources
                .get("process")
                .unwrap()
                .get("pid")
                .unwrap()
                .as_u64()
                .unwrap(),
            process::id() as u64
        );
        assert!(
            v.get("runtime_progress")
                .unwrap()
                .get("plugin_filters")
                .is_some()
        );
        assert!(
            v.get("runtime_progress")
                .unwrap()
                .get("initialization_started_at")
                .is_some()
        );
        assert!(
            v.get("runtime_progress")
                .unwrap()
                .get("initialization_finished_at")
                .is_some()
        );
        assert_eq!(
            v.get("database_split")
                .unwrap()
                .get("enabled")
                .unwrap()
                .as_bool()
                .unwrap(),
            false
        );
    }

    #[actix_web::test]
    async fn test_ui_memory_trim_returns_resource_snapshots() {
        let cfg = test_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/api/admin/memory/trim", web::post().to(ui_memory_trim)),
        )
        .await;

        let req = actix_test::TestRequest::post()
            .uri("/api/admin/memory/trim")
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.get("status").unwrap().as_str().unwrap(), "ok");
        assert!(v.get("allocator_trim").is_some());
        assert!(v.get("before").unwrap().get("process").is_some());
        assert!(v.get("after").unwrap().get("process").is_some());
    }

    #[actix_web::test]
    async fn test_ui_split_status_enabled() {
        let cfg = test_split_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        state.set_table_info_list(test_table_info_list()).await;
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/api/split/status", web::get().to(ui_split_status)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/api/split/status")
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.get("enabled").unwrap().as_bool().unwrap(), true);
        assert!(v.get("runtime_progress").is_some());
        assert!(v.get("resources").unwrap().get("system").is_some());
        assert!(v.get("resources").unwrap().get("process").is_some());
        assert_eq!(
            v.get("cleanup")
                .unwrap()
                .get("tables")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            v.get("filter")
                .unwrap()
                .get("columns")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            v.get("runtime_progress")
                .unwrap()
                .get("plugin_filters")
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            0
        );
    }

    #[actix_web::test]
    async fn test_ui_split_status_disabled_returns_404() {
        let cfg = test_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/api/split/status", web::get().to(ui_split_status))
                .route(
                    "/api/split/cleanup/dry-run",
                    web::post().to(ui_split_cleanup_dry_run),
                )
                .route("/split", web::get().to(ui_split)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/api/split/status")
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let req = actix_test::TestRequest::get().uri("/split").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let req = actix_test::TestRequest::post()
            .uri("/api/split/cleanup/dry-run")
            .set_json(json!({}))
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
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
        assert!(s.contains("href=\"status\""));
        assert!(s.contains("fetch('status'"));
        assert!(!s.contains("fetch('/status'"));
        assert!(s.contains("Prometheus 指标"));
        assert!(s.contains("数据同步进度"));
        assert!(s.contains("syncProgressTable"));
        assert!(s.contains("initializationTimingGrid"));
        assert!(s.contains("初始化开始时间"));
        assert!(s.contains("Initialization Duration"));
        assert!(s.contains("function tableInitializationDurationSeconds"));
        assert!(s.contains("progress.initializing ? 0"));
        assert!(s.contains("sortHeader('syncProgress', 'initialization_duration'"));
        assert!(s.contains("langToggle"));
        assert!(s.contains("Data Sync Progress"));
        assert!(s.contains("资源占用"));
        assert!(s.contains("机器 CPU"));
        assert!(s.contains("进程内存"));
        assert!(s.contains("Resource Usage"));
        assert!(s.contains("function renderResources"));
        assert!(s.contains("触发内存回收"));
        assert!(s.contains("memoryTrimButton"));
        assert!(s.contains("function trimMemory"));
        assert!(s.contains("api/admin/memory/trim"));
        assert!(s.contains("tableSortState"));
        assert!(s.contains("syncProgress: { key: 'last_event_at', dir: 'desc' }"));
        assert!(s.contains("function setTableSort"));
        assert!(s.contains("onclick=\"setTableSort"));
        assert!(s.contains("position: sticky"));
        assert!(s.contains("phase-initializing"));
        assert!(s.contains("status_running: '运行中'"));
        assert!(s.contains("phase_initializing: '初始化中'"));
        assert!(s.contains("function displayStatus"));
        assert!(s.contains("function displayPhase"));
        assert!(s.contains("rawJson"));
        assert!(!s.contains("id=\"pluginFilterSection\""));
        assert!(!s.contains("数据库拆分</a>"));
    }

    #[actix_web::test]
    async fn test_ui_root_html_contains_split_entry_when_enabled() {
        let cfg = test_split_config();
        let state = UiState::new_with_started_at(&cfg, 123);
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/", web::get().to(ui_root))
                .route("/split", web::get().to(ui_split)),
        )
        .await;

        let req = actix_test::TestRequest::get().uri("/").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("数据库拆分</a>"));
        assert!(s.contains("插件过滤"));
        assert!(s.contains("id=\"pluginFilterSection\""));
        assert!(s.contains("columnInFilterTable"));
        assert!(s.contains("pluginFilters: { key: 'last_event_at', dir: 'desc' }"));
        assert!(s.contains("sortHeader('pluginFilters'"));
        assert!(s.contains("input_total"));
        assert!(s.contains("output_total"));
        assert!(s.contains("Input Rows"));
        assert!(s.contains("未命中过滤字段"));
        assert!(s.contains("No matched filter column"));

        let req = actix_test::TestRequest::get().uri("/split").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("progressTableWrap"));
        assert!(s.contains("清理 SQL"));
        assert!(s.contains("Dry-run 统计"));
        assert!(s.contains("下载删除 SQL"));
        assert!(s.contains("renderDryRunResult"));
        assert!(s.contains("待删除条数"));
        assert!(s.contains("api/split/cleanup/dry-run"));
        assert!(s.contains("api/split/cleanup/sql"));
        assert!(s.contains("fetch('api/split/status'"));
        assert!(!s.contains("fetch('/api/split/status'"));
        assert!(s.contains("资源占用"));
        assert!(s.contains("splitSystemCpu"));
        assert!(s.contains("splitProcessMemory"));
        assert!(s.contains("function renderSplitResources"));
    }

    #[test]
    fn test_load_config_missing_file_mentions_path() {
        let path = format!(
            "/tmp/rust_cdc_hub_missing_config_{}.yaml",
            std::process::id()
        );

        let err = load_config(path.as_str()).unwrap_err().to_string();

        assert!(err.contains(path.as_str()));
        assert!(err.contains("CONFIG_PATH="));
        assert!(err.contains("cwd="));
    }
}
