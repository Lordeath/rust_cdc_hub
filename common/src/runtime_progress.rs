use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

lazy_static! {
    static ref RUNTIME_PROGRESS: Mutex<RuntimeProgress> = Mutex::new(RuntimeProgress::new());
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProgress {
    pub initializing: bool,
    pub current_table: String,
    pub initialization_started_at: i64,
    pub initialization_finished_at: i64,
    #[serde(default)]
    pub schema_running: bool,
    #[serde(default)]
    pub schema_current_stage: String,
    #[serde(default)]
    pub schema_started_at: i64,
    #[serde(default)]
    pub schema_finished_at: i64,
    #[serde(default)]
    pub schema_stages: BTreeMap<String, SchemaStageProgress>,
    pub tables: BTreeMap<String, TableProgress>,
    pub plugin_filters: BTreeMap<String, PluginFilterProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaStageProgress {
    pub stage: String,
    pub label: String,
    pub running: bool,
    #[serde(default)]
    pub started_at: i64,
    #[serde(default)]
    pub finished_at: i64,
    #[serde(default)]
    pub duration_seconds: i64,
    pub item_total: u64,
    pub item_done: u64,
    pub error_total: u64,
    pub last_item: String,
    pub last_error: String,
    pub last_event_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableProgress {
    pub table_name: String,
    pub phase: String,
    #[serde(default)]
    pub initialization_started_at: i64,
    #[serde(default)]
    pub initialization_finished_at: i64,
    pub read_total: u64,
    pub synced_total: u64,
    pub filtered_total: u64,
    pub last_pk: String,
    pub last_event_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginFilterProgress {
    pub plugin_name: String,
    pub table_name: String,
    pub column_name: String,
    pub input_total: u64,
    pub output_total: u64,
    pub filtered_total: u64,
    pub last_event_at: i64,
}

impl RuntimeProgress {
    pub fn new() -> Self {
        RuntimeProgress {
            initializing: false,
            current_table: String::new(),
            initialization_started_at: 0,
            initialization_finished_at: 0,
            schema_running: false,
            schema_current_stage: String::new(),
            schema_started_at: 0,
            schema_finished_at: 0,
            schema_stages: BTreeMap::new(),
            tables: BTreeMap::new(),
            plugin_filters: BTreeMap::new(),
        }
    }

    pub fn begin_schema_stage(&mut self, stage: &str, label: &str, item_total: u64, now: i64) {
        self.schema_running = true;
        self.schema_current_stage = stage.to_string();
        self.schema_finished_at = 0;
        if self.schema_started_at == 0 {
            self.schema_started_at = now;
        }

        let stage_progress = self
            .schema_stages
            .entry(stage.to_string())
            .or_insert_with(|| SchemaStageProgress {
                stage: stage.to_string(),
                label: label.to_string(),
                running: false,
                started_at: 0,
                finished_at: 0,
                duration_seconds: 0,
                item_total: 0,
                item_done: 0,
                error_total: 0,
                last_item: String::new(),
                last_error: String::new(),
                last_event_at: 0,
            });
        stage_progress.label = label.to_string();
        stage_progress.running = true;
        stage_progress.started_at = now;
        stage_progress.finished_at = 0;
        stage_progress.item_total = stage_progress.item_total.saturating_add(item_total);
        stage_progress.last_event_at = now;
    }

    pub fn record_schema_stage_item(&mut self, stage: &str, item: &str, now: i64) {
        let stage_progress = self.schema_stage_mut(stage, now);
        stage_progress.item_done = stage_progress.item_done.saturating_add(1);
        stage_progress.last_item = item.to_string();
        stage_progress.last_event_at = now;
        if stage_progress.running {
            self.schema_running = true;
            self.schema_current_stage = stage.to_string();
            self.schema_finished_at = 0;
        }
    }

    pub fn record_schema_stage_error(&mut self, stage: &str, item: &str, error: &str, now: i64) {
        let stage_progress = self.schema_stage_mut(stage, now);
        stage_progress.error_total = stage_progress.error_total.saturating_add(1);
        stage_progress.last_item = item.to_string();
        stage_progress.last_error = error.chars().take(500).collect();
        stage_progress.last_event_at = now;
        if stage_progress.running {
            self.schema_running = true;
            self.schema_current_stage = stage.to_string();
            self.schema_finished_at = 0;
        }
    }

    pub fn finish_schema_stage(&mut self, stage: &str, now: i64) {
        if let Some(stage_progress) = self.schema_stages.get_mut(stage) {
            if stage_progress.started_at == 0 {
                stage_progress.started_at = now;
            }
            if stage_progress.running {
                stage_progress.duration_seconds = stage_progress
                    .duration_seconds
                    .saturating_add(now.saturating_sub(stage_progress.started_at));
            }
            stage_progress.running = false;
            stage_progress.finished_at = now;
            stage_progress.last_event_at = now;
        }
        self.refresh_schema_current_stage(now);
    }

    pub fn finish_schema_initialization(&mut self, now: i64) {
        if self.schema_started_at == 0 {
            return;
        }
        for stage_progress in self.schema_stages.values_mut() {
            if stage_progress.running {
                stage_progress.duration_seconds = stage_progress
                    .duration_seconds
                    .saturating_add(now.saturating_sub(stage_progress.started_at));
                stage_progress.running = false;
                stage_progress.finished_at = now;
                stage_progress.last_event_at = now;
            }
        }
        self.schema_running = false;
        self.schema_current_stage.clear();
        self.schema_finished_at = now;
    }

    pub fn begin_table_initialization(&mut self, table_name: &str, now: i64) {
        self.initializing = true;
        self.current_table = table_name.to_string();
        self.initialization_finished_at = 0;
        if self.initialization_started_at == 0 {
            self.initialization_started_at = now;
        }
        let table = self.table_mut(table_name);
        table.phase = "initializing".to_string();
        table.initialization_started_at = now;
        table.initialization_finished_at = 0;
        table.last_event_at = now;
    }

    pub fn finish_table_initialization(&mut self, table_name: &str, now: i64) {
        let table = self.table_mut(table_name);
        if table.initialization_started_at == 0 {
            table.initialization_started_at = now;
        }
        table.initialization_finished_at = now;
        table.phase = "done".to_string();
        table.last_event_at = now;
        self.refresh_current_table();
    }

    pub fn finish_initialization(&mut self, now: i64) {
        self.initializing = false;
        self.current_table.clear();
        self.initialization_finished_at = now;
    }

    pub fn record_read(
        &mut self,
        table_name: &str,
        phase: &str,
        last_pk: Option<String>,
        now: i64,
    ) {
        let table = self.table_mut(table_name);
        table.read_total += 1;
        table.phase = phase.to_string();
        if phase == "initializing" && table.initialization_started_at == 0 {
            table.initialization_started_at = now;
        }
        if let Some(pk) = last_pk
            && !pk.is_empty()
        {
            table.last_pk = pk;
        }
        table.last_event_at = now;
    }

    pub fn record_synced(&mut self, table_name: &str, now: i64) {
        let table = self.table_mut(table_name);
        table.synced_total += 1;
        table.last_event_at = now;
    }

    pub fn record_filtered(&mut self, table_name: &str, now: i64) {
        let table = self.table_mut(table_name);
        table.filtered_total += 1;
        table.last_event_at = now;
    }

    pub fn record_plugin_filter_result(
        &mut self,
        plugin_name: &str,
        table_name: &str,
        column_name: &str,
        passed: bool,
        now: i64,
    ) {
        let key = format!("{}|{}|{}", plugin_name, table_name, column_name);
        let filter = self
            .plugin_filters
            .entry(key)
            .or_insert_with(|| PluginFilterProgress {
                plugin_name: plugin_name.to_string(),
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
                input_total: 0,
                output_total: 0,
                filtered_total: 0,
                last_event_at: 0,
            });
        filter.input_total += 1;
        if passed {
            filter.output_total += 1;
        } else {
            filter.filtered_total += 1;
        }
        filter.last_event_at = now;
    }

    fn table_mut(&mut self, table_name: &str) -> &mut TableProgress {
        self.tables
            .entry(table_name.to_string())
            .or_insert_with(|| TableProgress {
                table_name: table_name.to_string(),
                phase: "cdc".to_string(),
                initialization_started_at: 0,
                initialization_finished_at: 0,
                read_total: 0,
                synced_total: 0,
                filtered_total: 0,
                last_pk: String::new(),
                last_event_at: 0,
            })
    }

    fn schema_stage_mut(&mut self, stage: &str, now: i64) -> &mut SchemaStageProgress {
        if self.schema_started_at == 0 {
            self.schema_started_at = now;
        }
        self.schema_stages
            .entry(stage.to_string())
            .or_insert_with(|| SchemaStageProgress {
                stage: stage.to_string(),
                label: stage.to_string(),
                running: false,
                started_at: now,
                finished_at: 0,
                duration_seconds: 0,
                item_total: 0,
                item_done: 0,
                error_total: 0,
                last_item: String::new(),
                last_error: String::new(),
                last_event_at: now,
            })
    }

    fn refresh_current_table(&mut self) {
        let current_is_initializing = self
            .tables
            .get(self.current_table.as_str())
            .map(|table| table.phase == "initializing")
            .unwrap_or(false);
        if current_is_initializing {
            return;
        }
        self.current_table = self
            .tables
            .values()
            .filter(|table| table.phase == "initializing")
            .max_by_key(|table| table.last_event_at)
            .map(|table| table.table_name.clone())
            .unwrap_or_default();
    }

    fn refresh_schema_current_stage(&mut self, now: i64) {
        self.schema_current_stage = self
            .schema_stages
            .values()
            .filter(|stage| stage.running)
            .max_by_key(|stage| stage.last_event_at)
            .map(|stage| stage.stage.clone())
            .unwrap_or_default();
        self.schema_running = !self.schema_current_stage.is_empty();
        if !self.schema_running && self.schema_started_at > 0 {
            self.schema_finished_at = now;
        }
    }
}

impl Default for RuntimeProgress {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn begin_table_initialization(table_name: &str) {
    // Record initialization progress for the Actix UI runtime status.
    RUNTIME_PROGRESS
        .lock()
        .await
        .begin_table_initialization(table_name, chrono::Utc::now().timestamp());
}

pub async fn finish_table_initialization(table_name: &str) {
    RUNTIME_PROGRESS
        .lock()
        .await
        .finish_table_initialization(table_name, chrono::Utc::now().timestamp());
}

pub async fn finish_initialization() {
    RUNTIME_PROGRESS
        .lock()
        .await
        .finish_initialization(chrono::Utc::now().timestamp());
}

pub async fn begin_schema_stage(stage: &str, label: &str, item_total: u64) {
    RUNTIME_PROGRESS.lock().await.begin_schema_stage(
        stage,
        label,
        item_total,
        chrono::Utc::now().timestamp(),
    );
}

pub async fn record_schema_stage_item(stage: &str, item: &str) {
    RUNTIME_PROGRESS.lock().await.record_schema_stage_item(
        stage,
        item,
        chrono::Utc::now().timestamp(),
    );
}

pub async fn record_schema_stage_error(stage: &str, item: &str, error: &str) {
    RUNTIME_PROGRESS.lock().await.record_schema_stage_error(
        stage,
        item,
        error,
        chrono::Utc::now().timestamp(),
    );
}

pub async fn finish_schema_stage(stage: &str) {
    RUNTIME_PROGRESS
        .lock()
        .await
        .finish_schema_stage(stage, chrono::Utc::now().timestamp());
}

pub async fn finish_schema_initialization() {
    RUNTIME_PROGRESS
        .lock()
        .await
        .finish_schema_initialization(chrono::Utc::now().timestamp());
}

pub async fn record_read(table_name: &str, phase: &str, last_pk: Option<String>) {
    RUNTIME_PROGRESS.lock().await.record_read(
        table_name,
        phase,
        last_pk,
        chrono::Utc::now().timestamp(),
    );
}

pub async fn record_synced(table_name: &str) {
    RUNTIME_PROGRESS
        .lock()
        .await
        .record_synced(table_name, chrono::Utc::now().timestamp());
}

pub async fn record_filtered(table_name: &str) {
    RUNTIME_PROGRESS
        .lock()
        .await
        .record_filtered(table_name, chrono::Utc::now().timestamp());
}

pub async fn record_plugin_filter_result(
    plugin_name: &str,
    table_name: &str,
    column_name: &str,
    passed: bool,
) {
    // Record the actual plugin filter field so the Dashboard can explain filter sources.
    RUNTIME_PROGRESS.lock().await.record_plugin_filter_result(
        plugin_name,
        table_name,
        column_name,
        passed,
        chrono::Utc::now().timestamp(),
    );
}

pub async fn snapshot() -> RuntimeProgress {
    RUNTIME_PROGRESS.lock().await.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_progress_defaults_to_not_initializing() {
        let progress = RuntimeProgress::new();
        assert!(!progress.initializing);
        assert!(progress.current_table.is_empty());
        assert_eq!(progress.initialization_started_at, 0);
        assert_eq!(progress.initialization_finished_at, 0);
        assert!(!progress.schema_running);
        assert!(progress.schema_current_stage.is_empty());
        assert_eq!(progress.schema_started_at, 0);
        assert_eq!(progress.schema_finished_at, 0);
        assert!(progress.schema_stages.is_empty());
        assert!(progress.tables.is_empty());
        assert!(progress.plugin_filters.is_empty());
    }

    #[test]
    fn runtime_progress_counts_by_table() {
        let mut progress = RuntimeProgress::new();
        progress.begin_table_initialization("orders", 10);
        progress.record_read("orders", "initializing", Some("1".to_string()), 11);
        progress.record_synced("orders", 12);
        progress.record_filtered("orders", 13);
        progress.finish_table_initialization("orders", 14);

        let table = progress.tables.get("orders").unwrap();
        assert_eq!(progress.current_table, "");
        assert_eq!(table.phase, "done");
        assert_eq!(table.initialization_started_at, 10);
        assert_eq!(table.initialization_finished_at, 14);
        assert_eq!(table.read_total, 1);
        assert_eq!(table.synced_total, 1);
        assert_eq!(table.filtered_total, 1);
        assert_eq!(table.last_pk, "1");
    }

    #[test]
    fn runtime_progress_keeps_current_table_for_parallel_initialization() {
        let mut progress = RuntimeProgress::new();
        progress.begin_table_initialization("db_a.orders", 10);
        progress.begin_table_initialization("db_b.orders", 11);

        progress.finish_table_initialization("db_b.orders", 12);

        assert_eq!(progress.current_table, "db_a.orders");
        assert!(progress.initializing);
        assert_eq!(progress.initialization_finished_at, 0);
        assert_eq!(
            progress
                .tables
                .get("db_b.orders")
                .unwrap()
                .initialization_started_at,
            11
        );
        assert_eq!(
            progress
                .tables
                .get("db_b.orders")
                .unwrap()
                .initialization_finished_at,
            12
        );

        progress.finish_table_initialization("db_a.orders", 13);
        progress.finish_initialization(14);

        assert!(progress.current_table.is_empty());
        assert!(!progress.initializing);
        assert_eq!(progress.initialization_started_at, 10);
        assert_eq!(progress.initialization_finished_at, 14);
        assert_eq!(
            progress
                .tables
                .get("db_a.orders")
                .unwrap()
                .initialization_finished_at,
            13
        );
    }

    #[test]
    fn runtime_progress_counts_plugin_filters_by_table_and_column() {
        let mut progress = RuntimeProgress::new();
        progress.record_plugin_filter_result("ColumnIn", "orders", "project_id", true, 10);
        progress.record_plugin_filter_result("ColumnIn", "orders", "project_id", false, 11);
        progress.record_plugin_filter_result("ColumnIn", "charges", "tenant_id", false, 12);

        let orders = progress
            .plugin_filters
            .get("ColumnIn|orders|project_id")
            .unwrap();
        let charges = progress
            .plugin_filters
            .get("ColumnIn|charges|tenant_id")
            .unwrap();
        assert_eq!(orders.input_total, 2);
        assert_eq!(orders.output_total, 1);
        assert_eq!(orders.filtered_total, 1);
        assert_eq!(orders.column_name, "project_id");
        assert_eq!(charges.input_total, 1);
        assert_eq!(charges.output_total, 0);
        assert_eq!(charges.filtered_total, 1);
        assert_eq!(charges.table_name, "charges");
    }

    #[test]
    fn runtime_progress_tracks_schema_stage() {
        let mut progress = RuntimeProgress::new();
        progress.begin_schema_stage("source.mysql.tables", "MySQL source 表发现", 2, 10);
        progress.record_schema_stage_item("source.mysql.tables", "orders", 11);
        progress.record_schema_stage_error("source.mysql.tables", "charges", "query failed", 12);

        assert!(progress.schema_running);
        assert_eq!(progress.schema_current_stage, "source.mysql.tables");
        assert_eq!(progress.schema_started_at, 10);
        assert_eq!(progress.schema_finished_at, 0);

        progress.finish_schema_stage("source.mysql.tables", 15);
        let stage = progress.schema_stages.get("source.mysql.tables").unwrap();
        assert!(!stage.running);
        assert_eq!(stage.item_total, 2);
        assert_eq!(stage.item_done, 1);
        assert_eq!(stage.error_total, 1);
        assert_eq!(stage.last_item, "charges");
        assert_eq!(stage.last_error, "query failed");
        assert_eq!(stage.duration_seconds, 5);
        assert!(!progress.schema_running);
        assert!(progress.schema_current_stage.is_empty());
        assert_eq!(progress.schema_finished_at, 15);
    }

    #[test]
    fn runtime_progress_finishes_running_schema_stages() {
        let mut progress = RuntimeProgress::new();
        progress.begin_schema_stage("sink.mysql.create_table", "MySQL sink 自动建表", 1, 20);

        progress.finish_schema_initialization(24);

        let stage = progress
            .schema_stages
            .get("sink.mysql.create_table")
            .unwrap();
        assert!(!stage.running);
        assert_eq!(stage.finished_at, 24);
        assert_eq!(stage.duration_seconds, 4);
        assert!(!progress.schema_running);
        assert_eq!(progress.schema_finished_at, 24);
    }
}
