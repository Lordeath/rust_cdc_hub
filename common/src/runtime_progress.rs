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
    pub tables: BTreeMap<String, TableProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableProgress {
    pub table_name: String,
    pub phase: String,
    pub read_total: u64,
    pub synced_total: u64,
    pub filtered_total: u64,
    pub last_pk: String,
    pub last_event_at: i64,
}

impl RuntimeProgress {
    pub fn new() -> Self {
        RuntimeProgress {
            initializing: false,
            current_table: String::new(),
            initialization_started_at: 0,
            initialization_finished_at: 0,
            tables: BTreeMap::new(),
        }
    }

    pub fn begin_table_initialization(&mut self, table_name: &str, now: i64) {
        self.initializing = true;
        self.current_table = table_name.to_string();
        if self.initialization_started_at == 0 {
            self.initialization_started_at = now;
        }
        let table = self.table_mut(table_name);
        table.phase = "initializing".to_string();
        table.last_event_at = now;
    }

    pub fn finish_table_initialization(&mut self, table_name: &str, now: i64) {
        let table = self.table_mut(table_name);
        table.phase = "done".to_string();
        table.last_event_at = now;
        if self.current_table.eq_ignore_ascii_case(table_name) {
            self.current_table.clear();
        }
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

    fn table_mut(&mut self, table_name: &str) -> &mut TableProgress {
        self.tables
            .entry(table_name.to_string())
            .or_insert_with(|| TableProgress {
                table_name: table_name.to_string(),
                phase: "cdc".to_string(),
                read_total: 0,
                synced_total: 0,
                filtered_total: 0,
                last_pk: String::new(),
                last_event_at: 0,
            })
    }
}

impl Default for RuntimeProgress {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn begin_table_initialization(table_name: &str) {
    // Modified By Codex 20260508 ITOPS-130877 记录初始化表进度供 Actix UI 展示
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
        assert!(progress.tables.is_empty());
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
        assert_eq!(table.read_total, 1);
        assert_eq!(table.synced_total, 1);
        assert_eq!(table.filtered_total, 1);
        assert_eq!(table.last_pk, "1");
    }
}
