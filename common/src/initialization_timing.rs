//! 初始化数据库操作计时。仅在状态更新时持锁，操作跨 await 或被取消均可结算。
use crate::runtime_progress::RuntimeProgress;
use crate::{DataBuffer, Operation};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

lazy_static! {
    static ref TIMINGS: Mutex<TimingState> = Mutex::new(TimingState::default());
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct InitializationTimingProgress {
    pub initialization_query_duration_ms: u64,
    pub initialization_write_duration_ms: u64,
    pub initialization_query_active: bool,
    pub initialization_write_active: bool,
    pub initialization_write_estimated: bool,
}

#[derive(Clone, Copy)]
enum Kind {
    Query,
    Write,
}

#[derive(Clone, Default)]
struct Accumulated {
    query_ns: u128,
    write_ns: u128,
    query_active: bool,
    write_active: bool,
    estimated: bool,
}

impl Accumulated {
    fn progress(&self) -> InitializationTimingProgress {
        InitializationTimingProgress {
            initialization_query_duration_ms: (self.query_ns / 1_000_000).min(u64::MAX as u128)
                as u64,
            initialization_write_duration_ms: (self.write_ns / 1_000_000).min(u64::MAX as u128)
                as u64,
            initialization_query_active: self.query_active,
            initialization_write_active: self.write_active,
            initialization_write_estimated: self.estimated,
        }
    }
}

struct ActiveOperation {
    kind: Kind,
    batch: WriteTimingBatch,
    started_at: Instant,
}

impl ActiveOperation {
    fn apply(&self, tables: &mut BTreeMap<String, Accumulated>, now: Instant, active: bool) {
        let elapsed = now.saturating_duration_since(self.started_at).as_nanos();
        let estimated = self.batch.weights.len() > 1
            || self.batch.weights.values().sum::<u64>() < self.batch.rows;
        // 用累计权重分配余数，避免小批次的取整误差逐次丢失。
        let mut weight_before = 0u128;
        for (table_name, weight) in &self.batch.weights {
            let weight_after = weight_before + u128::from(*weight);
            let duration = elapsed * weight_after / u128::from(self.batch.rows)
                - elapsed * weight_before / u128::from(self.batch.rows);
            weight_before = weight_after;
            let table = tables.entry(table_name.clone()).or_default();
            match self.kind {
                Kind::Query => {
                    table.query_ns = table.query_ns.saturating_add(duration);
                    table.query_active |= active;
                }
                Kind::Write => {
                    table.write_ns = table.write_ns.saturating_add(duration);
                    table.write_active |= active;
                    table.estimated |= estimated;
                }
            }
        }
    }
}

#[derive(Default)]
struct TimingState {
    next_id: u64,
    completed: BTreeMap<String, Accumulated>,
    active: BTreeMap<u64, ActiveOperation>,
}

impl TimingState {
    fn start(&mut self, kind: Kind, batch: WriteTimingBatch, now: Instant) -> u64 {
        self.next_id += 1;
        self.active.insert(
            self.next_id,
            ActiveOperation {
                kind,
                batch,
                started_at: now,
            },
        );
        self.next_id
    }

    fn finish(&mut self, id: u64, now: Instant) {
        if let Some(operation) = self.active.remove(&id) {
            operation.apply(&mut self.completed, now, false);
        }
    }

    fn snapshot_into(&self, progress: &mut RuntimeProgress, now: Instant) {
        let mut tables = self.completed.clone();
        for operation in self.active.values() {
            operation.apply(&mut tables, now, true);
        }
        let mut total = Accumulated::default();
        for (table_name, timing) in tables {
            total.query_ns = total.query_ns.saturating_add(timing.query_ns);
            total.write_ns = total.write_ns.saturating_add(timing.write_ns);
            total.query_active |= timing.query_active;
            total.write_active |= timing.write_active;
            progress.table_mut(&table_name).initialization_timing = timing.progress();
        }
        // 汇总按每次实际请求计一次；估算标记只用于表级分摊。
        progress.initialization_timing = total.progress();
    }
}

/// 保存参与实际请求的源表行数，分组前收集即可，无需额外复制记录内容。
#[derive(Clone, Default)]
pub struct WriteTimingBatch {
    weights: BTreeMap<String, u64>,
    rows: u64,
}

impl WriteTimingBatch {
    pub fn add(&mut self, record: &DataBuffer) {
        self.rows += 1;
        if matches!(record.op, Operation::CREATE(true)) {
            *self.weights.entry(record.source_table_key()).or_default() += 1;
        }
    }

    pub fn start(&self) -> OperationTiming {
        start(Kind::Write, self.clone())
    }
}

/// Drop 同步结算，失败返回、panic 和异步任务取消均不会留下活动计时。
pub struct OperationTiming(Option<u64>);

impl Drop for OperationTiming {
    fn drop(&mut self) {
        if let Some(id) = self.0 {
            TIMINGS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .finish(id, Instant::now());
        }
    }
}

fn start(kind: Kind, batch: WriteTimingBatch) -> OperationTiming {
    if batch.weights.is_empty() {
        return OperationTiming(None);
    }
    let id = TIMINGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .start(kind, batch, Instant::now());
    OperationTiming(Some(id))
}

pub fn track_query(table_name: &str) -> OperationTiming {
    start(
        Kind::Query,
        WriteTimingBatch {
            weights: BTreeMap::from([(table_name.to_string(), 1)]),
            rows: 1,
        },
    )
}

pub fn track_write(records: &[DataBuffer]) -> OperationTiming {
    let mut batch = WriteTimingBatch::default();
    for record in records {
        batch.add(record);
    }
    batch.start()
}

pub(crate) fn snapshot_into(progress: &mut RuntimeProgress) {
    TIMINGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .snapshot_into(progress, Instant::now());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case_insensitive_hash_map::CaseInsensitiveHashMap;
    use std::time::Duration;

    fn batch(weights: &[(&str, u64)]) -> WriteTimingBatch {
        WriteTimingBatch {
            weights: weights
                .iter()
                .map(|(name, count)| (name.to_string(), *count))
                .collect(),
            rows: weights.iter().map(|(_, count)| count).sum(),
        }
    }

    fn snapshot(state: &TimingState, now: Instant) -> RuntimeProgress {
        let mut progress = RuntimeProgress::new();
        state.snapshot_into(&mut progress, now);
        progress
    }

    #[test]
    fn tiny_operations_accumulate_before_millisecond_rounding() {
        let mut state = TimingState::default();
        let now = Instant::now();
        for _ in 0..10 {
            let id = state.start(Kind::Query, batch(&[("db.orders", 1)]), now);
            state.finish(id, now + Duration::from_micros(150));
        }
        let progress = snapshot(&state, now);
        assert_eq!(
            progress
                .initialization_timing
                .initialization_query_duration_ms,
            1
        );
        assert!(
            !progress.tables["db.orders"]
                .initialization_timing
                .initialization_query_active
        );
    }

    #[test]
    fn live_parallel_operations_are_not_settled_twice_and_freeze_after_finishing() {
        let mut state = TimingState::default();
        let now = Instant::now();
        let query = state.start(Kind::Query, batch(&[("a.orders", 1)]), now);
        let write = state.start(Kind::Write, batch(&[("b.orders", 1)]), now);
        let later = now + Duration::from_secs(3 * 3600);
        for _ in 0..2 {
            let progress = snapshot(&state, later);
            assert_eq!(
                progress
                    .initialization_timing
                    .initialization_query_duration_ms,
                10_800_000
            );
            assert_eq!(
                progress
                    .initialization_timing
                    .initialization_write_duration_ms,
                10_800_000
            );
            assert!(
                progress.tables["a.orders"]
                    .initialization_timing
                    .initialization_query_active
            );
            assert!(
                progress.tables["b.orders"]
                    .initialization_timing
                    .initialization_write_active
            );
        }
        state.finish(query, later);
        state.finish(query, later + Duration::from_secs(1));
        state.finish(write, later);
        let progress = snapshot(&state, later + Duration::from_secs(10));
        assert_eq!(
            progress
                .initialization_timing
                .initialization_query_duration_ms,
            10_800_000
        );
        assert!(!progress.initialization_timing.initialization_query_active);
        assert!(!progress.initialization_timing.initialization_write_active);
    }

    #[test]
    fn mixed_source_tables_share_actual_request_time_and_retry_attempts_accumulate() {
        let mut state = TimingState::default();
        let now = Instant::now();
        for _ in 0..2 {
            let id = state.start(Kind::Write, batch(&[("a.orders", 1), ("b.orders", 3)]), now);
            state.finish(id, now + Duration::from_millis(100));
        }
        let progress = snapshot(&state, now);
        assert_eq!(
            progress.tables["a.orders"]
                .initialization_timing
                .initialization_write_duration_ms,
            50
        );
        assert_eq!(
            progress.tables["b.orders"]
                .initialization_timing
                .initialization_write_duration_ms,
            150
        );
        assert!(
            progress.tables["a.orders"]
                .initialization_timing
                .initialization_write_estimated
        );
        assert_eq!(
            progress
                .initialization_timing
                .initialization_write_duration_ms,
            200
        );
        assert!(
            !progress
                .initialization_timing
                .initialization_write_estimated
        );
    }

    #[test]
    fn cdc_records_do_not_contribute_to_initialization_time() {
        let record = |op| {
            DataBuffer::new_with_route(
                "db".into(),
                "target".into(),
                "orders".into(),
                CaseInsensitiveHashMap::new_with_no_arg(),
                CaseInsensitiveHashMap::new_with_no_arg(),
                op,
                String::new(),
                0,
                0,
            )
        };
        let mut mixed = WriteTimingBatch::default();
        mixed.add(&record(Operation::CREATE(false)));
        assert!(mixed.weights.is_empty());
        assert!(mixed.start().0.is_none());
        mixed.add(&record(Operation::CREATE(true)));
        let mut state = TimingState::default();
        let now = Instant::now();
        let id = state.start(Kind::Write, mixed, now);
        state.finish(id, now + Duration::from_millis(100));
        let progress = snapshot(&state, now);
        assert_eq!(
            progress
                .initialization_timing
                .initialization_write_duration_ms,
            50
        );
    }

    #[tokio::test]
    async fn cancellation_drops_the_activity_guard() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let timing = track_query("cancel_test.orders");
            tx.send(timing.0.unwrap()).unwrap();
            std::future::pending::<()>().await;
            drop(timing);
        });
        let id = rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!TIMINGS.lock().unwrap().active.contains_key(&id));
    }

    #[test]
    fn legacy_status_without_timing_fields_deserializes() {
        let mut progress = RuntimeProgress::new();
        progress.begin_table_initialization("db.orders", 1);
        let mut json = serde_json::to_value(&progress).unwrap();
        json.as_object_mut().unwrap().retain(|key, _| {
            !key.starts_with("initialization_query_") && !key.starts_with("initialization_write_")
        });
        json["tables"]["db.orders"]
            .as_object_mut()
            .unwrap()
            .retain(|key, _| {
                !key.starts_with("initialization_query_")
                    && !key.starts_with("initialization_write_")
            });
        let restored: RuntimeProgress = serde_json::from_value(json).unwrap();
        assert_eq!(restored, progress);
    }
}
