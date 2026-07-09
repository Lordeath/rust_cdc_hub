use crate::CdcConfig;
use crate::mysql_checkpoint::MysqlCheckPointDetailEntity;
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

const DEFAULT_CHECKPOINT_DIR: &str = "/checkpoint";
const SQLITE_CHECKPOINT_FILE_NAME: &str = "checkpoints.sqlite";
pub const DEFAULT_CHECKPOINT_FLUSH_INTERVAL_SECS: u64 = 30;
const CHECKPOINT_COMMAND_BUFFER: usize = 256;

#[async_trait]
pub trait CheckpointManager: Send + Sync {
    async fn save(&self, key: &str, entity: &MysqlCheckPointDetailEntity) -> Result<(), String>;
    async fn load(&self, key: &str) -> Result<Option<MysqlCheckPointDetailEntity>, String>;

    async fn save_many(
        &self,
        entities: &[(String, MysqlCheckPointDetailEntity)],
    ) -> Result<(), String> {
        for (key, entity) in entities {
            self.save(key, entity).await?;
        }
        Ok(())
    }
}

pub fn checkpoint_flush_interval_secs(config: &CdcConfig) -> u64 {
    config
        .checkpoint_flush_interval_secs
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CHECKPOINT_FLUSH_INTERVAL_SECS)
}

pub async fn checkpoint_service_from_config(config: &CdcConfig) -> CheckpointServiceHandle {
    let configured_path = config
        .checkpoint_file_path
        .clone()
        .unwrap_or_else(|| DEFAULT_CHECKPOINT_DIR.to_string());
    let flush_interval_secs = checkpoint_flush_interval_secs(config);
    CheckpointServiceHandle::start(configured_path, flush_interval_secs)
        .await
        .unwrap_or_else(|e| panic!("初始化 SQLite checkpoint service 失败: {}", e))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointPathResolution {
    pub sqlite_path: PathBuf,
    pub legacy_scan_dir: PathBuf,
}

pub fn resolve_checkpoint_path(configured_path: &str) -> CheckpointPathResolution {
    let trimmed = configured_path.trim();
    let base = if trimmed.is_empty() {
        PathBuf::from(DEFAULT_CHECKPOINT_DIR)
    } else {
        PathBuf::from(trimmed)
    };

    if is_sqlite_file_path(&base) {
        let legacy_scan_dir = base
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        return CheckpointPathResolution {
            sqlite_path: base,
            legacy_scan_dir,
        };
    }

    CheckpointPathResolution {
        sqlite_path: base.join(SQLITE_CHECKPOINT_FILE_NAME),
        legacy_scan_dir: base,
    }
}

fn is_sqlite_file_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "db" || ext == "sqlite" || ext == "sqlite3"
        })
        .unwrap_or(false)
}

pub struct FileCheckpointManager {
    pub base_path: String,
}

impl FileCheckpointManager {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl CheckpointManager for FileCheckpointManager {
    async fn save(&self, _key: &str, entity: &MysqlCheckPointDetailEntity) -> Result<(), String> {
        if entity.checkpoint_filepath.starts_with("memory://") {
            return Ok(());
        }
        let path = Path::new(&entity.checkpoint_filepath);
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let content = serde_json::to_string_pretty(entity).map_err(|e| e.to_string())?;
        fs::write(path, content).map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn load(&self, key: &str) -> Result<Option<MysqlCheckPointDetailEntity>, String> {
        let path = Path::new(key);
        if !path.exists() {
            return Ok(None);
        }
        let json = fs::read_to_string(path).map_err(|e| e.to_string())?;
        serde_json::from_str::<MysqlCheckPointDetailEntity>(&json)
            .map(Some)
            .map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamCheckpoint {
    pub stream_key: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub updated_at: i64,
}

impl StreamCheckpoint {
    pub fn new(stream_key: String, binlog_filename: String, binlog_position: u32) -> Self {
        Self {
            stream_key,
            binlog_filename,
            binlog_position,
            updated_at: Utc::now().timestamp(),
        }
    }

    fn is_valid(&self) -> bool {
        !self.stream_key.trim().is_empty()
            && !self.binlog_filename.trim().is_empty()
            && self.binlog_position > 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointPositionSnapshot {
    pub kind: String,
    pub key: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointServiceSnapshot {
    pub running: bool,
    pub started_at: i64,
    pub last_heartbeat_at: i64,
    pub flush_interval_secs: u64,
    pub flush_count: u64,
    pub flush_error_count: u64,
    pub last_flush_started_at: i64,
    pub last_flush_succeeded_at: i64,
    pub last_flush_failed_at: i64,
    pub last_error: Option<String>,
    pub dirty_table_count: usize,
    pub dirty_stream_count: usize,
    pub last_persisted_table_count: usize,
    pub last_persisted_stream_count: usize,
    pub last_persisted_position: Option<CheckpointPositionSnapshot>,
    pub sqlite_path: String,
}

impl CheckpointServiceSnapshot {
    fn new(sqlite_path: String, flush_interval_secs: u64) -> Self {
        let now = Utc::now().timestamp();
        Self {
            running: false,
            started_at: now,
            last_heartbeat_at: 0,
            flush_interval_secs,
            flush_count: 0,
            flush_error_count: 0,
            last_flush_started_at: 0,
            last_flush_succeeded_at: 0,
            last_flush_failed_at: 0,
            last_error: None,
            dirty_table_count: 0,
            dirty_stream_count: 0,
            last_persisted_table_count: 0,
            last_persisted_stream_count: 0,
            last_persisted_position: None,
            sqlite_path,
        }
    }

    pub fn disabled_for_tests() -> Self {
        Self::new(
            "disabled".to_string(),
            DEFAULT_CHECKPOINT_FLUSH_INTERVAL_SECS,
        )
    }
}

type CheckpointCommandResult<T> = oneshot::Sender<Result<T, String>>;

enum CheckpointServiceCommand {
    LoadTable {
        key: String,
        resp: CheckpointCommandResult<Option<MysqlCheckPointDetailEntity>>,
    },
    LoadStream {
        stream_key: String,
        resp: CheckpointCommandResult<Option<StreamCheckpoint>>,
    },
    RecordTables {
        entries: Vec<(String, MysqlCheckPointDetailEntity)>,
        resp: CheckpointCommandResult<()>,
    },
    RecordStream {
        checkpoint: StreamCheckpoint,
        resp: CheckpointCommandResult<()>,
    },
    FlushNow {
        resp: CheckpointCommandResult<()>,
    },
}

#[derive(Clone)]
pub struct CheckpointServiceHandle {
    tx: mpsc::Sender<CheckpointServiceCommand>,
    snapshot: Arc<Mutex<CheckpointServiceSnapshot>>,
}

impl CheckpointServiceHandle {
    pub async fn start(configured_path: String, flush_interval_secs: u64) -> Result<Self, String> {
        let resolved = resolve_checkpoint_path(configured_path.as_str());
        let snapshot = Arc::new(Mutex::new(CheckpointServiceSnapshot::new(
            resolved.sqlite_path.display().to_string(),
            flush_interval_secs,
        )));
        let (tx, rx) = mpsc::channel(CHECKPOINT_COMMAND_BUFFER);
        let (ready_tx, ready_rx) = oneshot::channel();
        let snapshot_for_task = snapshot.clone();

        tokio::spawn(async move {
            run_checkpoint_service(
                configured_path,
                flush_interval_secs,
                rx,
                snapshot_for_task,
                ready_tx,
            )
            .await;
        });

        match ready_rx.await {
            Ok(Ok(())) => Ok(Self { tx, snapshot }),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(format!("checkpoint service 启动失败: {}", e)),
        }
    }

    pub fn disabled_for_tests() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self {
            tx,
            snapshot: Arc::new(Mutex::new(CheckpointServiceSnapshot::disabled_for_tests())),
        }
    }

    pub async fn load_table_checkpoint(
        &self,
        key: &str,
    ) -> Result<Option<MysqlCheckPointDetailEntity>, String> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(CheckpointServiceCommand::LoadTable {
                key: key.to_string(),
                resp,
            })
            .await
            .map_err(|e| format!("checkpoint service 不可用: {}", e))?;
        rx.await
            .map_err(|e| format!("checkpoint service 读取失败: {}", e))?
    }

    pub async fn load_stream_checkpoint(
        &self,
        stream_key: &str,
    ) -> Result<Option<StreamCheckpoint>, String> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(CheckpointServiceCommand::LoadStream {
                stream_key: stream_key.to_string(),
                resp,
            })
            .await
            .map_err(|e| format!("checkpoint service 不可用: {}", e))?;
        rx.await
            .map_err(|e| format!("checkpoint service 读取失败: {}", e))?
    }

    pub async fn record_table_applied(
        &self,
        key: &str,
        entity: MysqlCheckPointDetailEntity,
    ) -> Result<(), String> {
        self.record_table_applied_many(vec![(key.to_string(), entity)])
            .await
    }

    pub async fn record_table_applied_many(
        &self,
        entries: Vec<(String, MysqlCheckPointDetailEntity)>,
    ) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(CheckpointServiceCommand::RecordTables { entries, resp })
            .await
            .map_err(|e| format!("checkpoint service 不可用: {}", e))?;
        rx.await
            .map_err(|e| format!("checkpoint service 写入内存队列失败: {}", e))?
    }

    pub async fn record_stream_consumed(
        &self,
        stream_key: String,
        binlog_filename: String,
        binlog_position: u32,
    ) -> Result<(), String> {
        let checkpoint = StreamCheckpoint::new(stream_key, binlog_filename, binlog_position);
        if !checkpoint.is_valid() {
            return Ok(());
        }
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(CheckpointServiceCommand::RecordStream { checkpoint, resp })
            .await
            .map_err(|e| format!("checkpoint service 不可用: {}", e))?;
        rx.await
            .map_err(|e| format!("checkpoint service 写入 stream 队列失败: {}", e))?
    }

    pub async fn flush_now(&self) -> Result<(), String> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(CheckpointServiceCommand::FlushNow { resp })
            .await
            .map_err(|e| format!("checkpoint service 不可用: {}", e))?;
        rx.await
            .map_err(|e| format!("checkpoint service flush 失败: {}", e))?
    }

    pub async fn snapshot(&self) -> CheckpointServiceSnapshot {
        self.snapshot.lock().await.clone()
    }
}

async fn run_checkpoint_service(
    configured_path: String,
    flush_interval_secs: u64,
    mut rx: mpsc::Receiver<CheckpointServiceCommand>,
    snapshot: Arc<Mutex<CheckpointServiceSnapshot>>,
    ready_tx: oneshot::Sender<Result<(), String>>,
) {
    let store = match SqliteCheckpointStore::new(configured_path).await {
        Ok(store) => store,
        Err(e) => {
            {
                let mut snapshot = snapshot.lock().await;
                snapshot.running = false;
                snapshot.last_error = Some(e.clone());
                snapshot.last_flush_failed_at = Utc::now().timestamp();
            }
            let _ = ready_tx.send(Err(e));
            return;
        }
    };

    {
        let mut snapshot = snapshot.lock().await;
        let now = Utc::now().timestamp();
        snapshot.running = true;
        snapshot.started_at = now;
        snapshot.last_heartbeat_at = now;
        snapshot.sqlite_path = store.sqlite_path.display().to_string();
    }
    let _ = ready_tx.send(Ok(()));

    let mut dirty_tables: HashMap<String, MysqlCheckPointDetailEntity> = HashMap::new();
    let mut dirty_streams: HashMap<String, StreamCheckpoint> = HashMap::new();
    let mut ticker = time::interval(Duration::from_secs(flush_interval_secs));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                touch_heartbeat(&snapshot).await;
                handle_checkpoint_command(
                    command,
                    &store,
                    &snapshot,
                    &mut dirty_tables,
                    &mut dirty_streams,
                )
                .await;
            }
            _ = ticker.tick() => {
                touch_heartbeat(&snapshot).await;
                let _ = flush_dirty_checkpoints(
                    &store,
                    &snapshot,
                    &mut dirty_tables,
                    &mut dirty_streams,
                )
                .await;
            }
        }
    }

    let _ = flush_dirty_checkpoints(&store, &snapshot, &mut dirty_tables, &mut dirty_streams).await;
    {
        let mut snapshot = snapshot.lock().await;
        snapshot.running = false;
        snapshot.last_heartbeat_at = Utc::now().timestamp();
    }
    info!("checkpoint service stopped");
}

async fn handle_checkpoint_command(
    command: CheckpointServiceCommand,
    store: &SqliteCheckpointStore,
    snapshot: &Arc<Mutex<CheckpointServiceSnapshot>>,
    dirty_tables: &mut HashMap<String, MysqlCheckPointDetailEntity>,
    dirty_streams: &mut HashMap<String, StreamCheckpoint>,
) {
    match command {
        CheckpointServiceCommand::LoadTable { key, resp } => {
            let dirty = dirty_tables
                .get(key.as_str())
                .cloned()
                .or_else(|| dirty_table_by_lookup_key(dirty_tables, key.as_str()));
            let result = match dirty {
                Some(entity) => Ok(Some(entity)),
                None => store.load_table(key.as_str()).await,
            };
            let _ = resp.send(result);
        }
        CheckpointServiceCommand::LoadStream { stream_key, resp } => {
            let result = match dirty_streams.get(stream_key.as_str()) {
                Some(checkpoint) => Ok(Some(checkpoint.clone())),
                None => store.load_stream(stream_key.as_str()).await,
            };
            let _ = resp.send(result);
        }
        CheckpointServiceCommand::RecordTables { entries, resp } => {
            for (key, entity) in entries {
                if entity.checkpoint_filepath.starts_with("memory://") {
                    continue;
                }
                dirty_tables.insert(checkpoint_key(key.as_str(), &entity), entity);
            }
            update_dirty_counts(snapshot, dirty_tables.len(), dirty_streams.len()).await;
            let _ = resp.send(Ok(()));
        }
        CheckpointServiceCommand::RecordStream { checkpoint, resp } => {
            if checkpoint.is_valid() {
                dirty_streams.insert(checkpoint.stream_key.clone(), checkpoint);
            }
            update_dirty_counts(snapshot, dirty_tables.len(), dirty_streams.len()).await;
            let _ = resp.send(Ok(()));
        }
        CheckpointServiceCommand::FlushNow { resp } => {
            let result =
                flush_dirty_checkpoints(store, snapshot, dirty_tables, dirty_streams).await;
            let _ = resp.send(result);
        }
    }
}

fn dirty_table_by_lookup_key(
    dirty_tables: &HashMap<String, MysqlCheckPointDetailEntity>,
    key: &str,
) -> Option<MysqlCheckPointDetailEntity> {
    dirty_tables.values().find_map(|entity| {
        if entity.checkpoint_filepath == key || entity.table.eq_ignore_ascii_case(key) {
            Some(entity.clone())
        } else {
            None
        }
    })
}

async fn touch_heartbeat(snapshot: &Arc<Mutex<CheckpointServiceSnapshot>>) {
    snapshot.lock().await.last_heartbeat_at = Utc::now().timestamp();
}

async fn update_dirty_counts(
    snapshot: &Arc<Mutex<CheckpointServiceSnapshot>>,
    table_count: usize,
    stream_count: usize,
) {
    let mut snapshot = snapshot.lock().await;
    snapshot.dirty_table_count = table_count;
    snapshot.dirty_stream_count = stream_count;
}

async fn flush_dirty_checkpoints(
    store: &SqliteCheckpointStore,
    snapshot: &Arc<Mutex<CheckpointServiceSnapshot>>,
    dirty_tables: &mut HashMap<String, MysqlCheckPointDetailEntity>,
    dirty_streams: &mut HashMap<String, StreamCheckpoint>,
) -> Result<(), String> {
    if dirty_tables.is_empty() && dirty_streams.is_empty() {
        update_dirty_counts(snapshot, 0, 0).await;
        return Ok(());
    }

    let table_entries = dirty_tables
        .iter()
        .map(|(key, entity)| (key.clone(), entity.clone()))
        .collect::<Vec<_>>();
    let stream_entries = dirty_streams.values().cloned().collect::<Vec<_>>();
    {
        let mut snapshot = snapshot.lock().await;
        snapshot.last_flush_started_at = Utc::now().timestamp();
    }

    match store
        .save_tables_and_streams(&table_entries, &stream_entries)
        .await
    {
        Ok((table_count, stream_count)) => {
            dirty_tables.clear();
            dirty_streams.clear();
            let last_position = last_persisted_position(&table_entries, &stream_entries);
            let mut snapshot = snapshot.lock().await;
            snapshot.flush_count += 1;
            snapshot.last_flush_succeeded_at = Utc::now().timestamp();
            snapshot.last_error = None;
            snapshot.dirty_table_count = 0;
            snapshot.dirty_stream_count = 0;
            snapshot.last_persisted_table_count = table_count;
            snapshot.last_persisted_stream_count = stream_count;
            snapshot.last_persisted_position = last_position;
            Ok(())
        }
        Err(e) => {
            error!("checkpoint service flush failed: {}", e);
            let mut snapshot = snapshot.lock().await;
            snapshot.flush_error_count += 1;
            snapshot.last_flush_failed_at = Utc::now().timestamp();
            snapshot.last_error = Some(e.clone());
            snapshot.dirty_table_count = dirty_tables.len();
            snapshot.dirty_stream_count = dirty_streams.len();
            Err(e)
        }
    }
}

fn last_persisted_position(
    table_entries: &[(String, MysqlCheckPointDetailEntity)],
    stream_entries: &[StreamCheckpoint],
) -> Option<CheckpointPositionSnapshot> {
    if let Some(stream) = stream_entries.iter().max_by(|a, b| {
        a.binlog_filename
            .cmp(&b.binlog_filename)
            .then(a.binlog_position.cmp(&b.binlog_position))
    }) {
        return Some(CheckpointPositionSnapshot {
            kind: "stream".to_string(),
            key: stream.stream_key.clone(),
            binlog_filename: stream.binlog_filename.clone(),
            binlog_position: stream.binlog_position,
            updated_at: stream.updated_at,
        });
    }

    table_entries
        .iter()
        .max_by(|a, b| {
            a.1.last_binlog_filename
                .cmp(&b.1.last_binlog_filename)
                .then(a.1.last_binlog_position.cmp(&b.1.last_binlog_position))
        })
        .map(|(key, entity)| CheckpointPositionSnapshot {
            kind: "table".to_string(),
            key: key.clone(),
            binlog_filename: entity.last_binlog_filename.clone(),
            binlog_position: entity.last_binlog_position,
            updated_at: Utc::now().timestamp(),
        })
}

struct SqliteCheckpointStore {
    pool: Pool<Sqlite>,
    sqlite_path: PathBuf,
    legacy_scan_dir: PathBuf,
}

impl SqliteCheckpointStore {
    async fn new(configured_path: String) -> Result<Self, String> {
        let resolved = resolve_checkpoint_path(configured_path.as_str());
        if let Some(parent) = resolved.sqlite_path.parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }

        let options =
            SqliteConnectOptions::from_str(resolved.sqlite_path.to_string_lossy().as_ref())
                .map_err(|e| e.to_string())?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .busy_timeout(Duration::from_millis(5000));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| e.to_string())?;

        let store = Self {
            pool,
            sqlite_path: resolved.sqlite_path,
            legacy_scan_dir: resolved.legacy_scan_dir,
        };
        store.initialize().await?;
        let migrated = store.migrate_legacy_json().await?;
        if migrated > 0 {
            info!(
                "SQLite checkpoint 已自动迁移旧 JSON 文件: db={} count={}",
                store.sqlite_path.display(),
                migrated
            );
        }
        Ok(store)
    }

    async fn initialize(&self) -> Result<(), String> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cdc_checkpoints (
                checkpoint_key TEXT PRIMARY KEY,
                source_fingerprint TEXT NOT NULL DEFAULT '',
                source_database TEXT NOT NULL DEFAULT '',
                target_database TEXT NOT NULL DEFAULT '',
                table_name TEXT NOT NULL,
                binlog_filename TEXT NOT NULL,
                binlog_position INTEGER NOT NULL,
                is_new INTEGER NOT NULL,
                retry_times INTEGER NOT NULL DEFAULT 0,
                checkpoint_filepath TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cdc_checkpoints_table
            ON cdc_checkpoints(table_name)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cdc_stream_checkpoints (
                stream_key TEXT PRIMARY KEY,
                binlog_filename TEXT NOT NULL,
                binlog_position INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn migrate_legacy_json(&self) -> Result<usize, String> {
        if !self.legacy_scan_dir.exists() {
            return Ok(0);
        }
        let mut migrated = 0usize;
        let entries = fs::read_dir(&self.legacy_scan_dir).map_err(|e| e.to_string())?;
        for entry in entries {
            let entry = match entry {
                Ok(v) => v,
                Err(e) => {
                    warn!("读取 checkpoint 迁移目录项失败: {}", e);
                    continue;
                }
            };
            let path = entry.path();
            if !is_legacy_checkpoint_json(&path) {
                continue;
            }
            let json = match fs::read_to_string(&path) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        "读取旧 checkpoint JSON 失败: path={} error={}",
                        path.display(),
                        e
                    );
                    continue;
                }
            };
            let entity = match serde_json::from_str::<MysqlCheckPointDetailEntity>(&json) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        "解析旧 checkpoint JSON 失败，已跳过: path={} error={}",
                        path.display(),
                        e
                    );
                    continue;
                }
            };
            if self
                .insert_entity(entity.checkpoint_filepath.as_str(), &entity, false)
                .await?
            {
                migrated += 1;
            }
        }
        Ok(migrated)
    }

    async fn insert_entity(
        &self,
        key: &str,
        entity: &MysqlCheckPointDetailEntity,
        overwrite: bool,
    ) -> Result<bool, String> {
        if entity.checkpoint_filepath.starts_with("memory://") {
            return Ok(false);
        }
        let checkpoint_key = checkpoint_key(key, entity);
        let payload_json = serde_json::to_string(entity).map_err(|e| e.to_string())?;
        let (source_database, table_name) = split_checkpoint_table(entity.table.as_str());
        let source_fingerprint = source_fingerprint_from_path(entity.checkpoint_filepath.as_str());
        let updated_at = Utc::now().timestamp();
        let query = if overwrite {
            r#"
            INSERT INTO cdc_checkpoints (
                checkpoint_key, source_fingerprint, source_database, target_database,
                table_name, binlog_filename, binlog_position, is_new, retry_times,
                checkpoint_filepath, payload_json, updated_at
            )
            VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(checkpoint_key) DO UPDATE SET
                source_fingerprint = excluded.source_fingerprint,
                source_database = excluded.source_database,
                table_name = excluded.table_name,
                binlog_filename = excluded.binlog_filename,
                binlog_position = excluded.binlog_position,
                is_new = excluded.is_new,
                retry_times = excluded.retry_times,
                checkpoint_filepath = excluded.checkpoint_filepath,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            "#
        } else {
            r#"
            INSERT OR IGNORE INTO cdc_checkpoints (
                checkpoint_key, source_fingerprint, source_database, target_database,
                table_name, binlog_filename, binlog_position, is_new, retry_times,
                checkpoint_filepath, payload_json, updated_at
            )
            VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)
            "#
        };

        let result = sqlx::query(query)
            .bind(checkpoint_key)
            .bind(source_fingerprint)
            .bind(source_database)
            .bind(table_name)
            .bind(entity.last_binlog_filename.as_str())
            .bind(i64::from(entity.last_binlog_position))
            .bind(if entity.is_new { 1i64 } else { 0i64 })
            .bind(i64::from(entity.retry_times))
            .bind(entity.checkpoint_filepath.as_str())
            .bind(payload_json)
            .bind(updated_at)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    async fn save_tables_and_streams(
        &self,
        table_entries: &[(String, MysqlCheckPointDetailEntity)],
        stream_entries: &[StreamCheckpoint],
    ) -> Result<(usize, usize), String> {
        if table_entries.is_empty() && stream_entries.is_empty() {
            return Ok((0, 0));
        }
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let mut table_count = 0usize;
        let mut stream_count = 0usize;
        for (key, entity) in table_entries {
            if entity.checkpoint_filepath.starts_with("memory://") {
                continue;
            }
            let checkpoint_key = checkpoint_key(key, entity);
            let payload_json = serde_json::to_string(entity).map_err(|e| e.to_string())?;
            let (source_database, table_name) = split_checkpoint_table(entity.table.as_str());
            let source_fingerprint =
                source_fingerprint_from_path(entity.checkpoint_filepath.as_str());
            sqlx::query(
                r#"
                INSERT INTO cdc_checkpoints (
                    checkpoint_key, source_fingerprint, source_database, target_database,
                    table_name, binlog_filename, binlog_position, is_new, retry_times,
                    checkpoint_filepath, payload_json, updated_at
                )
                VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(checkpoint_key) DO UPDATE SET
                    source_fingerprint = excluded.source_fingerprint,
                    source_database = excluded.source_database,
                    table_name = excluded.table_name,
                    binlog_filename = excluded.binlog_filename,
                    binlog_position = excluded.binlog_position,
                    is_new = excluded.is_new,
                    retry_times = excluded.retry_times,
                    checkpoint_filepath = excluded.checkpoint_filepath,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(checkpoint_key)
            .bind(source_fingerprint)
            .bind(source_database)
            .bind(table_name)
            .bind(entity.last_binlog_filename.as_str())
            .bind(i64::from(entity.last_binlog_position))
            .bind(if entity.is_new { 1i64 } else { 0i64 })
            .bind(i64::from(entity.retry_times))
            .bind(entity.checkpoint_filepath.as_str())
            .bind(payload_json)
            .bind(Utc::now().timestamp())
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            table_count += 1;
        }

        for checkpoint in stream_entries {
            if !checkpoint.is_valid() {
                continue;
            }
            sqlx::query(
                r#"
                INSERT INTO cdc_stream_checkpoints (
                    stream_key, binlog_filename, binlog_position, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stream_key) DO UPDATE SET
                    binlog_filename = excluded.binlog_filename,
                    binlog_position = excluded.binlog_position,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(checkpoint.stream_key.as_str())
            .bind(checkpoint.binlog_filename.as_str())
            .bind(i64::from(checkpoint.binlog_position))
            .bind(checkpoint.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
            stream_count += 1;
        }

        tx.commit()
            .await
            .map_err(|e| e.to_string())
            .map(|_| (table_count, stream_count))
    }

    async fn load_table(&self, key: &str) -> Result<Option<MysqlCheckPointDetailEntity>, String> {
        if key.starts_with("memory://") {
            return Ok(None);
        }
        let row = sqlx::query(
            r#"
            SELECT payload_json
            FROM cdc_checkpoints
            WHERE checkpoint_key = ?
               OR checkpoint_filepath = ?
               OR lower(table_name) = lower(?)
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(key)
        .bind(key)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        row.map(|row| {
            let payload_json: String = row.try_get("payload_json").map_err(|e| e.to_string())?;
            serde_json::from_str::<MysqlCheckPointDetailEntity>(&payload_json)
                .map_err(|e| e.to_string())
        })
        .transpose()
    }

    async fn load_stream(&self, stream_key: &str) -> Result<Option<StreamCheckpoint>, String> {
        let row = sqlx::query(
            r#"
            SELECT stream_key, binlog_filename, binlog_position, updated_at
            FROM cdc_stream_checkpoints
            WHERE stream_key = ?
            LIMIT 1
            "#,
        )
        .bind(stream_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        row.map(|row| {
            let position = row
                .try_get::<i64, _>("binlog_position")
                .map_err(|e| e.to_string())?;
            let binlog_position = u32::try_from(position)
                .map_err(|_| format!("invalid binlog_position: {}", position))?;
            Ok(StreamCheckpoint {
                stream_key: row.try_get("stream_key").map_err(|e| e.to_string())?,
                binlog_filename: row.try_get("binlog_filename").map_err(|e| e.to_string())?,
                binlog_position,
                updated_at: row.try_get("updated_at").map_err(|e| e.to_string())?,
            })
        })
        .transpose()
    }
}

fn is_legacy_checkpoint_json(path: &Path) -> bool {
    path.is_file()
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("checkpoint_") && name.ends_with(".json"))
}

fn checkpoint_key(key: &str, entity: &MysqlCheckPointDetailEntity) -> String {
    if !entity.checkpoint_filepath.trim().is_empty() {
        return entity.checkpoint_filepath.clone();
    }
    key.to_string()
}

fn split_checkpoint_table(table: &str) -> (String, String) {
    table
        .split_once('.')
        .map(|(db, table)| (db.to_string(), table.to_string()))
        .unwrap_or_else(|| ("".to_string(), table.to_string()))
}

fn source_fingerprint_from_path(path: &str) -> String {
    path.rsplit_once('_')
        .and_then(|(_, suffix)| suffix.strip_suffix(".json"))
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_checkpoint_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rust_cdc_hub_{}_{}_{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    fn test_entity(path: &Path, table: &str, position: u32) -> MysqlCheckPointDetailEntity {
        MysqlCheckPointDetailEntity {
            last_binlog_filename: "mysql-bin.000001".to_string(),
            last_binlog_position: position,
            retry_times: 0,
            is_new: false,
            checkpoint_filepath: path.display().to_string(),
            table: table.to_string(),
        }
    }

    #[test]
    fn resolves_directory_checkpoint_path() {
        let resolved = resolve_checkpoint_path("/checkpoint");
        assert_eq!(
            resolved.sqlite_path,
            PathBuf::from("/checkpoint/checkpoints.sqlite")
        );
        assert_eq!(resolved.legacy_scan_dir, PathBuf::from("/checkpoint"));
    }

    #[test]
    fn resolves_file_checkpoint_path() {
        let resolved = resolve_checkpoint_path("/tmp/cdc.db");
        assert_eq!(resolved.sqlite_path, PathBuf::from("/tmp/cdc.db"));
        assert_eq!(resolved.legacy_scan_dir, PathBuf::from("/tmp"));
    }

    #[test]
    fn splits_checkpoint_table() {
        assert_eq!(
            split_checkpoint_table("source.orders"),
            ("source".to_string(), "orders".to_string())
        );
        assert_eq!(
            split_checkpoint_table("orders"),
            ("".to_string(), "orders".to_string())
        );
    }

    #[tokio::test]
    async fn sqlite_checkpoint_store_saves_and_loads_entity() {
        let dir = temp_checkpoint_dir("save_load");
        fs::create_dir_all(&dir).unwrap();
        let store = SqliteCheckpointStore::new(dir.display().to_string())
            .await
            .unwrap();
        let legacy_path = dir.join("checkpoint_source.orders_deadbeef.json");
        let entity = test_entity(&legacy_path, "source.orders", 42);

        store
            .save_tables_and_streams(&[(entity.checkpoint_filepath.clone(), entity.clone())], &[])
            .await
            .unwrap();
        let loaded = store
            .load_table(entity.checkpoint_filepath.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.table, "source.orders");
        assert_eq!(loaded.last_binlog_position, 42);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn sqlite_checkpoint_store_migrates_legacy_json() {
        let dir = temp_checkpoint_dir("migrate");
        fs::create_dir_all(&dir).unwrap();
        let legacy_path = dir.join("checkpoint_source.orders_deadbeef.json");
        let entity = test_entity(&legacy_path, "source.orders", 88);
        fs::write(&legacy_path, serde_json::to_string(&entity).unwrap()).unwrap();

        let store = SqliteCheckpointStore::new(dir.display().to_string())
            .await
            .unwrap();
        let loaded = store
            .load_table(entity.checkpoint_filepath.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.last_binlog_position, 88);
        assert!(legacy_path.exists());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn sqlite_checkpoint_store_saves_and_loads_stream() {
        let dir = temp_checkpoint_dir("stream");
        fs::create_dir_all(&dir).unwrap();
        let store = SqliteCheckpointStore::new(dir.display().to_string())
            .await
            .unwrap();
        let checkpoint = StreamCheckpoint::new(
            "mysql:server1".to_string(),
            "mysql-bin.000010".to_string(),
            123,
        );

        store
            .save_tables_and_streams(&[], &[checkpoint.clone()])
            .await
            .unwrap();
        let loaded = store.load_stream("mysql:server1").await.unwrap().unwrap();

        assert_eq!(loaded.binlog_filename, checkpoint.binlog_filename);
        assert_eq!(loaded.binlog_position, checkpoint.binlog_position);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn checkpoint_service_records_dirty_and_flushes_on_demand() {
        let dir = temp_checkpoint_dir("service");
        fs::create_dir_all(&dir).unwrap();
        let handle = CheckpointServiceHandle::start(dir.display().to_string(), 3600)
            .await
            .unwrap();
        let legacy_path = dir.join("checkpoint_source.orders_deadbeef.json");
        let entity = test_entity(&legacy_path, "source.orders", 66);

        handle
            .record_table_applied(entity.checkpoint_filepath.as_str(), entity.clone())
            .await
            .unwrap();
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.dirty_table_count, 1);
        assert_eq!(snapshot.flush_count, 0);

        handle.flush_now().await.unwrap();
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.dirty_table_count, 0);
        assert_eq!(snapshot.flush_count, 1);
        let loaded = handle
            .load_table_checkpoint(entity.checkpoint_filepath.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.last_binlog_position, 66);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn checkpoint_service_loads_dirty_state_before_flush() {
        let dir = temp_checkpoint_dir("service_dirty_load");
        fs::create_dir_all(&dir).unwrap();
        let handle = CheckpointServiceHandle::start(dir.display().to_string(), 3600)
            .await
            .unwrap();
        let legacy_path = dir.join("checkpoint_source.orders_deadbeef.json");
        let entity = test_entity(&legacy_path, "source.orders", 77);
        let stream = StreamCheckpoint::new(
            "mysql:1:abc".to_string(),
            "mysql-bin.000020".to_string(),
            456,
        );

        handle
            .record_table_applied(entity.checkpoint_filepath.as_str(), entity.clone())
            .await
            .unwrap();
        handle
            .record_stream_consumed(
                stream.stream_key.clone(),
                stream.binlog_filename.clone(),
                stream.binlog_position,
            )
            .await
            .unwrap();

        let loaded_table = handle
            .load_table_checkpoint(entity.checkpoint_filepath.as_str())
            .await
            .unwrap()
            .unwrap();
        let loaded_stream = handle
            .load_stream_checkpoint(stream.stream_key.as_str())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded_table.last_binlog_position, 77);
        assert_eq!(loaded_stream.binlog_position, 456);
        assert_eq!(handle.snapshot().await.flush_count, 0);

        fs::remove_dir_all(&dir).unwrap();
    }
}
