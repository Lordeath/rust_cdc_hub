use crate::CdcConfig;
use crate::mysql_checkpoint::MysqlCheckPointDetailEntity;
use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const DEFAULT_CHECKPOINT_DIR: &str = "/checkpoint";
const SQLITE_CHECKPOINT_FILE_NAME: &str = "checkpoints.sqlite";

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

pub async fn checkpoint_manager_from_config(config: &CdcConfig) -> Arc<dyn CheckpointManager> {
    let configured_path = config
        .checkpoint_file_path
        .clone()
        .unwrap_or_else(|| DEFAULT_CHECKPOINT_DIR.to_string());
    Arc::new(
        SqliteCheckpointManager::new(configured_path)
            .await
            .unwrap_or_else(|e| panic!("初始化 SQLite checkpoint 失败: {}", e)),
    )
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

pub struct SqliteCheckpointManager {
    pool: Pool<Sqlite>,
    sqlite_path: PathBuf,
    legacy_scan_dir: PathBuf,
}

impl SqliteCheckpointManager {
    pub async fn new(configured_path: String) -> Result<Self, String> {
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

        let manager = Self {
            pool,
            sqlite_path: resolved.sqlite_path,
            legacy_scan_dir: resolved.legacy_scan_dir,
        };
        manager.initialize().await?;
        let migrated = manager.migrate_legacy_json().await?;
        if migrated > 0 {
            info!(
                "SQLite checkpoint 已自动迁移旧 JSON 文件: db={} count={}",
                manager.sqlite_path.display(),
                migrated
            );
        }
        Ok(manager)
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

#[async_trait]
impl CheckpointManager for SqliteCheckpointManager {
    async fn save(&self, key: &str, entity: &MysqlCheckPointDetailEntity) -> Result<(), String> {
        self.insert_entity(key, entity, true).await.map(|_| ())
    }

    async fn save_many(
        &self,
        entities: &[(String, MysqlCheckPointDetailEntity)],
    ) -> Result<(), String> {
        if entities.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        for (key, entity) in entities {
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
        }
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn load(&self, key: &str) -> Result<Option<MysqlCheckPointDetailEntity>, String> {
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
    async fn sqlite_checkpoint_manager_saves_and_loads_entity() {
        let dir = temp_checkpoint_dir("save_load");
        fs::create_dir_all(&dir).unwrap();
        let manager = SqliteCheckpointManager::new(dir.display().to_string())
            .await
            .unwrap();
        let legacy_path = dir.join("checkpoint_source.orders_deadbeef.json");
        let entity = test_entity(&legacy_path, "source.orders", 42);

        manager
            .save(entity.checkpoint_filepath.as_str(), &entity)
            .await
            .unwrap();
        let loaded = manager
            .load(entity.checkpoint_filepath.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.table, "source.orders");
        assert_eq!(loaded.last_binlog_position, 42);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn sqlite_checkpoint_manager_migrates_legacy_json() {
        let dir = temp_checkpoint_dir("migrate");
        fs::create_dir_all(&dir).unwrap();
        let legacy_path = dir.join("checkpoint_source.orders_deadbeef.json");
        let entity = test_entity(&legacy_path, "source.orders", 88);
        fs::write(&legacy_path, serde_json::to_string(&entity).unwrap()).unwrap();

        let manager = SqliteCheckpointManager::new(dir.display().to_string())
            .await
            .unwrap();
        let loaded = manager
            .load(entity.checkpoint_filepath.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.last_binlog_position, 88);
        assert!(legacy_path.exists());

        fs::remove_dir_all(&dir).unwrap();
    }
}
