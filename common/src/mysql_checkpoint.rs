use crate::checkpoint_manager::{
    CheckpointManager, CheckpointServiceHandle, FileCheckpointManager, resolve_checkpoint_path,
};
use crate::custom_error::CustomError;
use serde::{Deserialize, Serialize};
use sqlx::{Column, MySql, Pool, Row};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use tokio::time::{Duration, timeout};
use tracing::info;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MysqlInitializationPhase {
    #[default]
    Legacy,
    Copying,
    AwaitingCdc,
    Streaming,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum MysqlInitializationCursor {
    Signed(i64),
    Unsigned(u64),
    Text(String),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MysqlInitialization {
    pub phase: MysqlInitializationPhase,
    pub snapshot_file: String,
    pub snapshot_position: u32,
    #[serde(default)]
    pub cursor: Option<MysqlInitializationCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlCheckPointDetailEntity {
    pub last_binlog_filename: String,
    pub last_binlog_position: u32,
    pub retry_times: u32,
    pub is_new: bool,
    #[serde(default)]
    pub initialization: MysqlInitialization,

    pub checkpoint_filepath: String,
    pub table: String,
}

// 使用sqlite或者redis来做检查点
impl MysqlCheckPointDetailEntity {
    pub fn checkpoint_filepath(
        checkpoint_file_path: &str,
        connection_url: &str,
        table: &str,
    ) -> String {
        format!(
            "{}/checkpoint_{}_{:x}.json",
            checkpoint_file_path,
            table.to_lowercase(),
            md5::compute(connection_url)
        )
    }

    pub async fn from_config(
        checkpoint_file_path: String,
        connection_url: &String,
        table: String,
    ) -> Result<Self, CustomError> {
        let manager = FileCheckpointManager::new(checkpoint_file_path.clone());
        Self::from_config_with_manager(checkpoint_file_path, connection_url, table, &manager).await
    }

    pub async fn from_config_with_manager(
        checkpoint_file_path: String,
        connection_url: &String,
        table: String,
        checkpoint_manager: &dyn CheckpointManager,
    ) -> Result<Self, CustomError> {
        let path = Self::resolved_filepath(&checkpoint_file_path, connection_url, &table);
        let entity = checkpoint_manager.load(&path).await.map_err(|e| {
            CustomError::fatal(format!("读取 checkpoint 失败: table={table}, error={e}"))
        })?;
        Ok(entity.unwrap_or_else(|| Self::new_uninitialized(path, table)))
    }

    fn resolved_filepath(directory: &str, connection_url: &str, table: &str) -> String {
        let directory = resolve_checkpoint_path(directory).legacy_scan_dir;
        Self::checkpoint_filepath(&directory.to_string_lossy(), connection_url, table)
    }

    fn new_uninitialized(checkpoint_filepath: String, table: String) -> Self {
        Self {
            last_binlog_filename: String::new(),
            last_binlog_position: 0,
            retry_times: 0,
            is_new: true,
            checkpoint_filepath,
            table,
            initialization: MysqlInitialization::default(),
        }
    }

    pub async fn from_config_with_service(
        checkpoint_file_path: String,
        connection_url: &String,
        table: String,
        checkpoint_service: &CheckpointServiceHandle,
    ) -> Result<Self, CustomError> {
        let path = Self::resolved_filepath(&checkpoint_file_path, connection_url, &table);
        if let Some(entity) = checkpoint_service
            .load_table_checkpoint(&path)
            .await
            .map_err(|e| {
                CustomError::fatal(format!("读取 checkpoint 失败: table={table}, error={e}"))
            })?
        {
            return Ok(entity);
        }
        // 仅在服务确认没有记录时读取旧文件；读取失败不能当成新任务。
        if Path::new(&path).exists() {
            let json = fs::read_to_string(&path).map_err(|e| {
                CustomError::fatal(format!("读取旧 checkpoint 失败: table={table}, error={e}"))
            })?;
            return serde_json::from_str(&json).map_err(|e| {
                CustomError::fatal(format!("解析旧 checkpoint 失败: table={table}, error={e}"))
            });
        }
        Ok(Self::new_uninitialized(path, table))
    }

    // 刷新现在的binlog的记录
    pub fn update(&mut self, last_binlog_filename: String, last_binlog_position: u32) -> Self {
        self.last_binlog_filename = last_binlog_filename.clone();
        self.last_binlog_position = last_binlog_position;
        self.clone()
    }
    pub fn add_retry(&mut self) -> Self {
        self.retry_times += 1;
        self.clone()
    }

    // 保存到文件
    pub fn save(&self) -> Result<(), String> {
        let json_result = serde_json::to_string(&self);
        if let Err(e) = json_result {
            return Err(e.to_string());
        }
        let checkpoint_file = Path::new(&self.checkpoint_filepath);
        let file_result = File::create(checkpoint_file);
        if let Err(e) = file_result {
            return Err(e.to_string());
        }
        match file_result
            .unwrap()
            .write_all(json_result.unwrap().as_bytes())
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub async fn fetch_mysql_start_position(pool: &Pool<MySql>, table: &str) -> (String, u32) {
    info!("fetch mysql start position for checkpoint table {}", table);
    let row = timeout(
        Duration::from_secs(10),
        sqlx::query("SHOW MASTER STATUS").fetch_one(pool),
    )
    .await
    .unwrap_or_else(|_| panic!("获取mysql binlog文件位置超时: {}", table))
    .expect("获取mysql binlog文件位置失败");

    let columns: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
    let binlog_file: String = row
        .try_get::<String, _>("File")
        .or_else(|_| row.try_get::<String, _>("Log_name"))
        .or_else(|_| row.try_get::<String, _>(0))
        .unwrap_or_else(|e| panic!("读取 mysql binlog file 失败: {}; columns={:?}", e, columns));
    let position_u64: u64 = row
        .try_get::<u64, _>("Position")
        .or_else(|_| row.try_get::<u64, _>("Pos"))
        .or_else(|_| row.try_get::<u64, _>(1))
        .unwrap_or_else(|e| {
            panic!(
                "读取 mysql binlog position 失败: {}; columns={:?}",
                e, columns
            )
        });
    let position_u32: u32 = u32::try_from(position_u64).unwrap_or_else(|_| {
        panic!(
            "mysql binlog position 超出 u32: {} columns={:?}",
            position_u64, columns
        )
    });
    info!(
        "fetched mysql start position for checkpoint table {}: {}/{}",
        table, binlog_file, position_u32
    );
    (binlog_file, position_u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_reload_preserves_incomplete_snapshot_and_invalid_position() {
        let directory = format!(
            "/tmp/cdc-checkpoint-state-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        let url = "mysql://test.invalid/source".to_string();
        let table = "source.orders".to_string();
        let path = MysqlCheckPointDetailEntity::resolved_filepath(&directory, &url, &table);
        let service = CheckpointServiceHandle::start(directory.clone(), 3600)
            .await
            .unwrap();
        let mut checkpoint =
            MysqlCheckPointDetailEntity::new_uninitialized(path.clone(), table.clone());
        checkpoint.last_binlog_filename = "mysql-bin.000010".to_string();
        checkpoint.last_binlog_position = u32::MAX;
        checkpoint.initialization = MysqlInitialization {
            phase: MysqlInitializationPhase::Copying,
            snapshot_file: checkpoint.last_binlog_filename.clone(),
            snapshot_position: u32::MAX,
            cursor: Some(MysqlInitializationCursor::Text("主键 A".to_string())),
        };
        service
            .record_table_applied(&table, checkpoint)
            .await
            .unwrap();
        service.flush_now().await.unwrap();
        let loaded = MysqlCheckPointDetailEntity::from_config_with_service(
            directory.clone(),
            &url,
            table,
            &service,
        )
        .await
        .unwrap();
        assert!(loaded.is_new);
        assert_eq!(
            loaded.initialization.phase,
            MysqlInitializationPhase::Copying
        );
        assert_eq!(
            loaded.initialization.cursor,
            Some(MysqlInitializationCursor::Text("主键 A".to_string()))
        );
        assert_eq!(loaded.last_binlog_position, u32::MAX, "不能静默修正位点");
        drop(service);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn unavailable_checkpoint_service_is_not_a_new_task() {
        let error = MysqlCheckPointDetailEntity::from_config_with_service(
            "/tmp/cdc-no-fallback".to_string(),
            &"mysql://test.invalid/source".to_string(),
            "source.orders".to_string(),
            &CheckpointServiceHandle::disabled_for_tests(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.error_type,
            crate::custom_error::CustomErrorType::Fatal
        ));
    }

    #[test]
    fn legacy_checkpoint_deserialization_remains_supported() {
        let checkpoint: MysqlCheckPointDetailEntity = serde_json::from_str(r#"{
            "last_binlog_filename":"mysql-bin.000010","last_binlog_position":120,
            "retry_times":0,"is_new":false,"checkpoint_filepath":"/tmp/legacy.json","table":"source.orders"
        }"#).unwrap();
        assert_eq!(
            checkpoint.initialization.phase,
            MysqlInitializationPhase::Legacy
        );
        assert!(!checkpoint.is_new);
        let old_copy: MysqlInitialization = serde_json::from_str(
            r#"{
            "phase":"copying","snapshot_file":"mysql-bin.000010","snapshot_position":120
        }"#,
        )
        .unwrap();
        assert_eq!(old_copy.cursor, None);
    }

    #[test]
    fn test1() {
        assert_eq!(
            "202cb962ac59075b964b07152d234b70",
            format!("{:x}", md5::compute("123"))
        );
    }
}
