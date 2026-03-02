use crate::mysql_checkpoint::MysqlCheckPointDetailEntity;
use async_trait::async_trait;
use std::fs;
use std::path::Path;

#[async_trait]
pub trait CheckpointManager: Send + Sync {
    async fn save(&self, key: &str, entity: &MysqlCheckPointDetailEntity) -> Result<(), String>;
    async fn load(&self, key: &str) -> Result<Option<MysqlCheckPointDetailEntity>, String>;
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
        // Use entity.checkpoint_filepath as the file path, or construct it from base_path + key?
        // Currently MysqlCheckPointDetailEntity has checkpoint_filepath.
        // We should probably rely on that for backward compatibility or refactor it.
        // But the Manager should manage paths ideally.
        // However, MysqlCheckPointDetailEntity calculates filepath based on md5(connection_url+table).
        // Let's use that for now.
        
        let path = Path::new(&entity.checkpoint_filepath);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
        }
        let content = serde_json::to_string_pretty(entity).map_err(|e| e.to_string())?;
        fs::write(path, content).map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn load(&self, _key: &str) -> Result<Option<MysqlCheckPointDetailEntity>, String> {
        // Load logic is currently inside MysqlCheckPointDetailEntity::from_config which does logic + load.
        // We might need to refactor MysqlCheckPointDetailEntity to separate logic from I/O.
        // But for now, load is not used via manager in this refactoring step (it's done in new()).
        // We can implement it later or leave it unimplemented.
        Err("Not implemented".to_string())
    }
}
