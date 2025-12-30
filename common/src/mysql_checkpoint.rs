use crate::get_mysql_pool_by_url;
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool, Row};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlCheckPointDetailEntity {
    pub last_binlog_filename: String,
    pub last_binlog_position: u32,
    pub retry_times: u32,
    pub is_new: bool,

    checkpoint_filepath: String,
}

// 使用sqlite或者redis来做检查点
impl MysqlCheckPointDetailEntity {
    pub async fn from_config(checkpoint_file_path: String, connection_url: &String) -> Self {
        // 1. 确保 checkpoint 目录存在
        let dir = Path::new(&checkpoint_file_path);
        if !dir.exists() {
            fs::create_dir_all(dir).expect(&format!("创建目录失败: {}", &checkpoint_file_path));
        }
        let checkpoint_filepath = format!(
            "{}/{:x}.json",
            checkpoint_file_path,
            md5::compute(&connection_url)
        );

        // 3. 建立 MySQL Pool
        let pool: Pool<MySql> = get_mysql_pool_by_url(&connection_url)
            .await
            .expect(&format!("获取mysql连接池失败: {}", &connection_url));

        // 4. 获取当前 MySQL binlog 起点
        let (last_binlog_filename, last_binlog_position) = fetch_mysql_start_position(&pool).await;

        // 获取这个文件，如果没有，就新增这个文件
        let checkpoint_file = Path::new(&checkpoint_filepath);
        if !checkpoint_file.exists() {
            // 使用 connection_url 获取当前Mysql的binlog文件和position
            let mut file = File::create(checkpoint_file).expect("创建文件失败");

            // let init_checkpoint = format!(r#"{"binlog_file": "{}","position": {}}"#, last_binlog_filename, last_binlog_position);
            let entity = MysqlCheckPointDetailEntity {
                last_binlog_filename,
                last_binlog_position,
                retry_times: 0,
                is_new: true,
                checkpoint_filepath: checkpoint_filepath.to_string(),
            };
            let json = serde_json::to_string(&entity).expect("json转换失败");

            file.write_all(json.as_bytes()).expect("写入文件失败");
            entity
        } else {
            // 从文件里面进行查看
            let json = fs::read_to_string(&checkpoint_file).expect("读取文件失败");
            let mut entity = serde_json::from_str::<MysqlCheckPointDetailEntity>(&json)
                .expect(&format!("json转换失败: {}", &checkpoint_filepath));
            if entity.last_binlog_filename.eq(&last_binlog_filename)
                && entity.last_binlog_position > last_binlog_position
            {
                warn!(
                    "发现错误的起点，尝试进行修正 {} {} -> {}",
                    &last_binlog_filename, &entity.last_binlog_position, &last_binlog_position
                );
                entity.last_binlog_position = last_binlog_position;
            }
            entity.is_new = false;
            entity
        }
    }

    // 刷新现在的binlog的记录
    pub fn update(&mut self, last_binlog_filename: String, last_binlog_position: u32) -> Self {
        self.last_binlog_filename = last_binlog_filename.clone();
        self.last_binlog_position = last_binlog_position.clone();
        self.clone()
    }
    pub fn add_retry(&mut self) -> Self {
        self.retry_times += 1;
        self.clone()
    }

    // 保存到文件
    pub fn save(&mut self) -> Result<(), ()> {
        let json = serde_json::to_string(&self).expect("json转换失败");
        let checkpoint_file = Path::new(&self.checkpoint_filepath);
        let mut file = File::create(checkpoint_file).expect("创建文件失败");
        file.write_all(json.as_bytes()).expect("写入文件失败");
        Ok(())
    }
}

pub async fn fetch_mysql_start_position(pool: &Pool<MySql>) -> (String, u32) {
    let row = sqlx::query("SHOW MASTER STATUS")
        .fetch_one(pool)
        .await
        .expect("获取mysql binlog文件位置失败");

    let binlog_file: String = row.try_get(0).unwrap();
    let position: u64 = row.try_get(1).unwrap();
    let position = format!("{:?}", position);
    (
        // StartPosition::BinlogPosition(
        //     binlog_file.clone(),
        //     position.clone().parse::<u32>().unwrap(),
        // ),
        binlog_file,
        position.clone().parse::<u32>().unwrap(),
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn test1() {
        assert_eq!(
            "202cb962ac59075b964b07152d234b70",
            format!("{:x}", md5::compute("123"))
        );

    }
}
