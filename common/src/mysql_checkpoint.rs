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

    pub checkpoint_filepath: String,
    pub table: String,
}

// 使用sqlite或者redis来做检查点
impl MysqlCheckPointDetailEntity {
    pub async fn from_config(checkpoint_file_path: String, connection_url: &String, table: String) -> Self {
        // 1. 确保 checkpoint 目录存在
        let dir = Path::new(&checkpoint_file_path);
        if !dir.exists() {
            fs::create_dir_all(dir).unwrap_or_else(|_| panic!("创建目录失败: {}", &checkpoint_file_path));
        }
        let checkpoint_filepath = format!(
            "{}/checkpoint_{}_{:x}.json",
            checkpoint_file_path,
            table.to_lowercase(),
            md5::compute(connection_url)
        );

        // 3. 建立 MySQL Pool
        let pool: Pool<MySql> = get_mysql_pool_by_url(connection_url, &format!("checkpoint 初始化建立连接 {}", &table))
            .await
            .unwrap_or_else(|_| panic!("获取mysql连接池失败: {}", &connection_url));

        // 4. 获取当前 MySQL binlog 起点
        let (last_binlog_filename, last_binlog_position) = fetch_mysql_start_position(&pool).await;

        // 获取这个文件，如果没有，就新增这个文件
        let checkpoint_file = Path::new(&checkpoint_filepath);
        if !checkpoint_file.exists() {
            // // 使用 connection_url 获取当前Mysql的binlog文件和position
            // let mut file = File::create(checkpoint_file).expect("创建文件失败");

            // let init_checkpoint = format!(r#"{"binlog_file": "{}","position": {}}"#, last_binlog_filename, last_binlog_position);
            MysqlCheckPointDetailEntity {
                last_binlog_filename,
                last_binlog_position,
                retry_times: 0,
                is_new: true,
                checkpoint_filepath: checkpoint_filepath.to_string(),
                table,
            }
        } else {
            // 从文件里面进行查看
            let json = fs::read_to_string(checkpoint_file).expect("读取文件失败");
            let mut entity = serde_json::from_str::<MysqlCheckPointDetailEntity>(&json)
                .unwrap_or_else(|_| panic!("json转换失败: {}", &checkpoint_filepath));
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
