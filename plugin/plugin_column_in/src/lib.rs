use async_trait::async_trait;
use common::{DataBuffer, Operation, Plugin, PluginConfig, Value};
use std::collections::HashMap;

pub struct PluginColumnIn {
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

impl PluginColumnIn {
    pub fn new(config: &PluginConfig) -> PluginColumnIn {
        let columns: Vec<String> = config
            .get_config("columns")
            .split(",")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let values: Vec<String> = config
            .get_config("values")
            .split(",")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if columns.is_empty() {
            panic!("columns must be set");
        }
        if values.is_empty() {
            panic!("values must be set");
        }
        PluginColumnIn { columns, values }
    }
}

#[async_trait]
impl Plugin for PluginColumnIn {
    async fn collect(&mut self, data_buffer: DataBuffer) -> Result<DataBuffer, ()> {
        let data: &HashMap<String, Value> = if data_buffer.op == Operation::DELETE {
            &data_buffer.before
        } else {
            &data_buffer.after
        };
        let mut contains_some_column = Value::None;
        for column in &self.columns {
            // if data.contains_key(column) {
            //     contains_some_column = data.get(column).unwrap().clone();
            //     break;
            // }
            // if data.contains_key(column.to_lowercase().as_str()) {
            //     contains_some_column = data.get(column.to_lowercase().as_str()).unwrap().clone();
            //     break;
            // }
            let mut key_matches = "";
            for (key, _value) in data {
                if key.eq_ignore_ascii_case(column) {
                    key_matches = key;
                    break;
                }
            }
            if data.contains_key(key_matches) {
                contains_some_column = match data.get(key_matches) {
                    None => Value::None,
                    Some(x) => x.clone(),
                };
                break;
            }
        }
        if contains_some_column.is_none() {
            return Ok(data_buffer.clone());
        }
        let to_compare: String = contains_some_column.resolve_string();
        let mut result = false;
        for value in &self.values {
            if to_compare.eq_ignore_ascii_case(value) {
                result = true;
                break;
            }
        }
        if result {
            Ok(data_buffer.clone())
        } else {
            Err(())
        }
    }
}
