use async_trait::async_trait;
use common::{DataBuffer, Operation, Plugin, PluginConfig, Value};

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
        let is_delete = matches!(data_buffer.op, Operation::DELETE);
        // let data: &HashMap<String, Value> = if is_delete {
        //     &data_buffer.before
        // } else {
        //     &data_buffer.after
        // };
        let mut contains_some_column = Value::None;
        for column in &self.columns {
            // let mut key_matches = "";
            // for key in data.keys() {
            //     if key.eq_ignore_ascii_case(column) {
            //         key_matches = key;
            //         break;
            //     }
            // }
            // if data.contains_key(key_matches) {
            //     contains_some_column = match data.get(key_matches) {
            //         None => Value::None,
            //         Some(x) => x.clone(),
            //     };
            //     break;
            // }

            let v = if is_delete {
                data_buffer.get_column_before(column)
            } else {
                data_buffer.get_column_after(column)
            };
            if !v.is_none() {
                contains_some_column = v.clone();
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
