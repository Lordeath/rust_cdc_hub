use async_trait::async_trait;
use common::{DataBuffer, Operation, Plugin, PluginConfig, Value};
use std::collections::HashMap;

pub struct PluginPlus {
    pub columns: Vec<ColumnInfoDetail>,
    pub plus: i64,
}

#[derive(Eq, Hash, PartialEq)]
pub struct ColumnInfoDetail {
    pub table_name: String,
    pub column_name: String,
}

impl PluginPlus {
    pub fn new(config: &PluginConfig) -> Self {
        let columns: Vec<ColumnInfoDetail> = config
            .get_config("columns")
            .split(",")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(|s| {
                let mut split = s.split(".");
                ColumnInfoDetail {
                    table_name: split
                        .next()
                        .unwrap_or_else(|| panic!("table_name must be set"))
                        .to_string(),
                    column_name: split
                        .next()
                        .unwrap_or_else(|| panic!("column_name must be set"))
                        .to_string(),
                }
            })
            .collect();
        if columns.is_empty() {
            panic!("columns must be set");
        }
        let plus = config
            .get_config("plus")
            .parse::<i64>()
            .expect("plus must be a number");
        PluginPlus { columns, plus }
    }
}

#[async_trait]
impl Plugin for PluginPlus {
    async fn collect(&mut self, data_buffer: DataBuffer) -> Result<DataBuffer, ()> {
        let data_original = data_buffer.clone();
        let mut data: HashMap<String, Value> = if data_buffer.op == Operation::DELETE {
            data_buffer.before
        } else {
            data_buffer.after
        };
        let mut contains_some_column: HashMap<String, Value> = HashMap::new();
        for column in &self.columns {
            if !column
                .table_name
                .eq_ignore_ascii_case(&data_buffer.table_name)
            {
                continue;
            }
            for (k, v) in &data {
                if k.eq_ignore_ascii_case(&column.column_name) && !v.is_none() {
                    contains_some_column.insert(column.column_name.clone(), v.clone());
                    break;
                }
            }
        }
        if contains_some_column.is_empty() {
            return Ok(data_original);
        }
        for (column_name, value) in contains_some_column {
            let int_64: i64 = value.resolve_string().parse().unwrap();
            data.insert(column_name, Value::Int64(int_64 + self.plus));
        }
        if data_buffer.op == Operation::DELETE {
            return Ok(data_original.new_before(data));
        }

        Ok(data_original.new_after(data))
    }
}
