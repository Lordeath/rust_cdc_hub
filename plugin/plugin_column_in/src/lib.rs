use async_trait::async_trait;
use common::runtime_progress;
use common::{DataBuffer, Operation, Plugin, PluginConfig, Value};

const NO_MATCHED_FILTER_COLUMN: &str = "__no_matched_filter_column__";

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
        let mut contains_some_column = Value::None;
        let mut matched_column = String::new();
        for column in &self.columns {
            let v = if is_delete {
                data_buffer.before.get(column)
            } else {
                data_buffer.after.get(column)
            };
            if !v.is_none() {
                contains_some_column = v.clone();
                matched_column = column.clone();
                break;
            }
        }
        if contains_some_column.is_none() {
            runtime_progress::record_plugin_filter_result(
                "ColumnIn",
                &data_buffer.table_name,
                NO_MATCHED_FILTER_COLUMN,
                true,
            )
            .await;
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
        runtime_progress::record_plugin_filter_result(
            "ColumnIn",
            &data_buffer.table_name,
            &matched_column,
            result,
        )
        .await;
        if result {
            Ok(data_buffer.clone())
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::case_insensitive_hash_map::CaseInsensitiveHashMap;
    use common::runtime_progress;
    use common::{DataBuffer, Operation, PluginConfig};
    use std::collections::HashMap;

    fn config() -> PluginConfig {
        PluginConfig {
            plugin_type: common::PluginType::ColumnIn,
            config: HashMap::from([
                ("columns".to_string(), "project_id,tenant_id".to_string()),
                ("values".to_string(), "10001,10002".to_string()),
            ]),
        }
    }

    fn buffer(table_name: &str, after: HashMap<String, Value>) -> DataBuffer {
        DataBuffer::new(
            table_name.to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            CaseInsensitiveHashMap::new(after),
            Operation::CREATE(false),
            String::new(),
            0,
            0,
        )
    }

    #[tokio::test]
    async fn column_in_records_actual_filtered_column() {
        let mut plugin = PluginColumnIn::new(&config());
        let table_name = "column_in_filtered_orders";
        let data = buffer(
            table_name,
            HashMap::from([("project_id".to_string(), Value::String("20001".to_string()))]),
        );

        let result = plugin.collect(data).await;

        assert!(result.is_err());
        let snapshot = runtime_progress::snapshot().await;
        let filter = snapshot
            .plugin_filters
            .get(&format!("ColumnIn|{}|project_id", table_name))
            .unwrap();
        assert_eq!(filter.input_total, 1);
        assert_eq!(filter.output_total, 0);
        assert_eq!(filter.filtered_total, 1);
        assert_eq!(filter.column_name, "project_id");
    }

    #[tokio::test]
    async fn column_in_records_output_when_value_is_allowed() {
        let mut plugin = PluginColumnIn::new(&config());
        let table_name = "column_in_allowed_orders";
        let data = buffer(
            table_name,
            HashMap::from([("project_id".to_string(), Value::String("10001".to_string()))]),
        );

        let result = plugin.collect(data).await;

        assert!(result.is_ok());
        let snapshot = runtime_progress::snapshot().await;
        let filter = snapshot
            .plugin_filters
            .get(&format!("ColumnIn|{}|project_id", table_name))
            .unwrap();
        assert_eq!(filter.input_total, 1);
        assert_eq!(filter.output_total, 1);
        assert_eq!(filter.filtered_total, 0);
    }

    #[tokio::test]
    async fn column_in_records_output_when_no_configured_column_exists() {
        let mut plugin = PluginColumnIn::new(&config());
        let table_name = "column_in_missing_column_orders";
        let data = buffer(
            table_name,
            HashMap::from([("other_id".to_string(), Value::String("20001".to_string()))]),
        );

        let result = plugin.collect(data).await;

        assert!(result.is_ok());
        let snapshot = runtime_progress::snapshot().await;
        let filter = snapshot
            .plugin_filters
            .get(&format!(
                "ColumnIn|{}|{}",
                table_name, NO_MATCHED_FILTER_COLUMN
            ))
            .unwrap();
        assert_eq!(filter.input_total, 1);
        assert_eq!(filter.output_total, 1);
        assert_eq!(filter.filtered_total, 0);
    }
}
