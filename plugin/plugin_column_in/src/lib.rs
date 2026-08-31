use async_trait::async_trait;
use common::case_insensitive_hash_map::CaseInsensitiveHashMap;
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

    fn configured_value<'a>(
        &self,
        values: &'a CaseInsensitiveHashMap,
    ) -> Option<(&str, &'a Value)> {
        self.columns.iter().find_map(|column| {
            let value = values.get(column);
            (!value.is_none()).then_some((column.as_str(), value))
        })
    }

    fn is_allowed(&self, value: &Value) -> bool {
        let to_compare = value.resolve_string();
        self.values
            .iter()
            .any(|allowed| to_compare.eq_ignore_ascii_case(allowed))
    }
}

#[async_trait]
impl Plugin for PluginColumnIn {
    async fn collect(&mut self, data_buffer: DataBuffer) -> Result<DataBuffer, ()> {
        let values = if matches!(data_buffer.op, Operation::DELETE) {
            &data_buffer.before
        } else {
            &data_buffer.after
        };
        let Some((matched_column, value)) = self.configured_value(values) else {
            runtime_progress::record_plugin_filter_result(
                "ColumnIn",
                &data_buffer.table_name,
                NO_MATCHED_FILTER_COLUMN,
                true,
            )
            .await;
            return Ok(data_buffer);
        };
        let result = self.is_allowed(value);

        if matches!(data_buffer.op, Operation::UPDATE)
            && !result
            && self
                .configured_value(&data_buffer.before)
                .is_some_and(|(_, before_value)| self.is_allowed(before_value))
        {
            runtime_progress::record_plugin_filter_result(
                "ColumnIn",
                &data_buffer.table_name,
                matched_column,
                true,
            )
            .await;
            let mut delete = data_buffer;
            delete.op = Operation::DELETE;
            delete.after = CaseInsensitiveHashMap::new_with_no_arg();
            return Ok(delete);
        }

        runtime_progress::record_plugin_filter_result(
            "ColumnIn",
            &data_buffer.table_name,
            matched_column,
            result,
        )
        .await;
        if result { Ok(data_buffer) } else { Err(()) }
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

    #[tokio::test]
    async fn column_in_turns_matching_to_non_matching_update_into_delete() {
        let mut plugin = PluginColumnIn::new(&config());
        let before = CaseInsensitiveHashMap::new(HashMap::from([
            ("id".to_string(), Value::Int64(7)),
            ("project_id".to_string(), Value::String("10001".to_string())),
        ]));
        let after = CaseInsensitiveHashMap::new(HashMap::from([
            ("id".to_string(), Value::Int64(7)),
            ("project_id".to_string(), Value::String("20001".to_string())),
        ]));
        let update = DataBuffer::new(
            "orders".to_string(),
            before,
            after,
            Operation::UPDATE,
            "mysql-bin.000001".to_string(),
            1,
            120,
        );

        let result = plugin.collect(update).await.unwrap();

        assert_eq!(result.op, Operation::DELETE);
        assert_eq!(result.before.get("id").resolve_string(), "7");
        assert!(result.after.is_empty());
    }
}
