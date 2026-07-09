use async_trait::async_trait;
use common::case_insensitive_hash_map::{
    CaseInsensitiveHashMapTableInfoVo, CaseInsensitiveHashMapVecString,
};
use common::checkpoint_manager::{CheckpointManager, checkpoint_manager_from_config};
use common::metrics::{SINK_EVENTS_TOTAL, SINK_FLUSH_DURATION_SECONDS, SINK_FLUSH_ERRORS_TOTAL};
use common::mysql_checkpoint::MysqlCheckPointDetailEntity;
use common::schema::{
    extract_mysql_create_table_column_definitions, extract_mysql_generated_column_names,
    mysql_column_allows_null_from_definition, mysql_column_is_auto_increment_from_definition,
    mysql_primary_key_columns_from_create_table_sql, mysql_type_token_from_column_definition,
};
use common::{
    CdcConfig, DataBuffer, FlushByOperation, ForeignKeyInfo, MySqlRoutineDefinition,
    MySqlRoutineKind, MySqlViewDefinition, Operation, Sink, TableIndexInfo, TableInfoVo, Value,
    database_table_key, fetch_mysql_routines, fetch_mysql_views, get_mysql_pool_by_url,
    mysql_connection_url_from_config,
};
use dameng::Client;
use dameng::ToDmValue;
use dameng::row::Column as DamengResultColumn;
use dameng_types::{DmValue, DmValueType, decode_value};
use mysql_to_dameng::{
    convert_mysql_routine_to_dameng_with_name,
    convert_mysql_view_to_dameng_with_name_and_schema_routes,
};
use sqlx::types::BigDecimal;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

const DAMENG_INLINE_STRING_CHAR_LIMIT: u32 = 512;
const DAMENG_DECIMAL_MAX_PRECISION: u32 = 38;
const DAMENG_BLOB_WRITE_CHUNK_SIZE: usize = 2000;
const DAMENG_CLOB_WRITE_CHUNK_CHARS: usize = 2000;
const DAMENG_IDENTIFIER_CHAR_LIMIT: usize = 128;
const DAMENG_RANDOM_CHECK_TEXT_CHUNK_CHARS: usize = 200;
const DAMENG_RANDOM_CHECK_BINARY_CHUNK_BYTES: usize = 1000;
const DAMENG_RANDOM_CHECK_PAYLOAD_COLUMN_LIMIT: usize = 64;
const DAMENG_RANDOM_CHECK_RESULT_FILE: &str = "/opt/fxm/datacheck-resule.log";
const DEFAULT_DAMENG_INIT_INSERT_BATCH_ROWS: usize = 16;

#[derive(Debug, Clone)]
enum DamengParam {
    Null,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl ToDmValue for DamengParam {
    fn to_dm_value(&self) -> DmValue {
        match self {
            DamengParam::Null => DmValue::Null,
            DamengParam::I8(v) => DmValue::TinyInt(*v),
            DamengParam::I16(v) => DmValue::SmallInt(*v),
            DamengParam::I32(v) => DmValue::Int(*v),
            DamengParam::I64(v) => DmValue::BigInt(*v),
            DamengParam::F32(v) => DmValue::Float(*v),
            DamengParam::F64(v) => DmValue::Double(*v),
            DamengParam::Text(v) => DmValue::Text(v.clone()),
            DamengParam::Bytes(v) => DmValue::Bytea(v.clone()),
        }
    }
}

#[derive(Debug, Clone)]
struct DamengColumnInfo {
    data_type: String,
    data_length: u32,
    char_length: u32,
    data_precision: Option<u32>,
    data_scale: Option<u32>,
    default_value: Option<String>,
    nullable: bool,
}

#[derive(Debug, Clone)]
struct DamengRequiredColumnInfo {
    column_name: String,
    data_type: String,
    default_value: Option<String>,
    identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DamengForeignKeyConstraintInfo {
    schema: String,
    table_name: String,
    constraint_name: String,
}

#[derive(Debug, Clone)]
struct RandomCheckColumnPlan {
    source_column: String,
    target_column: String,
    #[allow(dead_code)]
    alias: String,
    kind: MysqlCompareKind,
}

#[derive(Debug, Clone)]
struct RandomCheckInitRow {
    pk_value: Value,
    record: DataBuffer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MysqlCompareKind {
    Integer,
    Decimal,
    Float,
    Date,
    Time,
    DateTime,
    Char,
    Varchar,
    Text,
    Json,
    Binary,
    Bit,
    Year,
}

#[derive(Debug, Default)]
struct RandomCheckSummary {
    table_count: usize,
    checked_rows: usize,
    mismatch_count: usize,
    table_error_count: usize,
}

#[derive(Debug, Default)]
struct RandomCheckTableSummary {
    checked_rows: usize,
    mismatch_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TextChunkCompareResult {
    Equal,
    Missing,
    Different(String),
}

pub struct DamengSink {
    client: Mutex<Client>,
    host: String,
    port: u16,
    username: String,
    password: String,
    schema: String,
    table_info_list: Vec<TableInfoVo>,
    buffer: Mutex<Vec<DataBuffer>>,
    initialized: RwLock<bool>,
    sink_batch_size: usize,
    auto_create_database: bool,
    auto_create_table: bool,
    auto_add_column: bool,
    auto_modify_column: bool,
    sync_foreign_key_tables: bool,
    table_info_cache: Mutex<CaseInsensitiveHashMapTableInfoVo>,
    columns_cache: Mutex<CaseInsensitiveHashMapVecString>,
    checkpoint: Mutex<HashMap<String, MysqlCheckPointDetailEntity>>,
    checkpoint_manager: Arc<dyn CheckpointManager>,
    identity_insert_tables: Mutex<HashSet<String>>,
    init_upsert_first_tables: Mutex<HashSet<String>>,
    active_identity_insert_table: Mutex<Option<(String, String)>>,
    init_insert_batch_rows: usize,
    random_check_data_after_init: bool,
    random_check_init_rows: Mutex<HashMap<String, Vec<RandomCheckInitRow>>>,
    random_check_init_columns: Mutex<HashMap<String, Vec<String>>>,
    init_disabled_foreign_key_constraints: Mutex<Vec<DamengForeignKeyConstraintInfo>>,
    dml_checked_column_keys: Mutex<HashMap<String, HashSet<String>>>,
    dml_required_column_defaults: Mutex<HashMap<String, Vec<(String, Value)>>>,
}

impl DamengSink {
    fn reset_random_check_result_file(header: &str) {
        if let Err(e) = fs::write(DAMENG_RANDOM_CHECK_RESULT_FILE, format!("{}\n", header)) {
            warn!(
                "Dameng random data check result file reset failed: path={} error={}",
                DAMENG_RANDOM_CHECK_RESULT_FILE, e
            );
        }
    }

    fn append_random_check_result_line(line: &str) {
        let result = OpenOptions::new()
            .create(true)
            .append(true)
            .open(DAMENG_RANDOM_CHECK_RESULT_FILE)
            .and_then(|mut file| writeln!(file, "{}", line));
        if let Err(e) = result {
            warn!(
                "Dameng random data check result file append failed: path={} error={}",
                DAMENG_RANDOM_CHECK_RESULT_FILE, e
            );
        }
    }

    pub async fn new(config: &CdcConfig, table_info_list: Vec<TableInfoVo>) -> Self {
        let host = config.first_sink_not_blank("host");
        let port = config.first_sink("port").parse::<u16>().unwrap_or(5236);
        let username = config.first_sink_not_blank("username");
        let password = config.first_sink_not_blank("password");
        let configured_schema = {
            let schema = config.first_sink("schema");
            if schema.is_empty() {
                config.first_sink("database")
            } else {
                schema
            }
        };
        let schema = config
            .sink_databases()
            .into_iter()
            .find(|schema| !schema.trim().is_empty())
            .unwrap_or(configured_schema);
        if let Err(e) = Self::validate_merged_target_schema(&table_info_list, schema.as_str()) {
            panic!("{}", e);
        }
        if let Err(e) = Self::validate_target_column_names(&table_info_list, schema.as_str()) {
            panic!("{}", e);
        }

        let client = Self::connect_client(&host, port, &username, &password)
            .unwrap_or_else(|e| panic!("Failed to connect to Dameng: {}", e));

        let sink_batch_size = config.sink_batch_size.unwrap_or(256);
        let init_insert_batch_rows = config
            .first_sink("init_insert_batch_rows")
            .parse::<usize>()
            .unwrap_or(DEFAULT_DAMENG_INIT_INSERT_BATCH_ROWS)
            .max(1);
        let sink = DamengSink {
            client: Mutex::new(client),
            host,
            port,
            username,
            password,
            schema,
            table_info_list,
            buffer: Mutex::new(Vec::with_capacity(sink_batch_size)),
            initialized: RwLock::new(false),
            sink_batch_size,
            auto_create_database: config.auto_create_database.unwrap_or(true),
            auto_create_table: config.auto_create_table.unwrap_or(true),
            auto_add_column: config.auto_add_column.unwrap_or(true),
            auto_modify_column: config.auto_modify_column.unwrap_or(true),
            sync_foreign_key_tables: config.sync_foreign_key_tables_enabled(),
            table_info_cache: Mutex::new(CaseInsensitiveHashMapTableInfoVo::new_with_no_arg()),
            columns_cache: Mutex::new(CaseInsensitiveHashMapVecString::new_with_no_arg()),
            checkpoint: Mutex::new(HashMap::new()),
            checkpoint_manager: checkpoint_manager_from_config(config).await,
            identity_insert_tables: Mutex::new(HashSet::new()),
            init_upsert_first_tables: Mutex::new(HashSet::new()),
            active_identity_insert_table: Mutex::new(None),
            init_insert_batch_rows,
            random_check_data_after_init: config.random_check_data_after_init_enabled(),
            random_check_init_rows: Mutex::new(HashMap::new()),
            random_check_init_columns: Mutex::new(HashMap::new()),
            init_disabled_foreign_key_constraints: Mutex::new(Vec::new()),
            dml_checked_column_keys: Mutex::new(HashMap::new()),
            dml_required_column_defaults: Mutex::new(HashMap::new()),
        };
        if sink.random_check_data_after_init {
            Self::reset_random_check_result_file(
                format!(
                    "START status=initializing tables={} result_file={}",
                    sink.table_info_list.len(),
                    DAMENG_RANDOM_CHECK_RESULT_FILE
                )
                .as_str(),
            );
        }
        sink.ensure_database()
            .await
            .unwrap_or_else(|e| panic!("Dameng ensure database failed: {}", e));
        sink.ensure_schema()
            .await
            .unwrap_or_else(|e| panic!("Dameng ensure schema failed: {}", e));
        sink.disable_foreign_key_constraints_for_init()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Dameng disable foreign key constraints before init failed: {}",
                    e
                )
            });
        if config.sync_stored_procedure_enabled() && !sink.random_check_data_after_init {
            sink.sync_stored_routines(config)
                .await
                .unwrap_or_else(|e| panic!("Dameng sync stored routines failed: {}", e));
        } else if config.sync_stored_procedure_enabled() {
            info!("Dameng random data check init mode skips stored routine synchronization");
        }
        if config.sync_stored_view_enabled() && !sink.random_check_data_after_init {
            sink.sync_stored_views(config)
                .await
                .unwrap_or_else(|e| panic!("Dameng sync stored views failed: {}", e));
        } else if config.sync_stored_view_enabled() {
            info!("Dameng random data check init mode skips view synchronization");
        }
        sink
    }

    fn connect_client(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
    ) -> Result<Client, Box<dyn Error + Send + Sync>> {
        let mut client = Client::new(host, port);
        client.connect(username, password)?;
        Ok(client)
    }

    async fn reconnect(&self) -> Result<(), String> {
        let client = Self::connect_client(&self.host, self.port, &self.username, &self.password)
            .map_err(|e| e.to_string())?;
        *self.client.lock().await = client;
        Ok(())
    }

    async fn spawn_random_check_after_init(&self) {
        if !self.random_check_data_after_init {
            info!("Dameng random data check after init is disabled");
            return;
        }
        let table_info_list = self.table_info_list.clone();
        let host = self.host.clone();
        let port = self.port;
        let username = self.username.clone();
        let password = self.password.clone();
        let fallback_schema = self.schema.clone();
        let init_rows = self.random_check_init_rows.lock().await.clone();
        let init_columns = self.random_check_init_columns.lock().await.clone();

        let spawn_result = std::thread::Builder::new()
            .name("dameng-random-check-after-init".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        error!(
                            "Dameng random data check after init runtime build failed: {}",
                            e
                        );
                        return;
                    }
                };
                runtime.block_on(async move {
                    Self::log_random_check_after_init_result(
                        Self::run_random_check_after_init(
                            table_info_list,
                            host,
                            port,
                            username,
                            password,
                            fallback_schema,
                            init_rows,
                            init_columns,
                        )
                        .await,
                    );
                });
            });
        if let Err(e) = spawn_result {
            error!(
                "Dameng random data check after init thread spawn failed: {}",
                e
            );
        }
    }

    async fn run_random_check_after_init_now(&self) {
        if !self.random_check_data_after_init {
            info!("Dameng random data check after init is disabled");
            return;
        }
        let table_info_list = self.table_info_list.clone();
        let host = self.host.clone();
        let port = self.port;
        let username = self.username.clone();
        let password = self.password.clone();
        let fallback_schema = self.schema.clone();
        let init_rows = self.random_check_init_rows.lock().await.clone();
        let init_columns = self.random_check_init_columns.lock().await.clone();

        Self::log_random_check_after_init_result(
            Self::run_random_check_after_init(
                table_info_list,
                host,
                port,
                username,
                password,
                fallback_schema,
                init_rows,
                init_columns,
            )
            .await,
        );
    }

    fn log_random_check_after_init_result(result: Result<RandomCheckSummary, String>) {
        match result {
            Ok(summary) => {
                if summary.mismatch_count > 0 || summary.table_error_count > 0 {
                    Self::append_random_check_result_line(
                        format!(
                            "FINAL status=error tables={} checked_rows={} mismatches={} table_errors={}",
                            summary.table_count,
                            summary.checked_rows,
                            summary.mismatch_count,
                            summary.table_error_count
                        )
                        .as_str(),
                    );
                    error!(
                        "Dameng random data check after init finished with errors: tables={} checked_rows={} mismatches={} table_errors={}",
                        summary.table_count,
                        summary.checked_rows,
                        summary.mismatch_count,
                        summary.table_error_count
                    );
                } else {
                    Self::append_random_check_result_line(
                        format!(
                            "FINAL status=success tables={} checked_rows={} mismatches={} table_errors={}",
                            summary.table_count,
                            summary.checked_rows,
                            summary.mismatch_count,
                            summary.table_error_count
                        )
                        .as_str(),
                    );
                    info!(
                        "Dameng random data check after init success: tables={} checked_rows={}",
                        summary.table_count, summary.checked_rows
                    );
                }
            }
            Err(e) => {
                Self::append_random_check_result_line(
                    format!("FINAL status=failed error={}", e).as_str(),
                );
                error!("Dameng random data check after init failed: {}", e);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_random_check_after_init(
        table_info_list: Vec<TableInfoVo>,
        host: String,
        port: u16,
        username: String,
        password: String,
        fallback_schema: String,
        init_rows: HashMap<String, Vec<RandomCheckInitRow>>,
        init_columns: HashMap<String, Vec<String>>,
    ) -> Result<RandomCheckSummary, String> {
        let cdc_tables = table_info_list
            .iter()
            .filter(|table_info| !table_info.pk_column.trim().is_empty())
            .filter(|table_info| {
                init_rows.contains_key(
                    Self::table_info_source_table_key(table_info)
                        .to_ascii_lowercase()
                        .as_str(),
                )
            })
            .collect::<Vec<_>>();
        if cdc_tables.is_empty() {
            info!("Dameng random data check after init skipped: no init pk was recorded");
            Self::reset_random_check_result_file(
                "Dameng random data check skipped: no init pk was recorded",
            );
            return Ok(RandomCheckSummary::default());
        }

        Self::reset_random_check_result_file(
            format!(
                "START status=running tables={} recorded_pk_tables={} result_file={}",
                cdc_tables.len(),
                init_rows.len(),
                DAMENG_RANDOM_CHECK_RESULT_FILE
            )
            .as_str(),
        );
        info!(
            "Dameng random data check after init started: tables={} recorded_pk_tables={}",
            cdc_tables.len(),
            init_rows.len()
        );

        let mut summary = RandomCheckSummary {
            table_count: cdc_tables.len(),
            ..Default::default()
        };
        for table_info in cdc_tables {
            let mut dameng_client = match Self::connect_client(&host, port, &username, &password) {
                Ok(client) => client,
                Err(e) => {
                    summary.table_error_count += 1;
                    Self::append_random_check_result_line(
                        format!(
                            "TABLE_FAILED table={} reason=connect Dameng failed error={}",
                            Self::table_info_source_table_key(table_info),
                            e
                        )
                        .as_str(),
                    );
                    error!(
                        "Dameng random data check table failed: connect Dameng failed for {} error={}",
                        Self::table_info_source_table_key(table_info),
                        e
                    );
                    continue;
                }
            };

            match Self::random_check_one_table(
                &mut dameng_client,
                &host,
                port,
                &username,
                &password,
                fallback_schema.as_str(),
                table_info,
                init_rows
                    .get(
                        Self::table_info_source_table_key(table_info)
                            .to_ascii_lowercase()
                            .as_str(),
                    )
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                init_columns
                    .get(
                        Self::table_info_source_table_key(table_info)
                            .to_ascii_lowercase()
                            .as_str(),
                    )
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            )
            .await
            {
                Ok(table_summary) => {
                    summary.checked_rows += table_summary.checked_rows;
                    summary.mismatch_count += table_summary.mismatch_count;
                }
                Err(e) => {
                    summary.table_error_count += 1;
                    Self::append_random_check_result_line(
                        format!(
                            "TABLE_FAILED table={} error={}",
                            Self::table_info_source_table_key(table_info),
                            e
                        )
                        .as_str(),
                    );
                    error!(
                        "Dameng random data check table failed: {} error={}",
                        Self::table_info_source_table_key(table_info),
                        e
                    );
                }
            }
        }

        Ok(summary)
    }

    async fn record_random_check_init_row(&self, record: &DataBuffer) {
        if !self.random_check_data_after_init || !matches!(record.op, Operation::CREATE(true)) {
            return;
        }
        let Some(table_info) = self.table_info_for_record(record) else {
            warn!(
                "Dameng random data check init pk skipped because table metadata is missing: {}.{}",
                record.source_database, record.table_name
            );
            return;
        };
        let pk_value = record.get_pk(table_info.pk_column.as_str());
        if matches!(pk_value, Value::None) {
            warn!(
                "Dameng random data check init pk skipped because pk is empty: {}.{} pk={}",
                record.source_database, record.table_name, table_info.pk_column
            );
            return;
        }
        let key = Self::table_info_source_table_key(table_info).to_ascii_lowercase();
        self.random_check_init_rows
            .lock()
            .await
            .entry(key.clone())
            .or_default()
            .push(RandomCheckInitRow {
                pk_value: pk_value.clone(),
                record: record.clone(),
            });
        let observed_columns =
            Self::merge_dml_columns_with_records(table_info, &[], std::slice::from_ref(record));
        let mut init_columns = self.random_check_init_columns.lock().await;
        let entry = init_columns.entry(key).or_default();
        let merged_columns = Self::merge_column_lists(entry, &observed_columns);
        *entry = merged_columns;
    }

    fn table_info_for_record(&self, record: &DataBuffer) -> Option<&TableInfoVo> {
        self.table_info_list.iter().find(|table_info| {
            table_info
                .table_name
                .eq_ignore_ascii_case(&record.table_name)
                && (record.source_database.trim().is_empty()
                    || table_info.source_database.trim().is_empty()
                    || table_info
                        .source_database
                        .eq_ignore_ascii_case(&record.source_database))
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn random_check_one_table(
        dameng_client: &mut Client,
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        fallback_schema: &str,
        table_info: &TableInfoVo,
        expected_rows: &[RandomCheckInitRow],
        init_columns: &[String],
    ) -> Result<RandomCheckTableSummary, String> {
        let columns = Self::merge_column_lists(&table_info.columns, init_columns);
        let plans = Self::random_check_column_plans_for_columns(table_info, &columns);
        if plans.is_empty() {
            return Err("no columns for random data check".to_string());
        }
        let target_schema = Self::table_info_target_schema(table_info, fallback_schema);
        let target_sql =
            Self::dameng_random_check_select_sql(target_schema.as_str(), table_info, &plans);
        let source_table = Self::table_info_source_table_key(table_info);
        let target_table =
            Self::target_table_key(target_schema.as_str(), table_info.table_name.as_str());
        info!(
            "Dameng random data check table started: {} -> {} pk_count={}",
            source_table,
            target_table,
            expected_rows.len()
        );
        Self::append_random_check_result_line(
            format!(
                "TABLE_START source_table={} target_table={} pk_count={} columns={}",
                source_table,
                target_table,
                expected_rows.len(),
                plans.len()
            )
            .as_str(),
        );
        if expected_rows.is_empty() {
            info!(
                "Dameng random data check table skipped because no init pk was recorded: {}",
                source_table
            );
            Self::append_random_check_result_line(
                format!(
                    "TABLE_SKIPPED source_table={} reason=no init pk",
                    source_table
                )
                .as_str(),
            );
            return Ok(RandomCheckTableSummary::default());
        }

        let mut summary = RandomCheckTableSummary::default();
        for expected_row in expected_rows {
            let pk_value = &expected_row.pk_value;
            let pk_display = pk_value.resolve_string();
            let source_values =
                Self::random_check_init_record_compare_values(&expected_row.record, &plans);
            let target_values = match Self::dameng_target_compare_values_with_fallback(
                dameng_client,
                host,
                port,
                username,
                password,
                target_schema.as_str(),
                table_info,
                target_sql.as_str(),
                pk_value,
                &plans,
                &source_values,
            ) {
                Ok(values) => values,
                Err(e) if Self::is_dameng_connection_error(e.as_str()) => {
                    warn!(
                        "Dameng random data check target query connection lost, reconnect and retry once: table={} pk={} error={}",
                        target_table, pk_display, e
                    );
                    *dameng_client = Self::connect_client(host, port, username, password).map_err(
                        |connect_error| {
                            format!(
                                "{}; reconnect Dameng for random data check failed: {}",
                                e, connect_error
                            )
                        },
                    )?;
                    Self::dameng_target_compare_values_with_fallback(
                        dameng_client,
                        host,
                        port,
                        username,
                        password,
                        target_schema.as_str(),
                        table_info,
                        target_sql.as_str(),
                        pk_value,
                        &plans,
                        &source_values,
                    )
                    .map_err(|retry_error| {
                        format!("{}; retry after reconnect failed: {}", e, retry_error)
                    })?
                }
                Err(e) => return Err(e),
            };
            summary.checked_rows += 1;

            let Some(target_values) = target_values else {
                summary.mismatch_count += 1;
                Self::append_random_check_result_line(
                    format!(
                        "MISMATCH source_table={} target_table={} pk={} reason=target row not found source_row={}",
                        source_table,
                        target_table,
                        pk_display,
                        Self::describe_compare_row(&plans, &source_values)
                    )
                    .as_str(),
                );
                error!(
                    "Dameng random data check mismatch: source_table={} target_table={} pk={} reason=target row not found source_row={}",
                    source_table,
                    target_table,
                    pk_display,
                    Self::describe_compare_row(&plans, &source_values)
                );
                continue;
            };

            for (idx, plan) in plans.iter().enumerate() {
                let source_value = source_values.get(idx).cloned().unwrap_or(None);
                let target_value = target_values.get(idx).cloned().unwrap_or(None);
                if Self::compare_values_equal(plan.kind, &source_value, &target_value) {
                    continue;
                }
                summary.mismatch_count += 1;
                Self::append_random_check_result_line(
                    format!(
                        "MISMATCH source_table={} target_table={} pk={} column={} target_column={} mysql_type={:?} reason={} source={} target={}",
                        source_table,
                        target_table,
                        pk_display,
                        plan.source_column,
                        plan.target_column,
                        plan.kind,
                        Self::compare_mismatch_reason(&source_value, &target_value),
                        Self::describe_compare_value(&source_value),
                        Self::describe_compare_value(&target_value)
                    )
                    .as_str(),
                );
                error!(
                    "Dameng random data check mismatch: source_table={} target_table={} pk={} column={} target_column={} mysql_type={:?} reason={} source={} target={}",
                    source_table,
                    target_table,
                    pk_display,
                    plan.source_column,
                    plan.target_column,
                    plan.kind,
                    Self::compare_mismatch_reason(&source_value, &target_value),
                    Self::describe_compare_value(&source_value),
                    Self::describe_compare_value(&target_value)
                );
            }
        }

        if summary.mismatch_count == 0 {
            Self::append_random_check_result_line(
                format!(
                    "TABLE_SUCCESS source_table={} target_table={} checked_rows={}",
                    source_table, target_table, summary.checked_rows
                )
                .as_str(),
            );
            info!(
                "Dameng random data check table success: {} -> {} checked_rows={}",
                source_table, target_table, summary.checked_rows
            );
        } else {
            Self::append_random_check_result_line(
                format!(
                    "TABLE_MISMATCH source_table={} target_table={} checked_rows={} mismatches={}",
                    source_table, target_table, summary.checked_rows, summary.mismatch_count
                )
                .as_str(),
            );
            error!(
                "Dameng random data check table has mismatches: {} -> {} checked_rows={} mismatches={}",
                source_table, target_table, summary.checked_rows, summary.mismatch_count
            );
        }
        Ok(summary)
    }

    fn random_check_init_record_compare_values(
        record: &DataBuffer,
        plans: &[RandomCheckColumnPlan],
    ) -> Vec<Option<String>> {
        plans
            .iter()
            .map(|plan| {
                let value = if record.after.contains_key(plan.source_column.as_str()) {
                    record.after.get(plan.source_column.as_str())
                } else if record.before.contains_key(plan.source_column.as_str()) {
                    record.before.get(plan.source_column.as_str())
                } else {
                    &Value::None
                };
                Self::value_to_compare_string(value, plan.kind)
            })
            .collect()
    }

    fn value_to_compare_string(value: &Value, kind: MysqlCompareKind) -> Option<String> {
        if matches!(value, Value::None) {
            return None;
        }
        let value = match (kind, value) {
            (MysqlCompareKind::Binary, Value::Blob(_)) => value.resolve_string(),
            (_, Value::Blob(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => value.resolve_string(),
        };
        Some(Self::normalize_compare_string(kind, value.as_str()))
    }

    #[cfg(test)]
    fn random_check_column_plans(table_info: &TableInfoVo) -> Vec<RandomCheckColumnPlan> {
        Self::random_check_column_plans_for_columns(table_info, &table_info.columns)
    }

    fn random_check_column_plans_for_columns(
        table_info: &TableInfoVo,
        columns: &[String],
    ) -> Vec<RandomCheckColumnPlan> {
        let generated_columns = Self::generated_source_column_keys(table_info);
        columns
            .iter()
            .filter(|source_column| {
                !generated_columns.contains(source_column.to_ascii_lowercase().as_str())
            })
            .enumerate()
            .map(|(idx, source_column)| {
                let kind = Self::mysql_compare_kind_for_column(table_info, source_column);
                RandomCheckColumnPlan {
                    source_column: source_column.clone(),
                    target_column: Self::target_column_name(source_column),
                    alias: format!("__cdc_random_check_col_{}", idx),
                    kind,
                }
            })
            .collect()
    }

    fn mysql_compare_kind_for_column(
        table_info: &TableInfoVo,
        column_name: &str,
    ) -> MysqlCompareKind {
        let type_token =
            Self::mysql_type_token_for_column(table_info, column_name).unwrap_or_default();
        let t = type_token.to_ascii_lowercase();
        if t.starts_with("bit") {
            MysqlCompareKind::Bit
        } else if t.starts_with("tinyint")
            || t.starts_with("smallint")
            || t.starts_with("mediumint")
            || t.starts_with("int")
            || t.starts_with("integer")
            || t.starts_with("bigint")
            || t.starts_with("bool")
            || t.starts_with("boolean")
        {
            MysqlCompareKind::Integer
        } else if t.starts_with("decimal") || t.starts_with("numeric") {
            MysqlCompareKind::Decimal
        } else if t.starts_with("float") || t.starts_with("double") || t.starts_with("real") {
            MysqlCompareKind::Float
        } else if t.starts_with("datetime") || t.starts_with("timestamp") {
            MysqlCompareKind::DateTime
        } else if t.starts_with("date") {
            MysqlCompareKind::Date
        } else if t.starts_with("time") {
            MysqlCompareKind::Time
        } else if t.starts_with("year") {
            MysqlCompareKind::Year
        } else if t.starts_with("json") {
            MysqlCompareKind::Json
        } else if t.starts_with("char") {
            if Self::mysql_type_length(t.as_str())
                .is_some_and(|len| len > DAMENG_INLINE_STRING_CHAR_LIMIT)
            {
                MysqlCompareKind::Text
            } else {
                MysqlCompareKind::Char
            }
        } else if t.starts_with("varchar") {
            if Self::mysql_type_length(t.as_str())
                .is_some_and(|len| len > DAMENG_INLINE_STRING_CHAR_LIMIT)
            {
                MysqlCompareKind::Text
            } else {
                MysqlCompareKind::Varchar
            }
        } else if t.starts_with("enum") || t.starts_with("set") {
            MysqlCompareKind::Text
        } else if t.contains("blob") || t.contains("binary") {
            MysqlCompareKind::Binary
        } else {
            MysqlCompareKind::Text
        }
    }

    fn mysql_type_token_for_column(table_info: &TableInfoVo, column_name: &str) -> Option<String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let def = defs.get(column_name.to_ascii_lowercase().as_str())?;
        mysql_type_token_from_column_definition(def.as_str())
    }

    #[cfg(test)]
    fn mysql_random_check_sample_sql(
        table_info: &TableInfoVo,
        plans: &[RandomCheckColumnPlan],
        batch_size: usize,
    ) -> String {
        let mut select_items = Vec::with_capacity(plans.len() + 1);
        select_items.push(format!(
            "{} AS {}",
            common::quote_mysql_identifier(table_info.pk_column.as_str()),
            common::quote_mysql_identifier("__cdc_random_check_pk")
        ));
        for plan in plans {
            select_items.push(format!(
                "{} AS {}",
                Self::mysql_compare_expression(plan.source_column.as_str(), plan.kind),
                common::quote_mysql_identifier(plan.alias.as_str())
            ));
        }
        format!(
            "SELECT {} FROM {} ORDER BY RAND() LIMIT {}",
            select_items.join(", "),
            Self::qualified_mysql_table_for_check(
                table_info.source_database.as_str(),
                table_info.table_name.as_str()
            ),
            batch_size.max(1)
        )
    }

    #[cfg(test)]
    fn qualified_mysql_table_for_check(database: &str, table_name: &str) -> String {
        if database.trim().is_empty() {
            common::quote_mysql_identifier(table_name)
        } else {
            common::qualified_mysql_name(database, table_name)
        }
    }

    #[cfg(test)]
    fn mysql_compare_expression(column_name: &str, kind: MysqlCompareKind) -> String {
        let column = common::quote_mysql_identifier(column_name);
        let value_expr = match kind {
            MysqlCompareKind::Binary => format!("UPPER(HEX({}))", column),
            MysqlCompareKind::Bit => format!("CAST(CAST({} AS UNSIGNED) AS CHAR)", column),
            MysqlCompareKind::Date => format!("DATE_FORMAT({}, '%Y-%m-%d')", column),
            MysqlCompareKind::Time => format!("TIME_FORMAT({}, '%H:%i:%s.%f')", column),
            MysqlCompareKind::DateTime => {
                format!("DATE_FORMAT({}, '%Y-%m-%d %H:%i:%s.%f')", column)
            }
            MysqlCompareKind::Integer
            | MysqlCompareKind::Decimal
            | MysqlCompareKind::Float
            | MysqlCompareKind::Char
            | MysqlCompareKind::Varchar
            | MysqlCompareKind::Text
            | MysqlCompareKind::Json
            | MysqlCompareKind::Year => format!("CAST({} AS CHAR)", column),
        };
        format!(
            "CASE WHEN {} IS NULL THEN NULL ELSE {} END",
            column, value_expr
        )
    }

    fn dameng_random_check_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plans: &[RandomCheckColumnPlan],
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let payload_sql = plans
            .iter()
            .map(|plan| {
                let column_sql = format!(
                    "t.{}",
                    Self::quote_column_ident(plan.target_column.as_str())
                );
                Self::dameng_random_check_field_payload_sql(column_sql.as_str(), plan.kind)
            })
            .collect::<Vec<_>>()
            .join(" || ");
        format!(
            "SELECT {} AS \"__cdc_payload\" FROM {} t WHERE t.{} = ?",
            payload_sql, table_sql, pk_sql
        )
    }

    fn dameng_random_check_single_value_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        let value_sql = Self::dameng_random_check_value_sql(column_sql.as_str(), plan.kind);
        format!(
            "SELECT {} AS \"__cdc_value\" FROM {} t WHERE t.{} = ?",
            value_sql, table_sql, pk_sql
        )
    }

    fn dameng_random_check_text_length_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        let value_sql = Self::dameng_random_check_text_value_sql(column_sql.as_str());
        format!(
            "SELECT CAST(DBMS_LOB.GETLENGTH({}) AS VARCHAR(4000)) AS \"__cdc_value\" FROM {} t WHERE t.{} = ?",
            value_sql, table_sql, pk_sql
        )
    }

    fn dameng_random_check_text_chunk_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
        start: usize,
        len: usize,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        let value_sql = Self::dameng_random_check_text_value_sql(column_sql.as_str());
        format!(
            "SELECT CAST(DBMS_LOB.SUBSTR({}, {}, {}) AS VARCHAR(4000)) AS \"__cdc_value\" FROM {} t WHERE t.{} = ?",
            value_sql, len, start, table_sql, pk_sql
        )
    }

    fn dameng_random_check_text_chunk_match_count_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
        start: usize,
        len: usize,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        let value_sql = Self::dameng_random_check_text_value_sql(column_sql.as_str());
        format!(
            "SELECT CAST(COUNT(*) AS VARCHAR(4000)) AS \"__cdc_matched\" FROM {} t WHERE t.{} = ? AND CAST(DBMS_LOB.SUBSTR({}, {}, {}) AS VARCHAR(4000)) = ?",
            table_sql, pk_sql, value_sql, len, start
        )
    }

    fn dameng_random_check_binary_length_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        format!(
            "SELECT CAST(DBMS_LOB.GETLENGTH({}) AS VARCHAR(4000)) AS \"__cdc_value\" FROM {} t WHERE t.{} = ?",
            column_sql, table_sql, pk_sql
        )
    }

    fn dameng_random_check_binary_chunk_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
        start: usize,
        len: usize,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        format!(
            "SELECT RAWTOHEX(DBMS_LOB.SUBSTR({}, {}, {})) AS \"__cdc_value\" FROM {} t WHERE t.{} = ?",
            column_sql, len, start, table_sql, pk_sql
        )
    }

    fn dameng_random_check_null_marker_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
    ) -> String {
        format!(
            "SELECT CAST(CASE WHEN t.{} IS NULL THEN 1 ELSE 0 END AS INT) AS \"__cdc_is_null\" FROM {} t WHERE t.{} = ?",
            Self::quote_column_ident(plan.target_column.as_str()),
            Self::qualified_table(schema, table_info.table_name.as_str()),
            Self::quote_column_ident(table_info.pk_column.as_str())
        )
    }

    fn dameng_random_check_string_match_count_select_sql(
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
        source_is_null: bool,
    ) -> String {
        let table_sql = Self::qualified_table(schema, table_info.table_name.as_str());
        let pk_sql = Self::quote_column_ident(table_info.pk_column.as_str());
        let column_sql = format!(
            "t.{}",
            Self::quote_column_ident(plan.target_column.as_str())
        );
        let compare_sql = if source_is_null {
            format!("{} IS NULL", column_sql)
        } else if matches!(plan.kind, MysqlCompareKind::Char) {
            format!("RTRIM(CAST({} AS VARCHAR(4000))) = ?", column_sql)
        } else {
            format!("CAST({} AS VARCHAR(4000)) = ?", column_sql)
        };
        format!(
            "SELECT CAST(COUNT(*) AS VARCHAR(4000)) AS \"__cdc_matched\" FROM {} t WHERE t.{} = ? AND {}",
            table_sql, pk_sql, compare_sql
        )
    }

    fn dameng_random_check_field_payload_sql(column_sql: &str, kind: MysqlCompareKind) -> String {
        let value_sql = Self::dameng_random_check_value_sql(column_sql, kind);
        format!(
            "CASE WHEN {} IS NULL THEN 'N;' ELSE 'V' || LENGTH({}) || ':' || {} END",
            column_sql, value_sql, value_sql
        )
    }

    fn dameng_random_check_value_sql(column_sql: &str, kind: MysqlCompareKind) -> String {
        match kind {
            MysqlCompareKind::Binary => format!("CAST(RAWTOHEX({}) AS CLOB)", column_sql),
            MysqlCompareKind::Date => {
                format!("CAST(TO_CHAR({}, 'YYYY-MM-DD') AS CLOB)", column_sql)
            }
            MysqlCompareKind::Time => {
                format!("CAST(TO_CHAR({}, 'HH24:MI:SS.FF6') AS CLOB)", column_sql)
            }
            MysqlCompareKind::DateTime => format!(
                "CAST(TO_CHAR({}, 'YYYY-MM-DD HH24:MI:SS.FF6') AS CLOB)",
                column_sql
            ),
            MysqlCompareKind::Bit => {
                format!("CAST(CAST({} AS VARCHAR(4000)) AS CLOB)", column_sql)
            }
            MysqlCompareKind::Char | MysqlCompareKind::Varchar => {
                format!("TO_CHAR({})", column_sql)
            }
            MysqlCompareKind::Text | MysqlCompareKind::Json => {
                format!("CAST({} AS CLOB)", column_sql)
            }
            MysqlCompareKind::Integer
            | MysqlCompareKind::Decimal
            | MysqlCompareKind::Float
            | MysqlCompareKind::Year => {
                format!("CAST(CAST({} AS VARCHAR(4000)) AS CLOB)", column_sql)
            }
        }
    }

    fn dameng_random_check_text_value_sql(column_sql: &str) -> String {
        format!("CAST({} AS CLOB)", column_sql)
    }

    fn dameng_target_compare_values(
        dameng_client: &mut Client,
        target_sql: &str,
        pk_value: &Value,
        plans: &[RandomCheckColumnPlan],
    ) -> Result<Option<Vec<Option<String>>>, String> {
        let params = vec![Self::value_to_param(pk_value)];
        let rows = Self::dameng_client_query_with_params(dameng_client, target_sql, &params)
            .map_err(|e| {
                format!(
                    "query Dameng target row failed: sql={} error={}",
                    target_sql, e
                )
            })?;
        if rows.rows.is_empty() {
            return Ok(None);
        }
        if rows.rows.len() > 1 {
            return Err(format!(
                "Dameng target query returned duplicate rows: pk={} sql={} rows={}",
                pk_value.resolve_string(),
                target_sql,
                rows.rows.len()
            ));
        }
        let column = rows.columns.first();
        let row_values = rows.rows[0].values.clone();
        let raw_payload = row_values.first().and_then(|value| value.as_deref());
        let payload = match column {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_payload, column)?
            }
            None => Self::dameng_raw_text_fallback_for_compare(raw_payload),
        }
        .ok_or_else(|| {
            format!(
                "Dameng target payload is NULL: pk={} sql={}",
                pk_value.resolve_string(),
                target_sql
            )
        })?;
        let parsed_values = Self::parse_random_check_payload(payload.as_str(), plans.len())?;
        let mut values = Vec::with_capacity(plans.len());
        for (idx, plan) in plans.iter().enumerate() {
            let value = parsed_values.get(idx).cloned().unwrap_or(None);
            values.push(Self::normalize_optional_compare_value(plan.kind, value));
        }
        Ok(Some(values))
    }

    #[allow(clippy::too_many_arguments)]
    fn dameng_target_compare_values_with_fallback(
        dameng_client: &mut Client,
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        schema: &str,
        table_info: &TableInfoVo,
        target_sql: &str,
        pk_value: &Value,
        plans: &[RandomCheckColumnPlan],
        source_values: &[Option<String>],
    ) -> Result<Option<Vec<Option<String>>>, String> {
        if !Self::random_check_should_use_payload(plans) {
            debug!(
                "Dameng random data check skips payload query for wide table and uses per-column queries: table={} columns={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                plans.len()
            );
            return Self::dameng_target_compare_values_by_column(
                dameng_client,
                host,
                port,
                username,
                password,
                schema,
                table_info,
                pk_value,
                plans,
                source_values,
            );
        }

        match Self::dameng_target_compare_values(dameng_client, target_sql, pk_value, plans) {
            Ok(Some(values)) => Ok(Some(values)),
            Ok(None) => {
                warn!(
                    "Dameng random data check payload query returned no row, reconnect and fallback to per-column target queries: table={} pk={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                );
                *dameng_client =
                    Self::connect_client(host, port, username, password).map_err(|connect_error| {
                        format!(
                            "reconnect Dameng before random data check fallback failed: table={} pk={} error={}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            pk_value.resolve_string(),
                            connect_error
                        )
                    })?;
                Self::dameng_target_compare_values_by_column(
                    dameng_client,
                    host,
                    port,
                    username,
                    password,
                    schema,
                    table_info,
                    pk_value,
                    plans,
                    source_values,
                )
            }
            Err(e) if Self::is_random_check_payload_unstable_error(e.as_str()) => {
                warn!(
                    "Dameng random data check payload query is unstable, reconnect and fallback to per-column target queries: table={} pk={} error={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    e
                );
                *dameng_client =
                    Self::connect_client(host, port, username, password).map_err(|connect_error| {
                        format!(
                            "{}; reconnect Dameng before random data check fallback failed: table={} pk={} error={}",
                            e,
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            pk_value.resolve_string(),
                            connect_error
                        )
                    })?;
                Self::dameng_target_compare_values_by_column(
                    dameng_client,
                    host,
                    port,
                    username,
                    password,
                    schema,
                    table_info,
                    pk_value,
                    plans,
                    source_values,
                )
            }
            Err(e) => Err(e),
        }
    }

    fn random_check_should_use_payload(plans: &[RandomCheckColumnPlan]) -> bool {
        plans.len() <= DAMENG_RANDOM_CHECK_PAYLOAD_COLUMN_LIMIT
    }

    fn is_random_check_payload_unstable_error(error: &str) -> bool {
        Self::is_dameng_string_truncation_error(error)
            || Self::is_random_check_payload_parse_error(error)
            || Self::is_dameng_connection_error(error)
            || error.contains("Dameng target query returned duplicate rows")
    }

    fn is_random_check_payload_parse_error(error: &str) -> bool {
        error.contains("Dameng target payload")
    }

    #[allow(clippy::too_many_arguments)]
    fn dameng_target_compare_values_by_column(
        dameng_client: &mut Client,
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plans: &[RandomCheckColumnPlan],
        source_values: &[Option<String>],
    ) -> Result<Option<Vec<Option<String>>>, String> {
        let mut values = Vec::with_capacity(plans.len());
        for (idx, plan) in plans.iter().enumerate() {
            if matches!(plan.kind, MysqlCompareKind::Binary) {
                let value = Self::dameng_target_compare_binary_value_by_chunks(
                    dameng_client,
                    schema,
                    table_info,
                    pk_value,
                    plan,
                )?;
                if value.is_none()
                    && !Self::dameng_target_row_exists(dameng_client, schema, table_info, pk_value)?
                {
                    if idx == 0 {
                        return Ok(None);
                    }
                    return Err(format!(
                        "Dameng target binary chunk query returned no row after previous columns existed: table={} pk={} column={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        pk_value.resolve_string(),
                        plan.target_column
                    ));
                }
                values.push(Self::normalize_optional_compare_value(plan.kind, value));
                continue;
            }

            if matches!(plan.kind, MysqlCompareKind::Text | MysqlCompareKind::Json) {
                let source_value = source_values.get(idx).cloned().unwrap_or(None);
                if Self::random_check_can_compare_text_inline(&source_value) {
                    let direct_compare_note: String;
                    match Self::dameng_target_string_value_match_count(
                        dameng_client,
                        schema,
                        table_info,
                        pk_value,
                        plan,
                        &source_value,
                    ) {
                        Ok(None) => {
                            if idx == 0 {
                                return Ok(None);
                            }
                            return Err(format!(
                                "Dameng target text equality query returned no row after previous columns existed: table={} pk={} column={}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                pk_value.resolve_string(),
                                plan.target_column
                            ));
                        }
                        Ok(Some(1)) => {
                            values.push(Self::normalize_optional_compare_value(
                                plan.kind,
                                source_value,
                            ));
                            continue;
                        }
                        Ok(Some(matched_rows)) => {
                            direct_compare_note = format!(
                                "matched_rows={} by simple text equality count",
                                matched_rows
                            );
                            if matched_rows > 1 {
                                values.push(Some(format!(
                                    "<Dameng target text equality matched multiple rows: matched_rows={}>",
                                    matched_rows
                                )));
                                continue;
                            }
                            if !Self::dameng_target_row_exists(
                                dameng_client,
                                schema,
                                table_info,
                                pk_value,
                            )? {
                                if idx == 0 {
                                    return Ok(None);
                                }
                                return Err(format!(
                                    "Dameng target text equality query matched no row and target row is missing after previous columns existed: table={} pk={} column={}",
                                    Self::target_table_key(schema, table_info.table_name.as_str()),
                                    pk_value.resolve_string(),
                                    plan.target_column
                                ));
                            }
                        }
                        Err(e) if Self::is_random_check_column_unstable_error(e.as_str()) => {
                            warn!(
                                "Dameng random data check text equality query is unstable, reconnect and retry simple equality count: table={} pk={} column={} error={}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                pk_value.resolve_string(),
                                plan.target_column,
                                e
                            );
                            *dameng_client = Self::connect_client(host, port, username, password)
                                .map_err(|connect_error| {
                                    format!(
                                        "{}; reconnect Dameng before random data check text read fallback failed: table={} pk={} column={} error={}",
                                        e,
                                        Self::target_table_key(
                                            schema,
                                            table_info.table_name.as_str()
                                        ),
                                        pk_value.resolve_string(),
                                        plan.target_column,
                                        connect_error
                                    )
                                })?;
                            match Self::dameng_target_string_value_match_count(
                                dameng_client,
                                schema,
                                table_info,
                                pk_value,
                                plan,
                                &source_value,
                            ) {
                                Ok(Some(1)) => {
                                    values.push(Self::normalize_optional_compare_value(
                                        plan.kind,
                                        source_value,
                                    ));
                                    continue;
                                }
                                Ok(Some(matched_rows)) => {
                                    direct_compare_note = format!(
                                        "matched_rows={} by simple text equality count after reconnect",
                                        matched_rows
                                    );
                                    if matched_rows > 1 {
                                        values.push(Some(format!(
                                            "<Dameng target text equality matched multiple rows after reconnect: matched_rows={}>",
                                            matched_rows
                                        )));
                                        continue;
                                    }
                                    if !Self::dameng_target_row_exists(
                                        dameng_client,
                                        schema,
                                        table_info,
                                        pk_value,
                                    )? {
                                        if idx == 0 {
                                            return Ok(None);
                                        }
                                        return Err(format!(
                                            "Dameng target text equality retry matched no row and target row is missing after previous columns existed: table={} pk={} column={}",
                                            Self::target_table_key(
                                                schema,
                                                table_info.table_name.as_str()
                                            ),
                                            pk_value.resolve_string(),
                                            plan.target_column
                                        ));
                                    }
                                }
                                Ok(None) => {
                                    if idx == 0 {
                                        return Ok(None);
                                    }
                                    return Err(format!(
                                        "Dameng target text equality retry returned no aggregate row after previous columns existed: table={} pk={} column={}",
                                        Self::target_table_key(
                                            schema,
                                            table_info.table_name.as_str()
                                        ),
                                        pk_value.resolve_string(),
                                        plan.target_column
                                    ));
                                }
                                Err(retry_error)
                                    if Self::is_random_check_column_unstable_error(
                                        retry_error.as_str(),
                                    ) =>
                                {
                                    direct_compare_note = format!(
                                        "simple text equality count failed after reconnect: {}; retry_error={}",
                                        e, retry_error
                                    );
                                }
                                Err(retry_error) => return Err(retry_error),
                            }
                        }
                        Err(e) => return Err(e),
                    }

                    match Self::dameng_target_compare_text_value_by_chunks(
                        dameng_client,
                        schema,
                        table_info,
                        pk_value,
                        plan,
                    ) {
                        Ok(value) => {
                            values.push(Self::normalize_optional_compare_value(plan.kind, value));
                            continue;
                        }
                        Err(chunk_error) => {
                            values.push(Some(format!(
                                "<Dameng target text value is not equal by SQL equality check ({}) and target text read failed: {}>",
                                direct_compare_note, chunk_error
                            )));
                            continue;
                        }
                    }
                }

                if let Some(source_text) = source_value.as_deref() {
                    match Self::dameng_target_text_matches_source_by_chunks(
                        dameng_client,
                        schema,
                        table_info,
                        pk_value,
                        plan,
                        source_text,
                    ) {
                        Ok(TextChunkCompareResult::Equal) => {
                            values.push(Self::normalize_optional_compare_value(
                                plan.kind,
                                source_value,
                            ));
                            continue;
                        }
                        Ok(TextChunkCompareResult::Missing) => {
                            if idx == 0 {
                                return Ok(None);
                            }
                            return Err(format!(
                                "Dameng target text chunk equality query returned no row after previous columns existed: table={} pk={} column={}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                pk_value.resolve_string(),
                                plan.target_column
                            ));
                        }
                        Ok(TextChunkCompareResult::Different(note)) => {
                            match Self::dameng_target_compare_text_value_by_chunks(
                                dameng_client,
                                schema,
                                table_info,
                                pk_value,
                                plan,
                            ) {
                                Ok(value) => {
                                    values.push(Self::normalize_optional_compare_value(
                                        plan.kind, value,
                                    ));
                                    continue;
                                }
                                Err(chunk_error) => {
                                    values.push(Some(format!(
                                        "<Dameng target text differs by DB chunk equality check ({}) and target text read failed: {}>",
                                        note, chunk_error
                                    )));
                                    continue;
                                }
                            }
                        }
                        Err(e) if Self::is_random_check_column_unstable_error(e.as_str()) => {
                            warn!(
                                "Dameng random data check text chunk equality query is unstable, reconnect and fallback to target text read: table={} pk={} column={} error={}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                pk_value.resolve_string(),
                                plan.target_column,
                                e
                            );
                            *dameng_client = Self::connect_client(host, port, username, password)
                                .map_err(|connect_error| {
                                    format!(
                                        "{}; reconnect Dameng before random data check text read failed: table={} pk={} column={} error={}",
                                        e,
                                        Self::target_table_key(
                                            schema,
                                            table_info.table_name.as_str()
                                        ),
                                        pk_value.resolve_string(),
                                        plan.target_column,
                                        connect_error
                                    )
                                })?;
                        }
                        Err(e) => return Err(e),
                    }
                }

                let value = match Self::dameng_target_compare_text_value_by_chunks(
                    dameng_client,
                    schema,
                    table_info,
                    pk_value,
                    plan,
                ) {
                    Ok(value) => value,
                    Err(chunk_error) => {
                        values.push(Some(format!(
                            "<Dameng target text read failed during chunk compare: {}>",
                            chunk_error
                        )));
                        continue;
                    }
                };
                if value.is_none()
                    && !Self::dameng_target_row_exists(dameng_client, schema, table_info, pk_value)?
                {
                    if idx == 0 {
                        return Ok(None);
                    }
                    return Err(format!(
                        "Dameng target text chunk query returned no row after previous columns existed: table={} pk={} column={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        pk_value.resolve_string(),
                        plan.target_column
                    ));
                }
                values.push(Self::normalize_optional_compare_value(plan.kind, value));
                continue;
            }

            if matches!(
                plan.kind,
                MysqlCompareKind::Char | MysqlCompareKind::Varchar
            ) {
                let source_value = source_values.get(idx).cloned().unwrap_or(None);
                let direct_compare_note: String;
                match Self::dameng_target_string_value_match_count(
                    dameng_client,
                    schema,
                    table_info,
                    pk_value,
                    plan,
                    &source_value,
                ) {
                    Ok(None) => {
                        if idx == 0 {
                            return Ok(None);
                        }
                        return Err(format!(
                            "Dameng target string equality query returned no row after previous columns existed: table={} pk={} column={}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            pk_value.resolve_string(),
                            plan.target_column
                        ));
                    }
                    Ok(Some(1)) => {
                        values.push(source_value);
                        continue;
                    }
                    Ok(Some(matched_rows)) => {
                        direct_compare_note =
                            format!("matched_rows={} by simple equality count", matched_rows);
                        if matched_rows > 1 {
                            values.push(Some(format!(
                                "<Dameng target string equality matched multiple rows: matched_rows={}>",
                                matched_rows
                            )));
                            continue;
                        }
                        if !Self::dameng_target_row_exists(
                            dameng_client,
                            schema,
                            table_info,
                            pk_value,
                        )? {
                            if idx == 0 {
                                return Ok(None);
                            }
                            return Err(format!(
                                "Dameng target string equality query matched no row and target row is missing after previous columns existed: table={} pk={} column={}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                pk_value.resolve_string(),
                                plan.target_column
                            ));
                        }
                    }
                    Err(e) if Self::is_random_check_column_unstable_error(e.as_str()) => {
                        warn!(
                            "Dameng random data check varchar equality query is unstable, reconnect and retry simple equality count: table={} pk={} column={} error={}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            pk_value.resolve_string(),
                            plan.target_column,
                            e
                        );
                        *dameng_client = Self::connect_client(host, port, username, password)
                            .map_err(|connect_error| {
                                format!(
                                    "{}; reconnect Dameng before random data check varchar read fallback failed: table={} pk={} column={} error={}",
                                    e,
                                    Self::target_table_key(schema, table_info.table_name.as_str()),
                                    pk_value.resolve_string(),
                                    plan.target_column,
                                    connect_error
                                )
                            })?;
                        match Self::dameng_target_string_value_match_count(
                            dameng_client,
                            schema,
                            table_info,
                            pk_value,
                            plan,
                            &source_value,
                        ) {
                            Ok(Some(1)) => {
                                values.push(source_value);
                                continue;
                            }
                            Ok(Some(matched_rows)) => {
                                direct_compare_note = format!(
                                    "matched_rows={} by simple equality count after reconnect",
                                    matched_rows
                                );
                                if matched_rows > 1 {
                                    values.push(Some(format!(
                                        "<Dameng target string equality matched multiple rows after reconnect: matched_rows={}>",
                                        matched_rows
                                    )));
                                    continue;
                                }
                                if !Self::dameng_target_row_exists(
                                    dameng_client,
                                    schema,
                                    table_info,
                                    pk_value,
                                )? {
                                    if idx == 0 {
                                        return Ok(None);
                                    }
                                    return Err(format!(
                                        "Dameng target string equality retry matched no row and target row is missing after previous columns existed: table={} pk={} column={}",
                                        Self::target_table_key(
                                            schema,
                                            table_info.table_name.as_str()
                                        ),
                                        pk_value.resolve_string(),
                                        plan.target_column
                                    ));
                                }
                            }
                            Ok(None) => {
                                if idx == 0 {
                                    return Ok(None);
                                }
                                return Err(format!(
                                    "Dameng target string equality retry returned no aggregate row after previous columns existed: table={} pk={} column={}",
                                    Self::target_table_key(schema, table_info.table_name.as_str()),
                                    pk_value.resolve_string(),
                                    plan.target_column
                                ));
                            }
                            Err(retry_error)
                                if Self::is_random_check_column_unstable_error(
                                    retry_error.as_str(),
                                ) =>
                            {
                                direct_compare_note = format!(
                                    "simple equality count failed after reconnect: {}; retry_error={}",
                                    e, retry_error
                                );
                            }
                            Err(retry_error) => return Err(retry_error),
                        }
                    }
                    Err(e) => return Err(e),
                }

                match Self::dameng_target_compare_single_value_by_column(
                    dameng_client,
                    schema,
                    table_info,
                    pk_value,
                    plan,
                ) {
                    Ok(Some(value)) => {
                        values.push(Self::normalize_optional_compare_value(plan.kind, value));
                        continue;
                    }
                    Ok(None) => {
                        if idx == 0 {
                            return Ok(None);
                        }
                        return Err(format!(
                            "Dameng target column query returned no row after previous columns existed: table={} pk={} column={}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            pk_value.resolve_string(),
                            plan.target_column
                        ));
                    }
                    Err(e) if Self::is_random_check_column_unstable_error(e.as_str()) => {
                        warn!(
                            "Dameng random data check varchar column query is unstable, reconnect and fallback to text chunks: table={} pk={} column={} error={}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            pk_value.resolve_string(),
                            plan.target_column,
                            e
                        );
                        *dameng_client = Self::connect_client(host, port, username, password)
                            .map_err(|connect_error| {
                                format!(
                                    "{}; reconnect Dameng before random data check varchar chunk fallback failed: table={} pk={} column={} error={}",
                                    e,
                                    Self::target_table_key(schema, table_info.table_name.as_str()),
                                    pk_value.resolve_string(),
                                    plan.target_column,
                                    connect_error
                                )
                            })?;
                        let value = match Self::dameng_target_compare_text_value_by_chunks(
                            dameng_client,
                            schema,
                            table_info,
                            pk_value,
                            plan,
                        ) {
                            Ok(value) => value,
                            Err(chunk_error) => {
                                let note = direct_compare_note.as_str();
                                values.push(Some(format!(
                                    "<Dameng target value is not equal by SQL equality check ({}) and target value read failed: {}; varchar chunk fallback failed: {}>",
                                    note, e, chunk_error
                                )));
                                continue;
                            }
                        };
                        if value.is_none()
                            && !Self::dameng_target_row_exists(
                                dameng_client,
                                schema,
                                table_info,
                                pk_value,
                            )?
                        {
                            if idx == 0 {
                                return Ok(None);
                            }
                            return Err(format!(
                                "Dameng target text chunk query returned no row after previous columns existed: table={} pk={} column={}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                pk_value.resolve_string(),
                                plan.target_column
                            ));
                        }
                        values.push(Self::normalize_optional_compare_value(plan.kind, value));
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            match Self::dameng_target_compare_single_value_by_column(
                dameng_client,
                schema,
                table_info,
                pk_value,
                plan,
            )? {
                Some(value) => {
                    values.push(Self::normalize_optional_compare_value(plan.kind, value))
                }
                None => {
                    if idx == 0 {
                        return Ok(None);
                    }
                    return Err(format!(
                        "Dameng target column query returned no row after previous columns existed: table={} pk={} column={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        pk_value.resolve_string(),
                        plan.target_column
                    ));
                }
            }
        }
        Ok(Some(values))
    }

    fn is_random_check_column_unstable_error(error: &str) -> bool {
        Self::is_dameng_connection_error(error) || Self::is_dameng_string_truncation_error(error)
    }

    fn dameng_target_compare_single_value_by_column(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
    ) -> Result<Option<Option<String>>, String> {
        if matches!(
            plan.kind,
            MysqlCompareKind::Char | MysqlCompareKind::Varchar
        ) {
            match Self::dameng_target_column_is_null(
                dameng_client,
                schema,
                table_info,
                pk_value,
                plan,
            )? {
                None => return Ok(None),
                Some(true) => return Ok(Some(None)),
                Some(false) => {}
            }
        }

        let params = vec![Self::value_to_param(pk_value)];
        let sql = Self::dameng_random_check_single_value_select_sql(schema, table_info, plan);
        let rows = Self::dameng_client_query_with_params(dameng_client, sql.as_str(), &params)
            .map_err(|e| {
                format!(
                    "query Dameng target column failed: table={} pk={} column={} sql={} error={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    sql,
                    e
                )
            })?;
        if rows.rows.is_empty() {
            if Self::dameng_target_row_exists(dameng_client, schema, table_info, pk_value)? {
                return Ok(Some(None));
            }
            return Ok(None);
        }
        if rows.rows.len() > 1 {
            warn!(
                "Dameng target column query returned duplicate rows, use first row for random data check: table={} pk={} column={} sql={} rows={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                sql,
                rows.rows.len()
            );
        }
        let column = rows.columns.first();
        let row_values = rows.rows[0].values.clone();
        let raw_value = row_values.first().and_then(|value| value.as_deref());
        let value = match column {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_value, column)?
            }
            None => Self::dameng_raw_text_fallback_for_compare(raw_value),
        };
        Ok(Some(value))
    }

    fn dameng_target_column_is_null(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
    ) -> Result<Option<bool>, String> {
        let params = vec![Self::value_to_param(pk_value)];
        let sql = Self::dameng_random_check_null_marker_select_sql(schema, table_info, plan);
        let rows = Self::dameng_client_query_with_params(dameng_client, sql.as_str(), &params)
            .map_err(|e| {
                format!(
                    "query Dameng target column null marker failed: table={} pk={} column={} sql={} error={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    sql,
                    e
                )
            })?;
        if rows.rows.is_empty() {
            return Ok(None);
        }
        if rows.rows.len() > 1 {
            warn!(
                "Dameng target column null marker query returned duplicate rows, use first row for random data check: table={} pk={} column={} sql={} rows={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                sql,
                rows.rows.len()
            );
        }
        let column = rows.columns.first();
        let row_values = rows.rows[0].values.clone();
        let raw_value = row_values.first().and_then(|value| value.as_deref());
        let marker = match column {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_value, column)?
            }
            None => Self::dameng_raw_text_fallback_for_compare(raw_value),
        }
        .ok_or_else(|| {
            format!(
                "Dameng target column null marker is NULL: table={} pk={} column={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column
            )
        })?;
        match marker.trim() {
            "1" => Ok(Some(true)),
            "0" => Ok(Some(false)),
            other => Err(format!(
                "Dameng target column null marker is invalid: table={} pk={} column={} marker={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                other
            )),
        }
    }

    fn dameng_target_row_exists(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
    ) -> Result<bool, String> {
        let sql = format!(
            "SELECT CAST(COUNT(*) AS VARCHAR(4000)) AS \"__cdc_count\" FROM {} t WHERE t.{} = ?",
            Self::qualified_table(schema, table_info.table_name.as_str()),
            Self::quote_column_ident(table_info.pk_column.as_str())
        );
        let params = vec![Self::value_to_param(pk_value)];
        let rows = Self::dameng_client_query_with_params(dameng_client, sql.as_str(), &params)
            .map_err(|e| {
                format!(
                    "query Dameng target row exists failed: table={} pk={} sql={} error={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    sql,
                    e
                )
            })?;
        let raw_count_value = rows
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_deref());
        let count_value = match rows.columns.first() {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_count_value, column)?
            }
            None => Self::dameng_raw_text_fallback_for_compare(raw_count_value),
        }
        .unwrap_or_else(|| "0".to_string());
        let count = Self::parse_dameng_length_value(count_value.as_str()).map_err(|e| {
            format!(
                "parse Dameng target row exists count failed: table={} pk={} value={} error={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                Self::preview_value(count_value.as_str(), 200),
                e
            )
        })?;
        Ok(count > 0)
    }

    fn dameng_target_string_value_match_count(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
        source_value: &Option<String>,
    ) -> Result<Option<usize>, String> {
        let sql = Self::dameng_random_check_string_match_count_select_sql(
            schema,
            table_info,
            plan,
            source_value.is_none(),
        );
        let mut params = Vec::with_capacity(if source_value.is_some() { 2 } else { 1 });
        params.push(Self::value_to_param(pk_value));
        if let Some(value) = source_value {
            params.push(DamengParam::Text(value.clone()));
        }
        let rows = Self::dameng_client_query_with_params(dameng_client, sql.as_str(), &params)
            .map_err(|e| {
                format!(
                    "query Dameng target string equality count failed: table={} pk={} column={} sql={} error={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    sql,
                    e
                )
            })?;
        if rows.rows.is_empty() {
            return Ok(None);
        }
        if rows.rows.len() > 1 {
            warn!(
                "Dameng target string equality count query returned duplicate aggregate rows, use first row for random data check: table={} pk={} column={} sql={} rows={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                sql,
                rows.rows.len()
            );
        }

        let row_values = rows.rows[0].values.clone();
        let raw_matched = row_values.first().and_then(|value| value.as_deref());
        let matched_value = match rows.columns.first() {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_matched, column)?
            }
            None => Self::dameng_raw_text_fallback_for_compare(raw_matched),
        }
        .unwrap_or_else(|| "0".to_string());
        let matched_rows = Self::parse_dameng_length_value(matched_value.as_str()).map_err(|e| {
            format!(
                "parse Dameng target string equality count failed: table={} pk={} column={} value={} error={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                Self::preview_value(matched_value.as_str(), 200),
                e
            )
        })?;
        Ok(Some(matched_rows))
    }

    fn random_check_can_compare_text_inline(source_value: &Option<String>) -> bool {
        source_value
            .as_ref()
            .map(|value| value.chars().count() <= 4000)
            .unwrap_or(true)
    }

    fn dameng_target_text_chunk_match_count(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
        start: usize,
        chunk: &str,
    ) -> Result<Option<usize>, String> {
        let len = chunk.chars().count();
        let sql = Self::dameng_random_check_text_chunk_match_count_select_sql(
            schema, table_info, plan, start, len,
        );
        let params = vec![
            Self::value_to_param(pk_value),
            DamengParam::Text(chunk.to_string()),
        ];
        let rows = Self::dameng_client_query_with_params(dameng_client, sql.as_str(), &params)
            .map_err(|e| {
                format!(
                    "query Dameng target text chunk equality count failed: table={} pk={} column={} start={} len={} sql={} error={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    start,
                    len,
                    sql,
                    e
                )
            })?;
        if rows.rows.is_empty() {
            return Ok(None);
        }
        let row_values = rows.rows[0].values.clone();
        let raw_matched = row_values.first().and_then(|value| value.as_deref());
        let matched_value = match rows.columns.first() {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_matched, column)?
            }
            None => Self::dameng_raw_text_fallback_for_compare(raw_matched),
        }
        .unwrap_or_else(|| "0".to_string());
        let matched_rows = Self::parse_dameng_length_value(matched_value.as_str()).map_err(|e| {
            format!(
                "parse Dameng target text chunk equality count failed: table={} pk={} column={} start={} value={} error={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                start,
                Self::preview_value(matched_value.as_str(), 200),
                e
            )
        })?;
        Ok(Some(matched_rows))
    }

    fn dameng_target_text_matches_source_by_chunks(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
        source_value: &str,
    ) -> Result<TextChunkCompareResult, String> {
        match Self::dameng_target_column_is_null(dameng_client, schema, table_info, pk_value, plan)?
        {
            None => return Ok(TextChunkCompareResult::Missing),
            Some(true) => {
                return Ok(TextChunkCompareResult::Different(
                    "target is NULL".to_string(),
                ));
            }
            Some(false) => {}
        }

        let length_sql = Self::dameng_random_check_text_length_select_sql(schema, table_info, plan);
        let length_value = Self::dameng_target_single_text_value(
            dameng_client,
            length_sql.as_str(),
            pk_value,
            schema,
            table_info,
            plan,
            "length",
        )?;
        let Some(length_value) = length_value else {
            return Ok(TextChunkCompareResult::Missing);
        };
        let target_len = Self::parse_dameng_length_value(length_value.as_str()).map_err(|e| {
            format!(
                "parse Dameng target text length failed before chunk equality: table={} pk={} column={} value={} error={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                Self::preview_value(length_value.as_str(), 200),
                e
            )
        })?;
        let source_len = source_value.chars().count();
        if target_len != source_len {
            return Ok(TextChunkCompareResult::Different(format!(
                "length differs source_len={} target_len={}",
                source_len, target_len
            )));
        }
        if source_len == 0 {
            return Ok(TextChunkCompareResult::Equal);
        }

        let mut start = 1usize;
        for chunk in Self::string_chunks(source_value, DAMENG_RANDOM_CHECK_TEXT_CHUNK_CHARS) {
            let len = chunk.chars().count();
            match Self::dameng_target_text_chunk_match_count(
                dameng_client,
                schema,
                table_info,
                pk_value,
                plan,
                start,
                chunk.as_str(),
            )? {
                None => return Ok(TextChunkCompareResult::Missing),
                Some(1) => {}
                Some(matched_rows) => {
                    return Ok(TextChunkCompareResult::Different(format!(
                        "chunk differs start={} len={} matched_rows={} source_chunk={}",
                        start,
                        len,
                        matched_rows,
                        Self::preview_value(chunk.as_str(), 200)
                    )));
                }
            }
            start += len;
        }
        Ok(TextChunkCompareResult::Equal)
    }

    #[cfg(test)]
    fn random_check_uses_chunked_text(kind: MysqlCompareKind) -> bool {
        matches!(
            kind,
            MysqlCompareKind::Char
                | MysqlCompareKind::Varchar
                | MysqlCompareKind::Text
                | MysqlCompareKind::Json
        )
    }

    fn dameng_target_compare_binary_value_by_chunks(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
    ) -> Result<Option<String>, String> {
        match Self::dameng_target_column_is_null(dameng_client, schema, table_info, pk_value, plan)?
        {
            None => return Ok(None),
            Some(true) => return Ok(None),
            Some(false) => {}
        }

        let length_sql =
            Self::dameng_random_check_binary_length_select_sql(schema, table_info, plan);
        let length_value = Self::dameng_target_single_text_value(
            dameng_client,
            length_sql.as_str(),
            pk_value,
            schema,
            table_info,
            plan,
            "binary length",
        )?;
        let Some(length_value) = length_value else {
            return Ok(None);
        };
        let byte_len = Self::parse_dameng_length_value(length_value.as_str()).map_err(|e| {
            format!(
                "parse Dameng target binary length failed: table={} pk={} column={} value={} error={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                Self::preview_value(length_value.as_str(), 200),
                e
            )
        })?;
        if byte_len == 0 {
            return Ok(Some(String::new()));
        }

        let mut result = String::with_capacity(byte_len * 2);
        let mut start = 1usize;
        while start <= byte_len {
            let len = DAMENG_RANDOM_CHECK_BINARY_CHUNK_BYTES.min(byte_len - start + 1);
            let chunk_sql = Self::dameng_random_check_binary_chunk_select_sql(
                schema, table_info, plan, start, len,
            );
            let chunk = Self::dameng_target_single_text_value(
                dameng_client,
                chunk_sql.as_str(),
                pk_value,
                schema,
                table_info,
                plan,
                "binary chunk",
            )?
            .ok_or_else(|| {
                format!(
                    "Dameng target binary chunk is NULL: table={} pk={} column={} start={} len={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    start,
                    len
                )
            })?;
            result.push_str(chunk.as_str());
            start += len;
        }
        Ok(Some(result.to_ascii_uppercase()))
    }

    fn dameng_target_compare_text_value_by_chunks(
        dameng_client: &mut Client,
        schema: &str,
        table_info: &TableInfoVo,
        pk_value: &Value,
        plan: &RandomCheckColumnPlan,
    ) -> Result<Option<String>, String> {
        match Self::dameng_target_column_is_null(dameng_client, schema, table_info, pk_value, plan)?
        {
            None => return Ok(None),
            Some(true) => return Ok(None),
            Some(false) => {}
        }

        let length_sql = Self::dameng_random_check_text_length_select_sql(schema, table_info, plan);
        let length_value = Self::dameng_target_single_text_value(
            dameng_client,
            length_sql.as_str(),
            pk_value,
            schema,
            table_info,
            plan,
            "length",
        )?;
        let Some(length_value) = length_value else {
            return Ok(None);
        };
        let text_len = Self::parse_dameng_length_value(length_value.as_str()).map_err(|e| {
            format!(
                "parse Dameng target text length failed: table={} pk={} column={} value={} error={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                Self::preview_value(length_value.as_str(), 200),
                e
            )
        })?;
        if text_len == 0 {
            return Ok(Some(String::new()));
        }

        let mut result = String::new();
        let mut start = 1usize;
        while start <= text_len {
            let len = DAMENG_RANDOM_CHECK_TEXT_CHUNK_CHARS.min(text_len - start + 1);
            let chunk_sql = Self::dameng_random_check_text_chunk_select_sql(
                schema, table_info, plan, start, len,
            );
            let chunk = Self::dameng_target_single_text_value(
                dameng_client,
                chunk_sql.as_str(),
                pk_value,
                schema,
                table_info,
                plan,
                "chunk",
            )?
            .ok_or_else(|| {
                format!(
                    "Dameng target text chunk is NULL: table={} pk={} column={} start={} len={}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    start,
                    len
                )
            })?;
            result.push_str(chunk.as_str());
            start += len;
        }
        Ok(Some(result))
    }

    fn dameng_target_single_text_value(
        dameng_client: &mut Client,
        sql: &str,
        pk_value: &Value,
        schema: &str,
        table_info: &TableInfoVo,
        plan: &RandomCheckColumnPlan,
        query_kind: &str,
    ) -> Result<Option<String>, String> {
        let params = vec![Self::value_to_param(pk_value)];
        let rows =
            Self::dameng_client_query_with_params(dameng_client, sql, &params).map_err(|e| {
                format!(
                    "query Dameng target text {} failed: table={} pk={} column={} sql={} error={}",
                    query_kind,
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    pk_value.resolve_string(),
                    plan.target_column,
                    sql,
                    e
                )
            })?;
        if rows.rows.is_empty() {
            return Ok(None);
        }
        if rows.rows.len() > 1 {
            warn!(
                "Dameng target text {} query returned duplicate rows, use first row for random data check: table={} pk={} column={} sql={} rows={}",
                query_kind,
                Self::target_table_key(schema, table_info.table_name.as_str()),
                pk_value.resolve_string(),
                plan.target_column,
                sql,
                rows.rows.len()
            );
        }
        let column = rows.columns.first();
        let row_values = rows.rows[0].values.clone();
        let raw_value = row_values.first().and_then(|value| value.as_deref());
        match column {
            Some(column) => {
                Self::dameng_sql_text_value_for_compare(dameng_client, raw_value, column)
            }
            None => Ok(Self::dameng_raw_text_fallback_for_compare(raw_value)),
        }
    }

    fn parse_dameng_length_value(value: &str) -> Result<usize, String> {
        let value = value.trim();
        if let Ok(len) = value.parse::<usize>() {
            return Ok(len);
        }
        let Some((whole, fraction)) = value.split_once('.') else {
            return Err("not an integer".to_string());
        };
        if !fraction.chars().all(|ch| ch == '0') {
            return Err("has non-zero fraction".to_string());
        }
        whole
            .parse::<usize>()
            .map_err(|e| format!("not an integer: {}", e))
    }

    fn dameng_client_query_with_params(
        dameng_client: &mut Client,
        sql: &str,
        params: &[DamengParam],
    ) -> Result<dameng::ResultSet, String> {
        let inline_sql = Self::inline_sql_params(sql, params)?;
        let rows = dameng_client
            .query(inline_sql.as_str())
            .map_err(|e| e.to_string())?;
        Self::dameng_fetch_all_rows(dameng_client, rows)
    }

    fn dameng_fetch_all_rows(
        dameng_client: &mut Client,
        mut rows: dameng::ResultSet,
    ) -> Result<dameng::ResultSet, String> {
        while rows.rows.len() < rows.total_row_count as usize {
            let start_row = rows.rows.len();
            dameng_client
                .fetch_more(&mut rows, start_row, 65536)
                .map_err(|e| e.to_string())?;
            if rows.rows.len() == start_row {
                return Err(format!(
                    "Dameng fetch_more returned no additional rows: fetched={} total={}",
                    rows.rows.len(),
                    rows.total_row_count
                ));
            }
        }
        Ok(rows)
    }

    fn is_dameng_string_truncation_error(error: &str) -> bool {
        error.contains("-6108") || error.contains("字符串截断")
    }

    fn parse_random_check_payload(
        payload: &str,
        expected_values: usize,
    ) -> Result<Vec<Option<String>>, String> {
        let chars = payload.chars().collect::<Vec<_>>();
        let mut values = Vec::with_capacity(expected_values);
        let mut idx = 0usize;
        while values.len() < expected_values {
            let Some(marker) = chars.get(idx).copied() else {
                return Err(format!(
                    "Dameng target payload ended early: expected={} actual={} payload_preview={}",
                    expected_values,
                    values.len(),
                    Self::preview_value(payload, 500)
                ));
            };
            idx += 1;
            match marker {
                'N' => {
                    if chars.get(idx) != Some(&';') {
                        return Err(format!(
                            "Dameng target payload NULL marker is malformed at value_index={} payload_preview={}",
                            values.len(),
                            Self::preview_value(payload, 500)
                        ));
                    }
                    idx += 1;
                    values.push(None);
                }
                'V' => {
                    let len_start = idx;
                    while matches!(chars.get(idx), Some(ch) if ch.is_ascii_digit()) {
                        idx += 1;
                    }
                    if len_start == idx || chars.get(idx) != Some(&':') {
                        return Err(format!(
                            "Dameng target payload value length is malformed at value_index={} payload_preview={}",
                            values.len(),
                            Self::preview_value(payload, 500)
                        ));
                    }
                    let len = chars[len_start..idx]
                        .iter()
                        .collect::<String>()
                        .parse::<usize>()
                        .map_err(|e| {
                            format!(
                                "Dameng target payload value length parse failed at value_index={} error={} payload_preview={}",
                                values.len(),
                                e,
                                Self::preview_value(payload, 500)
                            )
                        })?;
                    idx += 1;
                    if idx + len > chars.len() {
                        return Err(format!(
                            "Dameng target payload value ended early at value_index={} len={} payload_preview={}",
                            values.len(),
                            len,
                            Self::preview_value(payload, 500)
                        ));
                    }
                    let value = chars[idx..idx + len].iter().collect::<String>();
                    idx += len;
                    values.push(Some(value));
                }
                other => {
                    return Err(format!(
                        "Dameng target payload marker is malformed at value_index={} marker={} payload_preview={}",
                        values.len(),
                        other,
                        Self::preview_value(payload, 500)
                    ));
                }
            }
        }
        if idx != chars.len() {
            return Err(format!(
                "Dameng target payload has trailing data: expected={} parsed_chars={} total_chars={} payload_preview={}",
                expected_values,
                idx,
                chars.len(),
                Self::preview_value(payload, 500)
            ));
        }
        Ok(values)
    }

    fn dameng_sql_text_value_for_compare(
        dameng_client: &mut Client,
        raw_value: Option<&[u8]>,
        column: &DamengResultColumn,
    ) -> Result<Option<String>, String> {
        let Some(raw_value) = raw_value else {
            return Ok(None);
        };
        if raw_value.is_empty() {
            return Ok(Some(String::new()));
        }

        let Some(value_type) = Self::dameng_value_type_for_column(column) else {
            return Ok(Self::dameng_raw_text_fallback_for_compare(Some(raw_value)));
        };
        let lob_meta = matches!(value_type, DmValueType::BLOB | DmValueType::CLOB)
            .then_some((column.lob_tab_id, column.lob_col_id));
        let Some(value) = decode_value(value_type, raw_value, lob_meta) else {
            return Ok(Self::dameng_raw_text_fallback_for_compare(Some(raw_value)));
        };
        let decoded_value = Self::dameng_sql_value_to_text(dameng_client, value)?;
        if decoded_value.is_none() {
            return Ok(Self::dameng_raw_text_fallback_for_compare(Some(raw_value)));
        }
        Ok(decoded_value)
    }

    fn dameng_raw_text_fallback_for_compare(raw_value: Option<&[u8]>) -> Option<String> {
        raw_value.map(|raw_value| String::from_utf8_lossy(raw_value).to_string())
    }

    fn dameng_sql_value_to_text(
        dameng_client: &mut Client,
        value: DmValue,
    ) -> Result<Option<String>, String> {
        match value {
            DmValue::Null => Ok(None),
            DmValue::Boolean(v) => Ok(Some(if v { "1" } else { "0" }.to_string())),
            DmValue::TinyInt(v) => Ok(Some(v.to_string())),
            DmValue::SmallInt(v) => Ok(Some(v.to_string())),
            DmValue::Int(v) => Ok(Some(v.to_string())),
            DmValue::BigInt(v) => Ok(Some(v.to_string())),
            DmValue::Float(v) => Ok(Some(v.to_string())),
            DmValue::Double(v) => Ok(Some(v.to_string())),
            DmValue::Text(v) => Ok(Some(v)),
            DmValue::Bytea(v) => Ok(Some(String::from_utf8_lossy(&v).to_string())),
            DmValue::Decimal(v) => Ok(Some(v.to_string())),
            DmValue::LobLocator(locator) => {
                let data = dameng_client
                    .read_lob(&locator)
                    .map_err(|e| format!("read Dameng LOB failed: {}", e))?;
                if let Err(e) = dameng_client.free_lob(&locator) {
                    warn!(
                        "free Dameng LOB locator failed after random data check: {}",
                        e
                    );
                }
                if locator.is_clob {
                    Ok(Some(String::from_utf8_lossy(&data).to_string()))
                } else {
                    Ok(Some(Self::bytes_to_hex(&data)))
                }
            }
        }
    }

    fn dameng_value_type_for_column(column: &DamengResultColumn) -> Option<DmValueType> {
        DmValueType::from_type_code(column.type_code).or_else(|| {
            let t = column.type_name.to_ascii_uppercase();
            if t.starts_with("DECIMAL") || t.starts_with("NUMERIC") {
                Some(DmValueType::DECIMAL)
            } else if t.starts_with("BIGINT") {
                Some(DmValueType::BIGINT)
            } else if t.starts_with("INT") || t.starts_with("INTEGER") {
                Some(DmValueType::INT)
            } else if t.starts_with("SMALLINT") {
                Some(DmValueType::SMALLINT)
            } else if t.starts_with("TINYINT") {
                Some(DmValueType::TINYINT)
            } else if t.starts_with("DOUBLE") {
                Some(DmValueType::DOUBLE)
            } else if t.starts_with("FLOAT") || t.starts_with("REAL") {
                Some(DmValueType::FLOAT)
            } else if t.starts_with("DATE") && !t.starts_with("DATETIME") {
                Some(DmValueType::DATE)
            } else if t.starts_with("TIME") {
                Some(DmValueType::TIME)
            } else if t.starts_with("TIMESTAMP") || t.starts_with("DATETIME") {
                Some(DmValueType::TIMESTAMP)
            } else if t.starts_with("BLOB")
                || t.starts_with("BINARY")
                || t.starts_with("VARBINARY")
                || t.starts_with("RAW")
            {
                Some(DmValueType::BLOB)
            } else if t.starts_with("CLOB") {
                Some(DmValueType::CLOB)
            } else if t.starts_with("BIT") || t.starts_with("BOOLEAN") {
                Some(DmValueType::BIT)
            } else if t.starts_with("CHAR") || t.starts_with("VARCHAR") || t.starts_with("VARCHAR2")
            {
                Some(DmValueType::VARCHAR)
            } else {
                None
            }
        })
    }

    fn normalize_optional_compare_value(
        kind: MysqlCompareKind,
        value: Option<String>,
    ) -> Option<String> {
        value.map(|value| Self::normalize_compare_string(kind, value.as_str()))
    }

    fn normalize_compare_string(kind: MysqlCompareKind, value: &str) -> String {
        match kind {
            MysqlCompareKind::Date => value.split_whitespace().next().unwrap_or(value).to_string(),
            MysqlCompareKind::Time | MysqlCompareKind::DateTime => {
                Self::trim_fractional_second_zeros(value)
            }
            MysqlCompareKind::Char => value.trim_end().to_string(),
            MysqlCompareKind::Varchar => value.to_string(),
            MysqlCompareKind::Json => serde_json::from_str::<serde_json::Value>(value)
                .map(|json| json.to_string())
                .unwrap_or_else(|_| value.to_string()),
            MysqlCompareKind::Binary => value.to_ascii_uppercase(),
            _ => value.to_string(),
        }
    }

    fn trim_fractional_second_zeros(value: &str) -> String {
        let Some(dot_pos) = value.rfind('.') else {
            return value.to_string();
        };
        let (head, fraction_with_dot) = value.split_at(dot_pos);
        let fraction = fraction_with_dot.trim_start_matches('.');
        let fraction = fraction.trim_end_matches('0');
        if fraction.is_empty() {
            head.to_string()
        } else {
            format!("{}.{}", head, fraction)
        }
    }

    fn compare_mismatch_reason(source: &Option<String>, target: &Option<String>) -> &'static str {
        match (source, target) {
            (None, Some(_)) => "source is NULL but target is not NULL",
            (Some(_), None) => "target is NULL but source is not NULL",
            (Some(_), Some(_)) => "value differs",
            (None, None) => "unknown mismatch",
        }
    }

    fn compare_values_equal(
        kind: MysqlCompareKind,
        source: &Option<String>,
        target: &Option<String>,
    ) -> bool {
        if source == target {
            return true;
        }
        let (Some(source), Some(target)) = (source.as_deref(), target.as_deref()) else {
            return false;
        };
        match kind {
            MysqlCompareKind::Integer | MysqlCompareKind::Year => {
                Self::decimal_compare_values_equal(source, target)
            }
            MysqlCompareKind::Decimal => Self::decimal_compare_values_equal(source, target),
            MysqlCompareKind::Float => Self::float_compare_values_equal(source, target),
            _ => false,
        }
    }

    fn decimal_compare_values_equal(source: &str, target: &str) -> bool {
        let Ok(source_decimal) = BigDecimal::from_str(source.trim()) else {
            return Self::float_compare_values_equal(source, target);
        };
        let Ok(target_decimal) = BigDecimal::from_str(target.trim()) else {
            return Self::float_compare_values_equal(source, target);
        };
        source_decimal.normalized().to_string() == target_decimal.normalized().to_string()
    }

    fn float_compare_values_equal(source: &str, target: &str) -> bool {
        let Ok(source_value) = source.trim().parse::<f64>() else {
            return false;
        };
        let Ok(target_value) = target.trim().parse::<f64>() else {
            return false;
        };
        if !source_value.is_finite() || !target_value.is_finite() {
            return source_value == target_value;
        }
        let diff = (source_value - target_value).abs();
        let tolerance = source_value.abs().max(target_value.abs()).max(1.0) * 1e-6;
        diff <= tolerance
    }

    fn describe_compare_row(plans: &[RandomCheckColumnPlan], values: &[Option<String>]) -> String {
        plans
            .iter()
            .enumerate()
            .map(|(idx, plan)| {
                let value = values.get(idx).cloned().unwrap_or(None);
                format!(
                    "{}={}",
                    plan.source_column,
                    Self::describe_compare_value(&value)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn describe_compare_value(value: &Option<String>) -> String {
        match value {
            None => "NULL".to_string(),
            Some(value) => {
                let preview = Self::preview_value(value, 500);
                format!("len={} value={}", value.chars().count(), preview)
            }
        }
    }

    fn preview_value(value: &str, max_chars: usize) -> String {
        let mut preview = value.chars().take(max_chars).collect::<String>();
        if value.chars().count() > max_chars {
            preview.push_str("...");
        }
        format!("{:?}", preview)
    }

    fn table_info_source_table_key(table_info: &TableInfoVo) -> String {
        database_table_key(
            table_info.source_database.as_str(),
            table_info.table_name.as_str(),
        )
    }

    async fn ensure_schema(&self) -> Result<(), String> {
        if self.random_check_data_after_init {
            info!(
                "Dameng random data check init mode skips schema ensure: relies on existing target tables and DML auto-add columns"
            );
            return Ok(());
        }
        for table_info in &self.table_info_list {
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            let table_created = self
                .ensure_table(schema.as_str(), table_info)
                .await
                .map_err(|e| {
                    format!(
                        "Dameng ensure table failed: {} {}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        e
                    )
                })?;
            self.ensure_columns(schema.as_str(), table_info)
                .await
                .map_err(|e| {
                    format!(
                        "Dameng ensure columns failed: {} {}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        e
                    )
                })?;
            if table_created {
                self.ensure_table_comment(schema.as_str(), table_info)
                    .await
                    .map_err(|e| {
                        format!(
                            "Dameng ensure table comment failed: {} {}",
                            Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                            e
                        )
                    })?;
            }
            self.ensure_column_comments(schema.as_str(), table_info)
                .await
                .map_err(|e| {
                    format!(
                        "Dameng ensure column comments failed: {} {}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        e
                    )
                })?;
        }
        Ok(())
    }

    async fn ensure_database(&self) -> Result<(), String> {
        for schema in self.target_schemas() {
            self.ensure_database_schema(schema.as_str()).await?;
        }
        Ok(())
    }

    async fn ensure_table_comment(
        &self,
        schema: &str,
        table_info: &TableInfoVo,
    ) -> Result<(), String> {
        let expected_comment = table_info.table_comment.trim();
        if expected_comment.is_empty() {
            return Ok(());
        }
        let existing_comment = self
            .existing_table_comment(schema, table_info.table_name.as_str())
            .await?;
        if existing_comment.as_deref() == Some(expected_comment) {
            return Ok(());
        }
        let sql =
            Self::comment_on_table_sql(schema, table_info.table_name.as_str(), expected_comment);
        match self.execute(sql.as_str()).await {
            Ok(_) => info!(
                "Dameng auto comment table success: {}",
                Self::target_table_key(schema, table_info.table_name.as_str())
            ),
            Err(e) => {
                let msg = format!(
                    "Dameng auto comment table failed: {} {}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    e
                );
                error!("{}", msg);
                return Err(msg);
            }
        }
        Ok(())
    }

    async fn ensure_column_comments(
        &self,
        schema: &str,
        table_info: &TableInfoVo,
    ) -> Result<(), String> {
        let expected_comments = Self::mysql_column_comments(table_info);
        if expected_comments.is_empty() {
            return Ok(());
        }

        let existing_comments = self
            .existing_column_comments(schema, table_info.table_name.as_str())
            .await?;
        if existing_comments.is_empty() {
            warn!(
                "Dameng target table comment metadata is empty, skip comment check: {}",
                Self::target_table_key(schema, table_info.table_name.as_str())
            );
            return Ok(());
        }

        for src_col in &table_info.columns {
            let target_col = Self::target_column_name(src_col);
            let target_key = target_col.to_ascii_lowercase();
            let Some(expected_comment) = expected_comments.get(target_key.as_str()) else {
                continue;
            };
            match Self::column_comment_needs_update(
                &existing_comments,
                target_key.as_str(),
                expected_comment.as_str(),
            ) {
                None => {
                    warn!(
                        "Dameng skip comment for missing target column: {} {}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col
                    );
                    continue;
                }
                Some(false) => continue,
                Some(true) => {}
            }

            let sql = Self::comment_on_column_sql(
                schema,
                table_info.table_name.as_str(),
                target_col.as_str(),
                expected_comment.as_str(),
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto comment column success: {} {}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    src_col
                ),
                Err(e) => {
                    let msg = format!(
                        "Dameng auto comment column failed: {} {} {}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
        }
        Ok(())
    }

    fn column_comment_needs_update(
        existing_comments: &HashMap<String, String>,
        target_key: &str,
        expected_comment: &str,
    ) -> Option<bool> {
        existing_comments
            .get(target_key)
            .map(|existing_comment| existing_comment != expected_comment)
    }

    async fn ensure_database_schema(&self, schema: &str) -> Result<(), String> {
        if schema.is_empty() {
            return Ok(());
        }

        if self.schema_exists(schema).await? {
            return Ok(());
        }
        if !self.auto_create_database {
            return Err(format!(
                "target schema {} is unavailable and auto_create_database is false",
                schema
            ));
        }

        let create_schema_sql = Self::create_schema_sql(schema);
        match self.execute(create_schema_sql.as_str()).await {
            Ok(_) => {}
            Err(create_schema_error) => {
                if self.schema_exists(schema).await.unwrap_or(false) {
                    return Ok(());
                }
                return Err(format!(
                    "create schema failed: {} {}",
                    schema, create_schema_error
                ));
            }
        }
        if !self.schema_exists(schema).await? {
            return Err(format!(
                "create schema succeeded but schema is not visible: {}",
                schema
            ));
        }
        info!("Dameng auto create schema success: {}", schema);
        Ok(())
    }

    async fn sync_stored_routines(&self, config: &CdcConfig) -> Result<(), String> {
        let overwrite = config.overwrite_stored_procedure_enabled();
        let source_databases = config.source_databases();
        for (source_index, source_database) in source_databases.iter().enumerate() {
            if source_database.trim().is_empty() {
                continue;
            }
            let source_config_index = if config.multi_mode_open() {
                0
            } else {
                source_index
            };
            let mut target_schema = config.target_database_for_source(source_database);
            if target_schema.trim().is_empty() {
                target_schema = self.schema.clone();
            }
            let source_url = mysql_connection_url_from_config(
                &config.source_config[source_config_index],
                Some(source_database),
            );
            let source_pool =
                get_mysql_pool_by_url(&source_url, "dameng sink 同步存储程序-读取源库存储程序")
                    .await?;
            let routines = fetch_mysql_routines(
                &source_pool,
                source_database,
                &[MySqlRoutineKind::Function, MySqlRoutineKind::Procedure],
            )
            .await
            .map_err(|e| {
                format!(
                    "fetch source stored routines failed: {} -> {} {}",
                    source_database, target_schema, e
                )
            })?;
            if routines.is_empty() {
                info!("MySQL source stored routine not found: {}", source_database);
                continue;
            }

            for routine in routines {
                self.sync_one_stored_routine(
                    source_database,
                    target_schema.as_str(),
                    &routine,
                    overwrite,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn sync_stored_views(&self, config: &CdcConfig) -> Result<(), String> {
        let source_databases = config.source_databases();
        let schema_routes = Self::view_schema_routes(config);
        for (source_index, source_database) in source_databases.iter().enumerate() {
            if source_database.trim().is_empty() {
                continue;
            }
            let source_config_index = if config.multi_mode_open() {
                0
            } else {
                source_index
            };
            let mut target_schema = config.target_database_for_source(source_database);
            if target_schema.trim().is_empty() {
                target_schema = self.schema.clone();
            }
            let source_url = mysql_connection_url_from_config(
                &config.source_config[source_config_index],
                Some(source_database),
            );
            let source_pool =
                get_mysql_pool_by_url(&source_url, "dameng sink 同步视图-读取源库视图").await?;
            let views = fetch_mysql_views(&source_pool, source_database, config)
                .await
                .map_err(|e| {
                    format!(
                        "fetch source views failed: {} -> {} {}",
                        source_database, target_schema, e
                    )
                })?;
            if views.is_empty() {
                info!("MySQL source view not found: {}", source_database);
                continue;
            }
            self.sync_views_with_retry(
                source_database,
                target_schema.as_str(),
                &schema_routes,
                views,
            )
            .await?;
        }
        Ok(())
    }

    fn view_schema_routes(config: &CdcConfig) -> Vec<(String, String)> {
        let mut routes: Vec<(String, String)> = config.multi_mode_route_map().into_iter().collect();
        let mut extra_routes = Vec::new();
        for (source, target) in &routes {
            if let Some(base_source) = strip_trailing_numeric_schema_suffix(source)
                && !routes
                    .iter()
                    .chain(extra_routes.iter())
                    .any(|(existing, _)| existing.eq_ignore_ascii_case(base_source.as_str()))
            {
                extra_routes.push((base_source, target.clone()));
            }
        }
        routes.extend(extra_routes);
        routes
    }

    async fn sync_views_with_retry(
        &self,
        source_database: &str,
        target_schema: &str,
        schema_routes: &[(String, String)],
        mut pending: Vec<MySqlViewDefinition>,
    ) -> Result<(), String> {
        while !pending.is_empty() {
            let mut next_pending = Vec::new();
            let mut errors = Vec::new();
            let mut progress = false;
            for view in pending {
                match self
                    .sync_one_stored_view(source_database, target_schema, schema_routes, &view)
                    .await
                {
                    Ok(created) => {
                        if created {
                            progress = true;
                        }
                    }
                    Err(e) => {
                        errors.push(e);
                        next_pending.push(view);
                    }
                }
            }
            if next_pending.is_empty() {
                return Ok(());
            }
            if !progress {
                return Err(format!(
                    "create Dameng views failed after dependency retry:\n{}",
                    errors.join("\n")
                ));
            }
            pending = next_pending;
        }
        Ok(())
    }

    async fn sync_one_stored_view(
        &self,
        source_database: &str,
        target_schema: &str,
        schema_routes: &[(String, String)],
        view: &MySqlViewDefinition,
    ) -> Result<bool, String> {
        if self
            .dameng_view_exists(target_schema, view.name.as_str())
            .await?
        {
            info!(
                "Dameng view exists, skip: {}.{} -> {}.{}",
                source_database, view.name, target_schema, view.name
            );
            return Ok(false);
        }
        if self
            .dameng_object_name_exists(target_schema, view.name.as_str())
            .await?
        {
            return Err(format!(
                "Dameng object name exists and is not a view: {}.{} source={}.{}",
                target_schema, view.name, source_database, view.name
            ));
        }

        let create_sql = match convert_mysql_view_to_dameng_with_name_and_schema_routes(
            target_schema,
            source_database,
            Some(view.name.as_str()),
            view,
            schema_routes,
        ) {
            Ok(sql) => sql,
            Err(e) if Self::is_unsupported_mysql_syntax_error(e.as_str()) => {
                warn!(
                    "Dameng skip unsupported MySQL view: {}.{} -> {}.{} {}",
                    source_database, view.name, target_schema, view.name, e
                );
                return Ok(false);
            }
            Err(e) => {
                return Err(format!(
                    "convert view failed: {}.{} -> {}.{} {}",
                    source_database, view.name, target_schema, view.name, e
                ));
            }
        };
        self.execute(create_sql.as_str()).await.map_err(|e| {
            format!(
                "create Dameng view failed: {}.{} -> {}.{} sql={} error={}",
                source_database,
                view.name,
                target_schema,
                view.name,
                Self::sql_preview(create_sql.as_str()),
                e
            )
        })?;
        info!(
            "Dameng sync view success: {}.{} -> {}.{}",
            source_database, view.name, target_schema, view.name
        );
        Ok(true)
    }

    async fn sync_one_stored_routine(
        &self,
        source_database: &str,
        target_schema: &str,
        routine: &MySqlRoutineDefinition,
        overwrite: bool,
    ) -> Result<(), String> {
        let mut target_routine_name = routine.name.clone();
        let exists = self
            .dameng_routine_exists(target_schema, routine.kind, routine.name.as_str())
            .await?;
        if exists && !overwrite {
            info!(
                "Dameng stored routine exists, skip: {} {}.{} -> {}.{}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                routine.name
            );
            return Ok(());
        }
        if !exists
            && self
                .dameng_object_name_exists(target_schema, routine.name.as_str())
                .await?
        {
            let (renamed_routine_name, renamed_exists) = self
                .dameng_conflict_routine_name(target_schema, routine.kind, routine.name.as_str())
                .await?;
            if renamed_exists && !overwrite {
                info!(
                    "Dameng renamed stored routine exists, skip: {} {}.{} -> {}.{} original_target={}.{}",
                    routine.kind.routine_type(),
                    source_database,
                    routine.name,
                    target_schema,
                    renamed_routine_name,
                    target_schema,
                    routine.name
                );
                return Ok(());
            }
            warn!(
                "Dameng object name exists, rename stored routine: {} {}.{} -> {}.{} original_target={}.{}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                renamed_routine_name,
                target_schema,
                routine.name
            );
            target_routine_name = renamed_routine_name;
        }

        let create_sql = match convert_mysql_routine_to_dameng_with_name(
            target_schema,
            Some(target_routine_name.as_str()),
            routine,
        ) {
            Ok(sql) => sql,
            Err(e) if Self::is_unsupported_mysql_syntax_error(e.as_str()) => {
                warn!(
                    "Dameng skip unsupported MySQL stored routine: {} {}.{} -> {}.{} {}",
                    routine.kind.routine_type(),
                    source_database,
                    routine.name,
                    target_schema,
                    target_routine_name,
                    e
                );
                return Ok(());
            }
            Err(e) => {
                return Err(format!(
                    "convert stored routine failed: {} {}.{} -> {}.{} {}",
                    routine.kind.routine_type(),
                    source_database,
                    routine.name,
                    target_schema,
                    target_routine_name,
                    e
                ));
            }
        };
        self.execute(create_sql.as_str()).await.map_err(|e| {
            format!(
                "create Dameng stored routine failed: {} {}.{} -> {}.{} overwrite={} sql={} error={}",
                routine.kind.routine_type(),
                source_database,
                routine.name,
                target_schema,
                target_routine_name,
                overwrite,
                Self::sql_preview(create_sql.as_str()),
                e
            )
        })?;
        info!(
            "Dameng sync stored routine success: {} {}.{} -> {}.{} overwrite={}",
            routine.kind.routine_type(),
            source_database,
            routine.name,
            target_schema,
            target_routine_name,
            overwrite
        );
        Ok(())
    }

    async fn dameng_conflict_routine_name(
        &self,
        schema: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> Result<(String, bool), String> {
        for index in 0..100 {
            let candidate = Self::conflict_routine_name(routine_name, routine_kind, index);
            if self
                .dameng_routine_exists(schema, routine_kind, candidate.as_str())
                .await?
            {
                return Ok((candidate, true));
            }
            if !self
                .dameng_object_name_exists(schema, candidate.as_str())
                .await?
            {
                return Ok((candidate, false));
            }
        }
        Err(format!(
            "no available Dameng routine name for object conflict: {}.{} {}",
            schema,
            routine_name,
            routine_kind.routine_type()
        ))
    }

    fn conflict_routine_name(
        routine_name: &str,
        routine_kind: MySqlRoutineKind,
        index: usize,
    ) -> String {
        let suffix = match routine_kind {
            MySqlRoutineKind::Procedure => "_procedure",
            MySqlRoutineKind::Function => "_function",
        };
        let suffix = if index == 0 {
            suffix.to_string()
        } else {
            format!("{}_{}", suffix, index + 1)
        };
        Self::identifier_with_suffix(routine_name, suffix.as_str())
    }

    fn identifier_with_suffix(identifier: &str, suffix: &str) -> String {
        let suffix_len = suffix.chars().count();
        let prefix_len = DAMENG_IDENTIFIER_CHAR_LIMIT.saturating_sub(suffix_len);
        let prefix = identifier.chars().take(prefix_len).collect::<String>();
        format!("{}{}", prefix, suffix)
    }

    async fn dameng_routine_exists(
        &self,
        schema: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> Result<bool, String> {
        let sql = Self::dameng_routine_exists_sql(schema, routine_kind, routine_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn dameng_view_exists(&self, schema: &str, view_name: &str) -> Result<bool, String> {
        let sql = Self::dameng_view_exists_sql(schema, view_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn dameng_object_name_exists(
        &self,
        schema: &str,
        object_name: &str,
    ) -> Result<bool, String> {
        let sql = Self::dameng_object_name_exists_sql(schema, object_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    fn dameng_routine_exists_sql(
        schema: &str,
        routine_kind: MySqlRoutineKind,
        routine_name: &str,
    ) -> String {
        let object_type = routine_kind.routine_type();
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = {} AND {}",
                Self::quote_literal(object_type),
                Self::eq_original_or_upper_sql("OBJECT_NAME", routine_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_OBJECTS WHERE {} AND OBJECT_TYPE = {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::quote_literal(object_type),
                Self::eq_original_or_upper_sql("OBJECT_NAME", routine_name)
            )
        }
    }

    fn dameng_view_exists_sql(schema: &str, view_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'VIEW' AND {}",
                Self::eq_original_or_upper_sql("OBJECT_NAME", view_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_OBJECTS WHERE {} AND OBJECT_TYPE = 'VIEW' AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("OBJECT_NAME", view_name)
            )
        }
    }

    fn dameng_object_name_exists_sql(schema: &str, object_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE {}",
                Self::eq_original_or_upper_sql("OBJECT_NAME", object_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_OBJECTS WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("OBJECT_NAME", object_name)
            )
        }
    }

    fn sql_preview(sql: &str) -> String {
        let mut compact = sql
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if compact.len() > 500 {
            compact = compact.chars().take(500).collect();
            compact.push_str("...");
        }
        compact
    }

    fn is_unsupported_mysql_syntax_error(error: &str) -> bool {
        error.contains("unsupported MySQL syntax")
    }

    async fn ensure_table(&self, schema: &str, table_info: &TableInfoVo) -> Result<bool, String> {
        if !self.auto_create_table {
            return Ok(false);
        }
        if self
            .table_exists(schema, table_info.table_name.as_str())
            .await?
        {
            return Ok(false);
        }

        let sql = Self::create_table_sql(schema, table_info)?;
        self.execute(sql.as_str()).await?;
        info!(
            "Dameng auto create table success: {}",
            Self::target_table_key(schema, table_info.table_name.as_str())
        );
        Ok(true)
    }

    fn create_table_sql(schema: &str, table_info: &TableInfoVo) -> Result<String, String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let primary_key_columns = Self::source_primary_key_columns(table_info);
        let mut cols_sql: Vec<String> = Vec::with_capacity(table_info.columns.len());
        for src_col in &table_info.columns {
            let key = src_col.to_ascii_lowercase();
            let def = match defs.get(&key) {
                None => continue,
                Some(v) => v,
            };
            let mysql_type = match mysql_type_token_from_column_definition(def.as_str()) {
                None => continue,
                Some(v) => v,
            };
            let mut nullable = mysql_column_allows_null_from_definition(def.as_str());
            if Self::contains_source_column(&primary_key_columns, src_col) {
                nullable = false;
            }
            let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
            let auto_increment = mysql_column_is_auto_increment_from_definition(def.as_str());
            cols_sql.push(format!(
                "{} {}",
                Self::quote_column_ident(src_col),
                Self::dameng_column_definition(
                    mysql_type.as_str(),
                    def.as_str(),
                    nullable_sql,
                    auto_increment
                ),
            ));
        }
        if cols_sql.is_empty() {
            return Err("no columns parsed from source create table sql".to_string());
        }
        if !primary_key_columns.is_empty() {
            let primary_key_sql = primary_key_columns
                .iter()
                .map(|column| Self::quote_column_ident(column))
                .collect::<Vec<_>>()
                .join(", ");
            cols_sql.push(format!("PRIMARY KEY ({})", primary_key_sql));
        }
        let sql = format!(
            "CREATE TABLE {} ({})",
            Self::qualified_table(schema, table_info.table_name.as_str()),
            cols_sql.join(", ")
        );
        Ok(sql)
    }

    fn source_primary_key_columns(table_info: &TableInfoVo) -> Vec<String> {
        mysql_primary_key_columns_from_create_table_sql(table_info.create_table_sql.as_str())
    }

    fn contains_source_column(columns: &[String], column_name: &str) -> bool {
        columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case(column_name))
    }

    fn merge_column_lists(base_columns: &[String], extra_columns: &[String]) -> Vec<String> {
        let mut result = Vec::with_capacity(base_columns.len() + extra_columns.len());
        let mut seen = HashSet::new();
        for column in base_columns {
            Self::push_unique_source_column(&mut result, &mut seen, column);
        }
        for column in extra_columns {
            Self::push_unique_source_column(&mut result, &mut seen, column);
        }
        result
    }

    fn merge_columns_with_records(base_columns: &[String], records: &[DataBuffer]) -> Vec<String> {
        let mut result = Vec::with_capacity(base_columns.len());
        let mut seen = HashSet::new();
        for column in base_columns {
            Self::push_unique_source_column(&mut result, &mut seen, column);
        }

        let mut observed_columns = records
            .iter()
            .flat_map(|record| record.after.raw_keys().cloned())
            .filter(|column| !column.trim().is_empty())
            .collect::<Vec<_>>();
        observed_columns.sort_by_key(|column| column.to_ascii_lowercase());
        observed_columns.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
        for column in &observed_columns {
            Self::push_unique_source_column(&mut result, &mut seen, column);
        }
        result
    }

    fn merge_dml_columns_with_records(
        table_info: &TableInfoVo,
        base_columns: &[String],
        records: &[DataBuffer],
    ) -> Vec<String> {
        let generated_columns = Self::generated_source_column_keys(table_info);
        Self::merge_columns_with_records(base_columns, records)
            .into_iter()
            .filter(|column| !generated_columns.contains(column.to_ascii_lowercase().as_str()))
            .collect()
    }

    fn generated_source_column_keys(table_info: &TableInfoVo) -> HashSet<String> {
        extract_mysql_generated_column_names(table_info.create_table_sql.as_str())
            .into_iter()
            .map(|column| column.to_ascii_lowercase())
            .collect()
    }

    fn push_unique_source_column(
        result: &mut Vec<String>,
        seen_target_keys: &mut HashSet<String>,
        source_column: &str,
    ) {
        if source_column.trim().is_empty() {
            return;
        }
        if seen_target_keys.insert(Self::target_column_key(source_column)) {
            result.push(source_column.to_string());
        }
    }

    fn target_column_key(source_column: &str) -> String {
        Self::target_column_name(source_column).to_ascii_lowercase()
    }

    fn is_protected_source_column_for_auto_modify(
        primary_key_columns: &[String],
        column_name: &str,
        column_definition: &str,
    ) -> bool {
        Self::contains_source_column(primary_key_columns, column_name)
            || mysql_column_is_auto_increment_from_definition(column_definition)
    }

    fn should_relax_not_null_for_random_check(
        random_check_data_after_init: bool,
        mysql_nullable: bool,
        existing_col: &DamengColumnInfo,
        protected: bool,
        sampled_source_null: bool,
    ) -> bool {
        random_check_data_after_init
            && mysql_nullable
            && sampled_source_null
            && !existing_col.nullable
            && !protected
    }

    fn should_recreate_column_for_random_check(
        random_check_data_after_init: bool,
        mysql_type: &str,
        existing_col: &DamengColumnInfo,
    ) -> bool {
        random_check_data_after_init
            && Self::map_mysql_type_to_dameng(mysql_type).eq_ignore_ascii_case("BLOB")
            && !existing_col.data_type.eq_ignore_ascii_case("BLOB")
    }

    async fn recreate_column_for_random_check(
        &self,
        schema: &str,
        table_name: &str,
        source_column: &str,
        target_column: &str,
        add_column_definition: &str,
    ) -> Result<(), String> {
        let table_sql = Self::qualified_table(schema, table_name);
        let drop_sql = format!(
            "ALTER TABLE {} DROP COLUMN {}",
            table_sql,
            Self::quote_ident(target_column)
        );
        self.execute(drop_sql.as_str()).await.map_err(|e| {
            format!(
                "Dameng random data check recreate column drop failed: {}.{} {} sql={} error={}",
                Self::target_table_key(schema, table_name),
                source_column,
                target_column,
                drop_sql,
                e
            )
        })?;
        let add_sql = format!(
            "ALTER TABLE {} ADD {} {}",
            table_sql,
            Self::quote_ident(target_column),
            add_column_definition
        );
        self.execute(add_sql.as_str()).await.map_err(|e| {
            format!(
                "Dameng random data check recreate column add failed: {}.{} {} sql={} error={}",
                Self::target_table_key(schema, table_name),
                source_column,
                target_column,
                add_sql,
                e
            )
        })?;
        info!(
            "Dameng random data check recreated target column: {}.{} target_column={} definition={}",
            Self::target_table_key(schema, table_name),
            source_column,
            target_column,
            add_column_definition
        );
        Ok(())
    }

    async fn ensure_columns(&self, schema: &str, table_info: &TableInfoVo) -> Result<(), String> {
        if !self.auto_add_column && !self.auto_modify_column {
            return Ok(());
        }
        let existing_cols = self
            .existing_columns(schema, table_info.table_name.as_str())
            .await?;
        if existing_cols.is_empty() {
            warn!(
                "Dameng target table metadata is empty, skip column check: {} auto_create_table={} auto_add_column={} auto_modify_column={}",
                Self::target_table_key(schema, table_info.table_name.as_str()),
                self.auto_create_table,
                self.auto_add_column,
                self.auto_modify_column
            );
            return Ok(());
        }
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let primary_key_columns = Self::source_primary_key_columns(table_info);
        for src_col in &table_info.columns {
            let key = src_col.to_ascii_lowercase();
            let target_col = Self::target_column_name(src_col);
            let target_key = target_col.to_ascii_lowercase();
            let def = match defs.get(&key) {
                None => continue,
                Some(v) => v,
            };
            let mysql_type = match mysql_type_token_from_column_definition(def.as_str()) {
                None => continue,
                Some(v) => v,
            };
            let nullable = mysql_column_allows_null_from_definition(def.as_str());
            let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
            let dameng_type = Self::map_mysql_type_to_dameng(mysql_type.as_str());
            let add_column_definition = Self::dameng_column_definition(
                mysql_type.as_str(),
                def.as_str(),
                nullable_sql,
                false,
            );
            if let Some(existing_col) = existing_cols.get(target_key.as_str()) {
                let should_modify =
                    Self::should_modify_existing_column(&mysql_type, def.as_str(), existing_col);
                let protected = Self::is_protected_source_column_for_auto_modify(
                    &primary_key_columns,
                    src_col,
                    def.as_str(),
                );
                let should_relax_nullable = Self::should_relax_not_null_for_random_check(
                    self.random_check_data_after_init,
                    nullable,
                    existing_col,
                    protected,
                    false,
                );
                let should_recreate_for_random_check =
                    Self::should_recreate_column_for_random_check(
                        self.random_check_data_after_init,
                        mysql_type.as_str(),
                        existing_col,
                    );
                let default_mismatch =
                    Self::existing_column_default_mismatch(&mysql_type, def.as_str(), existing_col);
                if dameng_type.eq_ignore_ascii_case("CLOB")
                    || should_modify
                    || default_mismatch
                    || should_relax_nullable
                {
                    info!(
                        "Dameng column check: {}.{} existing={} data_length={} char_length={} nullable={} default={:?} expected={} mysql_nullable={} auto_modify={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.nullable,
                        existing_col.default_value,
                        dameng_type,
                        nullable,
                        self.auto_modify_column
                    );
                }
                if default_mismatch {
                    warn!(
                        "Dameng skip auto modify default mismatch: {}.{} existing_default={:?} expected_default={:?}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.default_value,
                        Self::mysql_column_default_value_sql(mysql_type.as_str(), def.as_str())
                    );
                }
                if should_modify && !self.auto_modify_column {
                    return Err(format!(
                        "Dameng column type mismatch and auto_modify_column is false: {}.{} existing={} data_length={} char_length={} default={:?} expected={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type
                    ));
                }
                if should_modify && protected {
                    warn!(
                        "Dameng skip auto modify protected column: {}.{} existing={} data_length={} char_length={} default={:?} expected={}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type
                    );
                    continue;
                }
                if should_modify || should_relax_nullable {
                    if should_recreate_for_random_check {
                        self.recreate_column_for_random_check(
                            schema,
                            table_info.table_name.as_str(),
                            src_col,
                            target_col.as_str(),
                            add_column_definition.as_str(),
                        )
                        .await?;
                        continue;
                    }
                    let nullable_sql = if should_relax_nullable { "NULL" } else { "" };
                    let sql = format!(
                        "ALTER TABLE {} MODIFY {} {}",
                        Self::qualified_table(schema, table_info.table_name.as_str()),
                        Self::quote_ident(target_col.as_str()),
                        Self::dameng_modify_column_definition_with_nullable(
                            mysql_type.as_str(),
                            nullable_sql
                        )
                    );
                    match self.execute(sql.as_str()).await {
                        Ok(_) => info!(
                            "Dameng auto modify column success: {} {} nullable_sql={}",
                            Self::target_table_key(schema, table_info.table_name.as_str()),
                            src_col,
                            nullable_sql
                        ),
                        Err(e) => {
                            let msg = format!(
                                "Dameng auto modify column failed: {} {} {}",
                                Self::target_table_key(schema, table_info.table_name.as_str()),
                                src_col,
                                e
                            );
                            error!("{}", msg);
                            return Err(msg);
                        }
                    }
                }
                continue;
            }
            if !self.auto_add_column {
                continue;
            }
            let sql = format!(
                "ALTER TABLE {} ADD {} {}",
                Self::qualified_table(schema, table_info.table_name.as_str()),
                Self::quote_ident(target_col.as_str()),
                add_column_definition
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto add column success: {} {}",
                    Self::target_table_key(schema, table_info.table_name.as_str()),
                    src_col
                ),
                Err(e) => {
                    let msg = format!(
                        "Dameng auto add column failed: {} {} {}",
                        Self::target_table_key(schema, table_info.table_name.as_str()),
                        src_col,
                        e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
        }
        Ok(())
    }

    async fn ensure_dml_columns(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        columns: &[String],
        records: &[DataBuffer],
        include_required_defaults: bool,
    ) -> Result<Vec<(String, Value)>, String> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }
        let requested_keys = columns
            .iter()
            .map(|column| Self::target_column_key(column))
            .collect::<HashSet<_>>();
        let checked_key = table_key.to_ascii_lowercase();
        {
            let checked_columns = self.dml_checked_column_keys.lock().await;
            if checked_columns
                .get(checked_key.as_str())
                .is_some_and(|checked| requested_keys.is_subset(checked))
            {
                if include_required_defaults {
                    return Ok(self
                        .dml_required_column_defaults
                        .lock()
                        .await
                        .get(checked_key.as_str())
                        .cloned()
                        .unwrap_or_default());
                }
                return Ok(Vec::new());
            }
        }

        let table_info = self.table_info_cache.lock().await.get(table_key);
        let existing_cols = self.existing_columns(schema, table_name).await?;
        if existing_cols.is_empty() {
            return Err(format!(
                "Dameng target table metadata is empty before DML column check: {}",
                table_key
            ));
        }

        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let primary_key_columns = Self::source_primary_key_columns(&table_info);
        let mut missing_columns = Vec::new();
        for src_col in columns {
            let target_col = Self::target_column_name(src_col);
            let target_key = target_col.to_ascii_lowercase();
            if let Some(existing_col) = existing_cols.get(target_key.as_str()) {
                let Some(def) = defs.get(src_col.to_ascii_lowercase().as_str()) else {
                    continue;
                };
                let Some(mysql_type) = mysql_type_token_from_column_definition(def.as_str()) else {
                    continue;
                };
                let nullable = mysql_column_allows_null_from_definition(def.as_str());
                let dameng_type = Self::map_mysql_type_to_dameng(mysql_type.as_str());
                let should_modify =
                    Self::should_modify_existing_column(&mysql_type, def.as_str(), existing_col);
                let protected = Self::is_protected_source_column_for_auto_modify(
                    &primary_key_columns,
                    src_col,
                    def.as_str(),
                );
                let should_relax_nullable = Self::should_relax_not_null_for_random_check(
                    self.random_check_data_after_init,
                    nullable,
                    existing_col,
                    protected,
                    records.iter().any(|record| {
                        record.after.contains_key(src_col) && record.after.get(src_col).is_none()
                    }),
                );
                let should_recreate_for_random_check =
                    Self::should_recreate_column_for_random_check(
                        self.random_check_data_after_init,
                        mysql_type.as_str(),
                        existing_col,
                    );
                let default_mismatch =
                    Self::existing_column_default_mismatch(&mysql_type, def.as_str(), existing_col);
                if dameng_type.eq_ignore_ascii_case("CLOB")
                    || should_modify
                    || default_mismatch
                    || should_relax_nullable
                {
                    info!(
                        "Dameng DML column check: {}.{} existing={} data_length={} char_length={} nullable={} default={:?} expected={} mysql_nullable={} auto_modify={}",
                        table_key,
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.nullable,
                        existing_col.default_value,
                        dameng_type,
                        nullable,
                        self.auto_modify_column
                    );
                }
                if default_mismatch {
                    warn!(
                        "Dameng DML skip auto modify default mismatch: {}.{} existing_default={:?} expected_default={:?}",
                        table_key,
                        src_col,
                        existing_col.default_value,
                        Self::mysql_column_default_value_sql(mysql_type.as_str(), def.as_str())
                    );
                }
                if should_modify && !self.auto_modify_column {
                    return Err(format!(
                        "Dameng DML column type mismatch and auto_modify_column is false: {}.{} existing={} data_length={} char_length={} default={:?} expected={}",
                        table_key,
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type
                    ));
                }
                if should_modify && protected {
                    warn!(
                        "Dameng DML skip auto modify protected column: {}.{} existing={} data_length={} char_length={} default={:?} expected={}",
                        table_key,
                        src_col,
                        existing_col.data_type,
                        existing_col.data_length,
                        existing_col.char_length,
                        existing_col.default_value,
                        dameng_type
                    );
                    continue;
                }
                if should_modify || should_relax_nullable {
                    if should_recreate_for_random_check {
                        let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
                        let add_column_definition = Self::dameng_column_definition(
                            mysql_type.as_str(),
                            def.as_str(),
                            nullable_sql,
                            false,
                        );
                        self.recreate_column_for_random_check(
                            schema,
                            table_name,
                            src_col,
                            target_col.as_str(),
                            add_column_definition.as_str(),
                        )
                        .await?;
                        continue;
                    }
                    let nullable_sql = if should_relax_nullable { "NULL" } else { "" };
                    let sql = format!(
                        "ALTER TABLE {} MODIFY {} {}",
                        Self::qualified_table(schema, table_name),
                        Self::quote_ident(target_col.as_str()),
                        Self::dameng_modify_column_definition_with_nullable(
                            mysql_type.as_str(),
                            nullable_sql
                        )
                    );
                    match self.execute(sql.as_str()).await {
                        Ok(_) => info!(
                            "Dameng DML auto modify column success: {} {} nullable_sql={}",
                            table_key, src_col, nullable_sql
                        ),
                        Err(e) => {
                            let msg = format!(
                                "Dameng DML auto modify column failed: {} {} {}",
                                table_key, src_col, e
                            );
                            error!("{}", msg);
                            return Err(msg);
                        }
                    }
                }
                continue;
            }
            if !self.auto_add_column {
                missing_columns.push(format!("{} -> {}", src_col, target_col));
                continue;
            }

            let (add_column_definition, definition_source) =
                Self::dameng_add_column_definition_for_dml(&table_info, src_col, records);
            let sql = format!(
                "ALTER TABLE {} ADD {} {}",
                Self::qualified_table(schema, table_name),
                Self::quote_ident(target_col.as_str()),
                add_column_definition
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => info!(
                    "Dameng auto add DML column success: {} {} definition={} source={}",
                    table_key, src_col, add_column_definition, definition_source
                ),
                Err(e) => {
                    let msg = format!(
                        "Dameng auto add DML column failed: {} {} definition={} source={} error={}",
                        table_key, src_col, add_column_definition, definition_source, e
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
        }
        if !missing_columns.is_empty() {
            return Err(format!(
                "Dameng target columns are missing and auto_add_column is false: {} columns={}",
                table_key,
                missing_columns.join(", ")
            ));
        }

        let required_defaults = if self.random_check_data_after_init && include_required_defaults {
            self.required_target_column_defaults(schema, table_name, table_key, &requested_keys)
                .await?
        } else {
            Vec::new()
        };
        let mut checked_column_keys = requested_keys;
        checked_column_keys.extend(
            required_defaults
                .iter()
                .map(|(column, _)| column.to_ascii_lowercase()),
        );
        self.dml_checked_column_keys
            .lock()
            .await
            .entry(checked_key)
            .or_default()
            .extend(checked_column_keys);
        if include_required_defaults {
            self.dml_required_column_defaults
                .lock()
                .await
                .insert(table_key.to_ascii_lowercase(), required_defaults.clone());
        }
        Ok(required_defaults)
    }

    async fn cache_dml_columns(&self, table_key: &str, columns: &[String]) {
        self.columns_cache
            .lock()
            .await
            .insert(table_key.to_string(), columns.to_vec());
    }

    async fn required_target_column_defaults(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        requested_target_keys: &HashSet<String>,
    ) -> Result<Vec<(String, Value)>, String> {
        let sql = Self::required_columns_metadata_sql(schema, table_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let metadata = rows
            .iter()
            .filter_map(|row| row.get::<String>(0).ok())
            .collect::<Vec<_>>()
            .join("\x1e");
        let mut defaults = Vec::new();
        let mut requested_identity_columns = Vec::new();
        for column in Self::parse_required_columns_metadata(metadata.as_str()) {
            let requested =
                requested_target_keys.contains(column.column_name.to_ascii_lowercase().as_str());
            if column.identity {
                if requested {
                    requested_identity_columns.push(column.column_name.clone());
                }
                info!(
                    "Dameng random data check init mode skips NOT NULL identity column placeholder: {}.{} column={} requested={}",
                    schema, table_name, column.column_name, requested
                );
                continue;
            }
            if requested {
                info!(
                    "Dameng random data check init mode keeps source column value for target NOT NULL column: {}.{} column={} type={} default={:?}",
                    schema, table_name, column.column_name, column.data_type, column.default_value
                );
                continue;
            }
            if Self::dameng_required_column_has_usable_default(&column) {
                continue;
            }
            let value = Self::dameng_required_column_default_value(&column);
            info!(
                "Dameng random data check init mode fills target-only NOT NULL column with placeholder: {}.{} column={} type={} default={:?} value={}",
                schema,
                table_name,
                column.column_name,
                column.data_type,
                column.default_value,
                value.resolve_string()
            );
            defaults.push((column.column_name, value));
        }
        if !requested_identity_columns.is_empty() {
            self.identity_insert_tables
                .lock()
                .await
                .insert(Self::identity_insert_cache_key(table_key));
            info!(
                "Dameng random data check init mode will enable identity_insert before writing requested identity columns: {} columns={}",
                table_key,
                requested_identity_columns.join(", ")
            );
        }
        Ok(defaults)
    }

    fn required_columns_metadata_sql(schema: &str, table_name: &str) -> String {
        let schema_filter = if schema.is_empty() {
            "sys_schema.NAME = USER".to_string()
        } else {
            Self::eq_original_or_upper_sql("sys_schema.NAME", schema)
        };
        let identity_expr = format!(
            "CASE WHEN EXISTS (SELECT 1 FROM SYS.SYSCOLUMNS sys_col JOIN SYS.SYSOBJECTS sys_table ON sys_col.ID = sys_table.ID JOIN SYS.SYSOBJECTS sys_schema ON sys_table.SCHID = sys_schema.ID WHERE sys_schema.TYPE$ = 'SCH' AND {} AND sys_table.NAME = c.TABLE_NAME AND sys_col.NAME = c.COLUMN_NAME AND NVL(sys_col.INFO2, 0) <> 0) THEN 'Y' ELSE 'N' END",
            schema_filter
        );
        let expr = format!(
            "c.COLUMN_NAME || CHR(31) || c.DATA_TYPE || CHR(31) || NVL(REPLACE(REPLACE(TRIM(c.DATA_DEFAULT), CHR(30), ' '), CHR(31), ' '), '') || CHR(31) || {}",
            identity_expr
        );
        let required_filter = "c.NULLABLE = 'N'";
        if schema.is_empty() {
            format!(
                "SELECT {} FROM USER_TAB_COLUMNS c WHERE {} AND {} ORDER BY c.COLUMN_ID",
                expr,
                Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name),
                required_filter
            )
        } else {
            format!(
                "SELECT {} FROM ALL_TAB_COLUMNS c WHERE {} AND {} AND {} ORDER BY c.COLUMN_ID",
                expr,
                Self::eq_original_or_upper_sql("c.OWNER", schema),
                Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name),
                required_filter
            )
        }
    }

    fn parse_required_columns_metadata(metadata: &str) -> Vec<DamengRequiredColumnInfo> {
        metadata
            .split('\x1e')
            .filter(|row| !row.is_empty())
            .filter_map(|row| {
                let mut parts = row.splitn(4, '\x1f');
                let column_name = parts.next()?;
                let data_type = parts.next()?;
                let default_value = parts
                    .next()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string);
                let identity = parts
                    .next()
                    .map(|value| {
                        let normalized = value.trim().to_ascii_uppercase();
                        normalized == "Y" || normalized == "YES" || normalized == "1"
                    })
                    .unwrap_or(false);
                if column_name.is_empty() || data_type.is_empty() {
                    return None;
                }
                Some(DamengRequiredColumnInfo {
                    column_name: column_name.to_string(),
                    data_type: data_type.to_string(),
                    default_value,
                    identity,
                })
            })
            .collect()
    }

    fn dameng_required_column_has_usable_default(column: &DamengRequiredColumnInfo) -> bool {
        column
            .default_value
            .as_deref()
            .map(|value| {
                let normalized = Self::normalize_default_for_compare(value);
                !normalized.is_empty() && normalized != "null"
            })
            .unwrap_or(false)
    }

    fn dameng_required_column_default_value(column: &DamengRequiredColumnInfo) -> Value {
        let data_type = column.data_type.to_ascii_uppercase();
        if data_type.contains("BLOB") || data_type.contains("BINARY") || data_type.contains("RAW") {
            Value::Blob(Vec::new())
        } else if data_type.contains("CHAR")
            || data_type.contains("CLOB")
            || data_type.contains("TEXT")
        {
            Value::String(String::new())
        } else if data_type == "TIME" {
            Value::Time("00:00:00".to_string())
        } else if data_type.contains("TIMESTAMP") || data_type.contains("DATETIME") {
            Value::DateTime("1970-01-01 00:00:00".to_string())
        } else if data_type.contains("DATE") {
            Value::Date("1970-01-01".to_string())
        } else if data_type.contains("BIT") || data_type.contains("BOOL") {
            Value::Bit(0)
        } else if data_type.contains("FLOAT")
            || data_type.contains("DOUBLE")
            || data_type.contains("REAL")
        {
            Value::Double(0.0)
        } else if data_type.contains("DECIMAL") || data_type.contains("NUMERIC") {
            Value::Decimal("0".to_string())
        } else {
            Value::Int64(0)
        }
    }

    fn apply_dml_required_defaults(
        columns: &[String],
        records: &[DataBuffer],
        required_defaults: &[(String, Value)],
    ) -> (Vec<String>, Vec<DataBuffer>) {
        if required_defaults.is_empty() {
            return (columns.to_vec(), records.to_vec());
        }
        let required_columns = required_defaults
            .iter()
            .map(|(column, _)| column.clone())
            .collect::<Vec<_>>();
        let dml_columns = Self::merge_column_lists(columns, &required_columns);
        let mut dml_records = records.to_vec();
        for record in &mut dml_records {
            for (column, value) in required_defaults {
                if !record.after.contains_key(column) {
                    record.after.insert(column.clone(), value.clone());
                }
            }
        }
        (dml_columns, dml_records)
    }

    fn dameng_add_column_definition_for_dml(
        table_info: &TableInfoVo,
        source_column: &str,
        records: &[DataBuffer],
    ) -> (String, &'static str) {
        if let Some(def) =
            Self::mysql_column_definition_for_source_column(table_info, source_column)
            && let Some(mysql_type) = mysql_type_token_from_column_definition(def.as_str())
        {
            let mut nullable = mysql_column_allows_null_from_definition(def.as_str());
            let primary_key_columns = Self::source_primary_key_columns(table_info);
            if Self::contains_source_column(&primary_key_columns, source_column) {
                nullable = false;
            }
            let nullable_sql = if nullable { "NULL" } else { "NOT NULL" };
            return (
                Self::dameng_column_definition(
                    mysql_type.as_str(),
                    def.as_str(),
                    nullable_sql,
                    false,
                ),
                "mysql_schema",
            );
        }

        (
            Self::dameng_column_definition_for_value(Self::first_non_null_record_value(
                records,
                source_column,
            )),
            "record_value",
        )
    }

    fn mysql_column_definition_for_source_column(
        table_info: &TableInfoVo,
        source_column: &str,
    ) -> Option<String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        defs.get(source_column.to_ascii_lowercase().as_str())
            .cloned()
    }

    fn first_non_null_record_value<'a>(
        records: &'a [DataBuffer],
        source_column: &str,
    ) -> Option<&'a Value> {
        records
            .iter()
            .map(|record| record.after.get(source_column))
            .find(|value| !value.is_none())
    }

    fn dameng_column_definition_for_value(value: Option<&Value>) -> String {
        let data_type = match value {
            Some(Value::Int8(_)) => "TINYINT",
            Some(Value::UnsignedInt8(_)) | Some(Value::Int16(_)) => "SMALLINT",
            Some(Value::UnsignedInt16(_)) | Some(Value::Int32(_)) => "INT",
            Some(Value::UnsignedInt32(_)) | Some(Value::Int64(_)) => "BIGINT",
            Some(Value::UnsignedInt64(_)) => "DECIMAL(20,0)",
            Some(Value::Float(_)) => "FLOAT",
            Some(Value::Double(_)) => "DOUBLE",
            Some(Value::Decimal(_)) => "DECIMAL(38,18)",
            Some(Value::Time(_)) => "TIME",
            Some(Value::Date(_)) => "DATE",
            Some(Value::DateTime(_)) | Some(Value::Timestamp(_)) => "TIMESTAMP",
            Some(Value::Year(_)) => "INT",
            Some(Value::Blob(_)) => "BLOB",
            Some(Value::Bit(_)) => "BIGINT",
            Some(Value::String(_)) | Some(Value::Json(_)) | Some(Value::None) | None => "CLOB",
        };
        format!("{} NULL", data_type)
    }

    async fn table_exists(&self, schema: &str, table_name: &str) -> Result<bool, String> {
        let sql = if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_TABLES WHERE {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_TABLES WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        };
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn schema_exists(&self, schema: &str) -> Result<bool, String> {
        let rows = self
            .query(Self::schema_exists_sql(schema).as_str(), &[])
            .await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn existing_columns(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<HashMap<String, DamengColumnInfo>, String> {
        let sql = Self::columns_metadata_sql(schema, table_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let metadata = rows
            .iter()
            .filter_map(|row| row.get::<String>(0).ok())
            .collect::<Vec<_>>()
            .join("\x1e");
        Ok(Self::parse_columns_metadata(metadata.as_str()))
    }

    fn columns_metadata_sql(schema: &str, table_name: &str) -> String {
        let expr = "COLUMN_NAME || CHR(31) || DATA_TYPE || CHR(31) || DATA_LENGTH || CHR(31) || CHAR_LENGTH || CHR(31) || NVL(TO_CHAR(DATA_PRECISION), '') || CHR(31) || NVL(TO_CHAR(DATA_SCALE), '') || CHR(31) || NVL(REPLACE(REPLACE(TRIM(DATA_DEFAULT), CHR(30), ' '), CHR(31), ' '), '') || CHR(31) || NULLABLE";
        if schema.is_empty() {
            format!(
                "SELECT {} FROM USER_TAB_COLUMNS WHERE {} ORDER BY COLUMN_ID",
                expr,
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT {} FROM ALL_TAB_COLUMNS WHERE {} AND {} ORDER BY COLUMN_ID",
                expr,
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        }
    }

    async fn existing_table_comment(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<Option<String>, String> {
        let sql = Self::table_comment_sql(schema, table_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        Ok(rows
            .first()
            .and_then(|row| row.get::<String>(0).ok())
            .map(|comment| comment.trim().to_string()))
    }

    fn table_comment_sql(schema: &str, table_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT NVL(COMMENTS, '') FROM USER_TAB_COMMENTS WHERE {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT NVL(COMMENTS, '') FROM ALL_TAB_COMMENTS WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name)
            )
        }
    }

    async fn existing_column_comments(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<HashMap<String, String>, String> {
        if !schema.is_empty() {
            let dba_sql = Self::dba_column_comments_sql(schema, table_name);
            match self.query(dba_sql.as_str(), &[]).await {
                Ok(rows) => {
                    let comments = Self::column_comments_from_rows(&rows);
                    if !comments.is_empty() {
                        return Ok(comments);
                    }
                    warn!(
                        "Dameng DBA_COL_COMMENTS returned no column comments, fallback to ALL_COL_COMMENTS: {}",
                        Self::target_table_key(schema, table_name)
                    );
                }
                Err(e) => warn!(
                    "Dameng DBA_COL_COMMENTS query failed, fallback to ALL_COL_COMMENTS: {} {}",
                    Self::target_table_key(schema, table_name),
                    e
                ),
            }
        }

        let sql = Self::all_column_comments_sql(schema, table_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        Ok(Self::column_comments_from_rows(&rows))
    }

    fn column_comments_from_rows(rows: &dameng::ResultSet) -> HashMap<String, String> {
        let metadata = rows
            .iter()
            .filter_map(|row| row.get::<String>(0).ok())
            .collect::<Vec<_>>()
            .join("\x1e");
        Self::parse_column_comments_metadata(metadata.as_str())
    }

    fn dba_column_comments_sql(schema: &str, table_name: &str) -> String {
        format!(
            "SELECT c.COLUMN_NAME || CHR(31) || NVL(REPLACE(REPLACE(c.COMMENTS, CHR(30), ' '), CHR(31), ' '), '') FROM DBA_COL_COMMENTS c WHERE {} AND {} ORDER BY c.COLUMN_NAME",
            Self::eq_original_or_upper_sql("c.OWNER", schema),
            Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name)
        )
    }

    fn all_column_comments_sql(schema: &str, table_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT c.COLUMN_NAME || CHR(31) || NVL(REPLACE(REPLACE(cc.COMMENTS, CHR(30), ' '), CHR(31), ' '), '') FROM USER_TAB_COLUMNS c LEFT JOIN USER_COL_COMMENTS cc ON cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME WHERE {} ORDER BY c.COLUMN_ID",
                Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name)
            )
        } else {
            format!(
                "SELECT c.COLUMN_NAME || CHR(31) || NVL(REPLACE(REPLACE(cc.COMMENTS, CHR(30), ' '), CHR(31), ' '), '') FROM ALL_TAB_COLUMNS c LEFT JOIN ALL_COL_COMMENTS cc ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME WHERE {} AND {} ORDER BY c.COLUMN_ID",
                Self::eq_original_or_upper_sql("c.OWNER", schema),
                Self::eq_original_or_upper_sql("c.TABLE_NAME", table_name)
            )
        }
    }

    fn parse_columns_metadata(metadata: &str) -> HashMap<String, DamengColumnInfo> {
        let mut cols = HashMap::new();
        let row_separator = if metadata.contains('\x1e') || metadata.contains('\x1f') {
            '\x1e'
        } else {
            '|'
        };
        for column in metadata.split(row_separator).filter(|s| !s.is_empty()) {
            let field_separator = if column.contains('\x1f') { '\x1f' } else { ':' };
            if let Some((name, info)) = Self::parse_column_metadata(column, field_separator) {
                cols.insert(name.to_ascii_lowercase(), info);
            }
        }
        cols
    }

    fn parse_column_metadata(
        column: &str,
        field_separator: char,
    ) -> Option<(&str, DamengColumnInfo)> {
        let parts = column.split(field_separator).collect::<Vec<_>>();
        if parts.len() < 5 {
            return None;
        }
        let name = match parts.first() {
            Some(v) if !v.is_empty() => *v,
            _ => return None,
        };
        let data_type = match parts.get(1) {
            Some(v) if !v.is_empty() => *v,
            _ => return None,
        };
        let data_length = parts
            .get(2)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let char_length = parts
            .get(3)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let (data_precision, data_scale, default_index) = if parts.len() >= 7 {
            (
                parts.get(4).and_then(|v| v.parse::<u32>().ok()),
                parts.get(5).and_then(|v| v.parse::<u32>().ok()),
                6,
            )
        } else {
            (None, None, 4)
        };
        let default_value = parts
            .get(default_index)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(ToString::to_string);
        let nullable = parts
            .get(default_index + 1)
            .map(|v| {
                let normalized = v.trim().to_ascii_uppercase();
                normalized != "N" && normalized != "NO" && normalized != "0"
            })
            .unwrap_or(true);
        Some((
            name,
            DamengColumnInfo {
                data_type: data_type.to_string(),
                data_length,
                char_length,
                data_precision,
                data_scale,
                default_value,
                nullable,
            },
        ))
    }

    fn parse_column_comments_metadata(metadata: &str) -> HashMap<String, String> {
        let mut comments = HashMap::new();
        for column in metadata.split('\x1e').filter(|s| !s.is_empty()) {
            let Some((name, comment)) = column.split_once('\x1f') else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            comments.insert(name.to_ascii_lowercase(), comment.to_string());
        }
        comments
    }

    fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn text_sql_literal(value: &str) -> String {
        if !value.chars().any(Self::text_char_requires_chr) {
            return Self::quote_literal(value);
        }

        let mut parts = Vec::new();
        let mut literal = String::new();
        for ch in value.chars() {
            if Self::text_char_requires_chr(ch) {
                if !literal.is_empty() {
                    parts.push(Self::quote_literal(literal.as_str()));
                    literal.clear();
                }
                parts.push(format!("CHR({})", ch as u32));
            } else {
                literal.push(ch);
            }
        }
        if !literal.is_empty() {
            parts.push(Self::quote_literal(literal.as_str()));
        }

        if parts.is_empty() {
            Self::quote_literal("")
        } else {
            parts.join(" || ")
        }
    }

    fn text_char_requires_chr(ch: char) -> bool {
        let code = ch as u32;
        code < 0x20 || code == 0x7f
    }

    fn eq_original_or_upper_sql(column_name: &str, value: &str) -> String {
        format!(
            "({} = {} OR {} = {})",
            column_name,
            Self::quote_literal(value),
            column_name,
            Self::quote_literal(value.to_ascii_uppercase().as_str())
        )
    }

    async fn execute(&self, sql: &str) -> Result<u64, String> {
        let first_result = {
            let mut client = self.client.lock().await;
            client.execute(sql)
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("Dameng execute failed, retrying: {} sql: {}", e, sql);
                self.reconnect().await?;
                self.client
                    .lock()
                    .await
                    .execute(sql)
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn execute_with_params(&self, sql: &str, params: &[DamengParam]) -> Result<u64, String> {
        if !sql.is_ascii() || Self::params_require_inline(params) {
            return self.execute_inline_params(sql, params, false).await;
        }

        let first_result = {
            let mut client = self.client.lock().await;
            let refs = Self::param_refs(params);
            client.execute_with_params(sql, &refs)
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("Dameng execute failed, retrying: {} sql: {}", e, sql);
                self.reconnect().await?;
                let mut client = self.client.lock().await;
                let refs = Self::param_refs(params);
                client
                    .execute_with_params(sql, &refs)
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn execute_insert_with_params(
        &self,
        sql: &str,
        params: &[DamengParam],
    ) -> Result<u64, String> {
        if !sql.is_ascii() || Self::params_require_inline(params) {
            return self.execute_inline_params(sql, params, true).await;
        }

        let first_result = {
            let mut client = self.client.lock().await;
            let refs = Self::param_refs(params);
            client.execute_with_params(sql, &refs)
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if Self::is_insert_control_flow_error(msg.as_str()) {
                    return Err(msg);
                }
                error!("Dameng execute failed, retrying: {} sql: {}", msg, sql);
                self.reconnect().await?;
                let mut client = self.client.lock().await;
                let refs = Self::param_refs(params);
                client
                    .execute_with_params(sql, &refs)
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn execute_inline_params(
        &self,
        sql: &str,
        params: &[DamengParam],
        insert_control_flow_passthrough: bool,
    ) -> Result<u64, String> {
        let inline_sql = Self::inline_sql_params(sql, params)?;
        let first_result = {
            let mut client = self.client.lock().await;
            client.execute(inline_sql.as_str())
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if insert_control_flow_passthrough
                    && Self::is_insert_control_flow_error(msg.as_str())
                {
                    return Err(msg);
                }
                error!(
                    "Dameng execute inline params failed, retrying: {} sql: {}",
                    msg, inline_sql
                );
                self.reconnect().await?;
                self.client
                    .lock()
                    .await
                    .execute(inline_sql.as_str())
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn query(&self, sql: &str, params: &[DamengParam]) -> Result<dameng::ResultSet, String> {
        if !sql.is_ascii() || Self::params_require_inline(params) {
            return self.query_inline_params(sql, params).await;
        }

        let mut client = self.client.lock().await;
        let refs = Self::param_refs(params);
        client
            .query_with_params(sql, &refs)
            .map_err(|e| e.to_string())
    }

    async fn query_inline_params(
        &self,
        sql: &str,
        params: &[DamengParam],
    ) -> Result<dameng::ResultSet, String> {
        let inline_sql = Self::inline_sql_params(sql, params)?;
        let first_result = {
            let mut client = self.client.lock().await;
            client.query(inline_sql.as_str())
        };
        match first_result {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                error!(
                    "Dameng query inline params failed, retrying: {} sql: {}",
                    msg, inline_sql
                );
                self.reconnect().await?;
                self.client
                    .lock()
                    .await
                    .query(inline_sql.as_str())
                    .map_err(|e| e.to_string())
            }
        }
    }

    fn param_refs(params: &[DamengParam]) -> Vec<&dyn ToDmValue> {
        params.iter().map(|p| p as &dyn ToDmValue).collect()
    }

    fn params_require_inline(params: &[DamengParam]) -> bool {
        params
            .iter()
            .any(|param| matches!(param, DamengParam::Text(v) if !v.is_ascii()))
    }

    fn inline_sql_params(sql: &str, params: &[DamengParam]) -> Result<String, String> {
        let mut result = String::with_capacity(sql.len() + params.len() * 8);
        let mut param_index = 0;
        let mut quote: Option<char> = None;
        let mut chars = sql.chars().peekable();

        while let Some(ch) = chars.next() {
            if let Some(quote_char) = quote {
                result.push(ch);
                if ch == quote_char {
                    if chars.peek() == Some(&quote_char) {
                        result.push(chars.next().unwrap());
                    } else {
                        quote = None;
                    }
                }
                continue;
            }

            match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    result.push(ch);
                }
                '?' => {
                    let Some(param) = params.get(param_index) else {
                        return Err(format!(
                            "Dameng inline params placeholder count exceeds params: sql={}",
                            sql
                        ));
                    };
                    result.push_str(Self::dameng_param_sql_literal(param).as_str());
                    param_index += 1;
                }
                _ => result.push(ch),
            }
        }

        if param_index != params.len() {
            return Err(format!(
                "Dameng inline params count mismatch: placeholders={} params={} sql={}",
                param_index,
                params.len(),
                sql
            ));
        }
        Ok(result)
    }

    fn is_dameng_connection_error(error: &str) -> bool {
        let error = error.to_ascii_lowercase();
        error.contains("broken pipe")
            || error.contains("connection reset")
            || error.contains("connection refused")
            || error.contains("connection aborted")
            || error.contains("connection timed out")
            || error.contains("unexpected eof")
            || error.contains("protocol error")
            || error.contains("incomplete protocol data")
            || error.contains("io error")
    }

    fn dameng_param_sql_literal(param: &DamengParam) -> String {
        match param {
            DamengParam::Null => "NULL".to_string(),
            DamengParam::I8(v) => v.to_string(),
            DamengParam::I16(v) => v.to_string(),
            DamengParam::I32(v) => v.to_string(),
            DamengParam::I64(v) => v.to_string(),
            DamengParam::F32(v) => v.to_string(),
            DamengParam::F64(v) => v.to_string(),
            DamengParam::Text(v) => Self::text_sql_literal(v),
            DamengParam::Bytes(v) => {
                format!("HEXTORAW({})", Self::quote_literal(&Self::bytes_to_hex(v)))
            }
        }
    }

    fn is_identity_insert_required_error(error: &str) -> bool {
        error.contains("-2723") || error.to_ascii_uppercase().contains("IDENTITY_INSERT")
    }

    fn is_duplicate_key_error(error: &str) -> bool {
        error.contains("-6602") || error.to_ascii_lowercase().contains("unique constraint")
    }

    fn is_insert_control_flow_error(error: &str) -> bool {
        Self::is_identity_insert_required_error(error) || Self::is_duplicate_key_error(error)
    }

    fn identity_insert_cache_key(table_key: &str) -> String {
        table_key.to_ascii_lowercase()
    }

    fn identity_insert_sql(qualified_table: &str, enabled: bool) -> String {
        format!(
            "SET IDENTITY_INSERT {} {}",
            qualified_table,
            if enabled { "ON" } else { "OFF" }
        )
    }

    async fn set_identity_insert(
        &self,
        qualified_table: &str,
        enabled: bool,
    ) -> Result<(), String> {
        self.execute(Self::identity_insert_sql(qualified_table, enabled).as_str())
            .await
            .map(|_| ())
    }

    async fn ensure_identity_insert_enabled(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
    ) -> Result<(), String> {
        let key = Self::identity_insert_cache_key(table_key);
        let qualified_table = Self::qualified_table(schema, table_name);
        let mut active = self.active_identity_insert_table.lock().await;
        if active
            .as_ref()
            .is_some_and(|(active_key, _)| active_key == &key)
        {
            return Ok(());
        }

        if let Some((_, active_qualified_table)) = active.as_ref() {
            self.set_identity_insert(active_qualified_table.as_str(), false)
                .await?;
        }
        self.set_identity_insert(qualified_table.as_str(), true)
            .await?;
        *active = Some((key, qualified_table));
        Ok(())
    }

    async fn disable_active_identity_insert(&self) -> Result<(), String> {
        let mut active = self.active_identity_insert_table.lock().await;
        if let Some((_, active_qualified_table)) = active.as_ref() {
            self.set_identity_insert(active_qualified_table.as_str(), false)
                .await?;
            *active = None;
        }
        Ok(())
    }

    async fn clear_active_identity_insert_state(&self) {
        *self.active_identity_insert_table.lock().await = None;
    }

    async fn get_pk_name_from_cache(&self, table_name: &str) -> String {
        self.table_info_cache
            .lock()
            .await
            .get(table_name)
            .pk_column
            .to_string()
    }

    fn create_schema_sql(schema: &str) -> String {
        format!("CREATE SCHEMA {}", Self::quote_ident(schema))
    }

    fn schema_exists_sql(schema: &str) -> String {
        format!(
            "SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE$ = 'SCH' AND {}",
            Self::eq_original_or_upper_sql("NAME", schema)
        )
    }

    fn table_info_target_schema(table_info: &TableInfoVo, fallback: &str) -> String {
        if table_info.target_database.trim().is_empty() {
            fallback.to_string()
        } else {
            table_info.target_database.clone()
        }
    }

    fn record_target_schema(&self, record: &DataBuffer) -> String {
        if record.target_database.trim().is_empty() {
            self.schema.clone()
        } else {
            record.target_database.clone()
        }
    }

    fn target_table_key(schema: &str, table_name: &str) -> String {
        database_table_key(schema, table_name)
    }

    fn target_schemas(&self) -> Vec<String> {
        let mut schemas = Vec::new();
        let mut seen = HashSet::new();
        if !self.schema.trim().is_empty() && seen.insert(self.schema.to_ascii_lowercase()) {
            schemas.push(self.schema.clone());
        }
        for table_info in &self.table_info_list {
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            if schema.trim().is_empty() {
                continue;
            }
            if seen.insert(schema.to_ascii_lowercase()) {
                schemas.push(schema);
            }
        }
        schemas
    }

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn quote_column_ident(column_name: &str) -> String {
        Self::quote_ident(Self::target_column_name(column_name).as_str())
    }

    fn target_column_name(column_name: &str) -> String {
        if Self::is_disallowed_dameng_column_name(column_name) {
            format!("{}_", column_name)
        } else {
            column_name.to_string()
        }
    }

    fn is_disallowed_dameng_column_name(column_name: &str) -> bool {
        matches!(
            column_name.to_ascii_uppercase().as_str(),
            "ROWID"
                | "ROWNUM"
                | "TRXID"
                | "VERSIONS_STARTTIME"
                | "VERSIONS_ENDTIME"
                | "VERSIONS_STARTTRXID"
                | "VERSIONS_ENDTRXID"
                | "VERSIONS_OPERATION"
        )
    }

    fn qualified_table(schema: &str, table_name: &str) -> String {
        if schema.is_empty() {
            Self::quote_ident(table_name)
        } else {
            format!(
                "{}.{}",
                Self::quote_ident(schema),
                Self::quote_ident(table_name)
            )
        }
    }

    fn foreign_key_sql(schema: &str, foreign_key: &ForeignKeyInfo) -> Option<String> {
        if foreign_key.columns.is_empty()
            || foreign_key.columns.len() != foreign_key.referenced_columns.len()
        {
            return None;
        }
        let columns = foreign_key
            .columns
            .iter()
            .map(|column| Self::quote_column_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let referenced_columns = foreign_key
            .referenced_columns
            .iter()
            .map(|column| Self::quote_column_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
            Self::qualified_table(schema, foreign_key.table_name.as_str()),
            Self::quote_ident(foreign_key.constraint_name.as_str()),
            columns,
            Self::qualified_table(schema, foreign_key.referenced_table_name.as_str()),
            referenced_columns
        );
        if let Some(delete_rule) = Self::dameng_fk_action(foreign_key.delete_rule.as_deref()) {
            sql.push_str(" ON DELETE ");
            sql.push_str(delete_rule);
        }
        if let Some(update_rule) = Self::dameng_fk_action(foreign_key.update_rule.as_deref()) {
            sql.push_str(" ON UPDATE ");
            sql.push_str(update_rule);
        }
        sql.push_str(" WITH INDEX");
        Some(sql)
    }

    fn dameng_fk_action(rule: Option<&str>) -> Option<&'static str> {
        match rule.unwrap_or("").trim().to_ascii_uppercase().as_str() {
            "CASCADE" => Some("CASCADE"),
            "SET NULL" => Some("SET NULL"),
            "SET DEFAULT" => Some("SET DEFAULT"),
            "NO ACTION" | "RESTRICT" => Some("NO ACTION"),
            _ => None,
        }
    }

    async fn foreign_key_exists(
        &self,
        schema: &str,
        table_name: &str,
        constraint_name: &str,
    ) -> Result<bool, String> {
        let sql = if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE {} AND {} AND CONSTRAINT_TYPE = 'R'",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name),
                Self::eq_original_or_upper_sql("CONSTRAINT_NAME", constraint_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_CONSTRAINTS WHERE {} AND {} AND {} AND CONSTRAINT_TYPE = 'R'",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name),
                Self::eq_original_or_upper_sql("CONSTRAINT_NAME", constraint_name)
            )
        };
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn ensure_foreign_keys(&self) -> Result<(), String> {
        if !self.sync_foreign_key_tables {
            return Ok(());
        }
        for table_info in &self.table_info_list {
            if table_info.foreign_keys.is_empty() {
                continue;
            }
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            for foreign_key in &table_info.foreign_keys {
                if self
                    .foreign_key_exists(
                        schema.as_str(),
                        table_info.table_name.as_str(),
                        foreign_key.constraint_name.as_str(),
                    )
                    .await?
                {
                    continue;
                }
                let Some(sql) = Self::foreign_key_sql(schema.as_str(), foreign_key) else {
                    warn!(
                        "Dameng skip invalid foreign key metadata: {}.{}",
                        table_info.table_name, foreign_key.constraint_name
                    );
                    continue;
                };
                match self.try_execute_optional(sql.as_str()).await {
                    Ok(_) => info!(
                        "Dameng auto add foreign key success: {}.{}",
                        schema, foreign_key.constraint_name
                    ),
                    Err(e) => warn!(
                        "Dameng skip foreign key after add failed: {}.{} {}",
                        schema, foreign_key.constraint_name, e
                    ),
                }
            }
        }
        Ok(())
    }

    async fn ensure_secondary_indexes(&self) -> Result<(), String> {
        for table_info in &self.table_info_list {
            if table_info.indexes.is_empty() {
                continue;
            }
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            for index in &table_info.indexes {
                let (target_index_name, exists) = self
                    .dameng_available_index_name(
                        schema.as_str(),
                        table_info.table_name.as_str(),
                        index.index_name.as_str(),
                    )
                    .await?;
                if exists {
                    continue;
                }
                let Some(sql) = Self::secondary_index_sql(
                    schema.as_str(),
                    table_info,
                    index,
                    target_index_name.as_str(),
                ) else {
                    warn!(
                        "Dameng skip unsupported secondary index: {}.{}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        index.index_name
                    );
                    continue;
                };
                match self.try_execute_optional(sql.as_str()).await {
                    Ok(_) => info!(
                        "Dameng auto create secondary index success: {}.{} -> {}",
                        Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                        index.index_name,
                        target_index_name
                    ),
                    Err(e) => {
                        let msg = format!(
                            "Dameng auto create secondary index failed: {}.{} target_index={} sql={} error={}",
                            Self::target_table_key(schema.as_str(), table_info.table_name.as_str()),
                            index.index_name,
                            target_index_name,
                            sql,
                            e
                        );
                        error!("{}", msg);
                        return Err(msg);
                    }
                }
            }
        }
        Ok(())
    }

    async fn dameng_available_index_name(
        &self,
        schema: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<(String, bool), String> {
        if self
            .dameng_table_index_exists(schema, table_name, index_name)
            .await?
        {
            return Ok((index_name.to_string(), true));
        }
        if !self.dameng_index_name_exists(schema, index_name).await? {
            return Ok((index_name.to_string(), false));
        }
        for index in 0..100 {
            let candidate = Self::conflict_index_name(table_name, index_name, index);
            if self
                .dameng_table_index_exists(schema, table_name, candidate.as_str())
                .await?
            {
                return Ok((candidate, true));
            }
            if !self
                .dameng_index_name_exists(schema, candidate.as_str())
                .await?
            {
                return Ok((candidate, false));
            }
        }
        Err(format!(
            "no available Dameng index name for conflict: {}.{} {}",
            schema, table_name, index_name
        ))
    }

    async fn dameng_table_index_exists(
        &self,
        schema: &str,
        table_name: &str,
        index_name: &str,
    ) -> Result<bool, String> {
        let sql = Self::dameng_table_index_exists_sql(schema, table_name, index_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    async fn dameng_index_name_exists(
        &self,
        schema: &str,
        index_name: &str,
    ) -> Result<bool, String> {
        let sql = Self::dameng_index_name_exists_sql(schema, index_name);
        let rows = self.query(sql.as_str(), &[]).await?;
        let count = rows
            .first()
            .and_then(|row| row.get::<i32>(0).ok())
            .unwrap_or(0);
        Ok(count > 0)
    }

    fn dameng_table_index_exists_sql(schema: &str, table_name: &str, index_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_INDEXES WHERE {} AND {}",
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name),
                Self::eq_original_or_upper_sql("INDEX_NAME", index_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_INDEXES WHERE {} AND {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("TABLE_NAME", table_name),
                Self::eq_original_or_upper_sql("INDEX_NAME", index_name)
            )
        }
    }

    fn dameng_index_name_exists_sql(schema: &str, index_name: &str) -> String {
        if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM USER_INDEXES WHERE {}",
                Self::eq_original_or_upper_sql("INDEX_NAME", index_name)
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ALL_INDEXES WHERE {} AND {}",
                Self::eq_original_or_upper_sql("OWNER", schema),
                Self::eq_original_or_upper_sql("INDEX_NAME", index_name)
            )
        }
    }

    fn secondary_index_sql(
        schema: &str,
        table_info: &TableInfoVo,
        index: &TableIndexInfo,
        target_index_name: &str,
    ) -> Option<String> {
        if index.columns.is_empty() {
            return None;
        }
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let mut columns = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            let def = defs.get(column.to_ascii_lowercase().as_str())?;
            let mysql_type = mysql_type_token_from_column_definition(def.as_str())?;
            let dameng_type = Self::map_mysql_type_to_dameng(mysql_type.as_str());
            if dameng_type.eq_ignore_ascii_case("CLOB") || dameng_type.eq_ignore_ascii_case("BLOB")
            {
                return None;
            }
            columns.push(Self::quote_column_ident(column));
        }
        let index_kind = if index.unique {
            "CREATE UNIQUE INDEX"
        } else {
            "CREATE INDEX"
        };
        Some(format!(
            "{} {} ON {} ({})",
            index_kind,
            Self::quote_ident(target_index_name),
            Self::qualified_table(schema, table_info.table_name.as_str()),
            columns.join(", ")
        ))
    }

    fn conflict_index_name(table_name: &str, index_name: &str, index: usize) -> String {
        let base = format!("{}_{}", table_name, index_name);
        let suffix = if index == 0 {
            "_idx".to_string()
        } else {
            format!("_idx{}", index + 1)
        };
        Self::identifier_with_suffix(base.as_str(), suffix.as_str())
    }

    async fn disable_foreign_key_constraints_for_init(&self) -> Result<(), String> {
        let target_tables = self.random_check_target_tables_by_schema();
        if target_tables.is_empty() {
            return Ok(());
        }

        let mut disabled_constraints = Vec::new();
        let mut disabled_count = 0usize;
        for (schema, table_names) in target_tables {
            let constraints = self
                .enabled_foreign_key_constraints_for_schema(schema.as_str())
                .await
                .map_err(|e| {
                    format!(
                        "query enabled foreign key constraints failed: schema={} {}",
                        schema, e
                    )
                })?;
            for constraint in constraints {
                if !table_names.contains(constraint.table_name.to_ascii_lowercase().as_str()) {
                    continue;
                }
                let sql = Self::disable_constraint_sql(
                    constraint.schema.as_str(),
                    constraint.table_name.as_str(),
                    constraint.constraint_name.as_str(),
                );
                self.execute(sql.as_str()).await.map_err(|e| {
                    format!(
                        "disable foreign key constraint failed: {}.{}.{} {}",
                        constraint.schema, constraint.table_name, constraint.constraint_name, e
                    )
                })?;
                disabled_count += 1;
                disabled_constraints.push(constraint.clone());
                info!(
                    "Dameng disabled foreign key constraint before init: {}.{}.{}",
                    constraint.schema, constraint.table_name, constraint.constraint_name
                );
            }
        }

        *self.init_disabled_foreign_key_constraints.lock().await = disabled_constraints;

        if disabled_count == 0 {
            info!("Dameng found no enabled foreign key constraints on sync tables before init");
        } else {
            info!(
                "Dameng disabled foreign key constraints on sync tables before init: count={}",
                disabled_count
            );
        }
        Ok(())
    }

    async fn enable_foreign_key_constraints_after_init(&self) {
        let constraints = {
            let mut guard = self.init_disabled_foreign_key_constraints.lock().await;
            std::mem::take(&mut *guard)
        };
        if constraints.is_empty() {
            return;
        }

        let mut enabled_count = 0usize;
        for constraint in constraints {
            let sql = Self::enable_constraint_sql(
                constraint.schema.as_str(),
                constraint.table_name.as_str(),
                constraint.constraint_name.as_str(),
            );
            match self.execute(sql.as_str()).await {
                Ok(_) => {
                    enabled_count += 1;
                    info!(
                        "Dameng re-enabled foreign key constraint after init: {}.{}.{}",
                        constraint.schema, constraint.table_name, constraint.constraint_name
                    );
                }
                Err(e) => warn!(
                    "Dameng re-enable foreign key constraint after init failed, keep disabled: {}.{}.{} {}",
                    constraint.schema, constraint.table_name, constraint.constraint_name, e
                ),
            }
        }
        info!(
            "Dameng re-enabled foreign key constraints after init: count={}",
            enabled_count
        );
    }

    fn random_check_target_tables_by_schema(&self) -> HashMap<String, HashSet<String>> {
        let mut result = HashMap::new();
        for table_info in &self.table_info_list {
            if table_info.table_name.trim().is_empty() {
                continue;
            }
            let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
            result
                .entry(schema)
                .or_insert_with(HashSet::new)
                .insert(table_info.table_name.to_ascii_lowercase());
        }
        result
    }

    async fn enabled_foreign_key_constraints_for_schema(
        &self,
        schema: &str,
    ) -> Result<Vec<DamengForeignKeyConstraintInfo>, String> {
        let sql = Self::foreign_key_constraints_schema_metadata_sql(schema);
        let rows = self.query(sql.as_str(), &[]).await?;
        let metadata = rows
            .iter()
            .filter_map(|row| row.get::<String>(0).ok())
            .collect::<Vec<_>>()
            .join("\x1e");
        Ok(Self::parse_foreign_key_constraints_metadata(
            schema,
            metadata.as_str(),
        ))
    }

    fn foreign_key_constraints_schema_metadata_sql(schema: &str) -> String {
        let enabled_fk_filter = "CONSTRAINT_TYPE = 'R' AND STATUS = 'ENABLED'";
        if schema.is_empty() {
            format!(
                "SELECT TABLE_NAME || CHR(31) || CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE {} ORDER BY TABLE_NAME, CONSTRAINT_NAME",
                enabled_fk_filter
            )
        } else {
            format!(
                "SELECT OWNER || CHR(31) || TABLE_NAME || CHR(31) || CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE {} AND {} ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME",
                Self::eq_original_or_upper_sql("OWNER", schema),
                enabled_fk_filter
            )
        }
    }

    fn parse_foreign_key_constraints_metadata(
        fallback_schema: &str,
        metadata: &str,
    ) -> Vec<DamengForeignKeyConstraintInfo> {
        metadata
            .split('\x1e')
            .filter(|row| !row.is_empty())
            .filter_map(|row| {
                let parts = row.split('\x1f').collect::<Vec<_>>();
                let (schema, table_name, constraint_name) = match parts.as_slice() {
                    [table_name, constraint_name] => {
                        (fallback_schema, *table_name, *constraint_name)
                    }
                    [schema, table_name, constraint_name] => {
                        (*schema, *table_name, *constraint_name)
                    }
                    _ => return None,
                };
                if table_name.is_empty() || constraint_name.is_empty() {
                    return None;
                }
                Some(DamengForeignKeyConstraintInfo {
                    schema: schema.to_string(),
                    table_name: table_name.to_string(),
                    constraint_name: constraint_name.to_string(),
                })
            })
            .collect()
    }

    fn disable_constraint_sql(schema: &str, table_name: &str, constraint_name: &str) -> String {
        format!(
            "ALTER TABLE {} DISABLE CONSTRAINT {}",
            Self::qualified_table(schema, table_name),
            Self::quote_ident(constraint_name)
        )
    }

    fn enable_constraint_sql(schema: &str, table_name: &str, constraint_name: &str) -> String {
        format!(
            "ALTER TABLE {} ENABLE CONSTRAINT {}",
            Self::qualified_table(schema, table_name),
            Self::quote_ident(constraint_name)
        )
    }

    async fn try_execute_optional(&self, sql: &str) -> Result<u64, String> {
        self.client
            .lock()
            .await
            .execute(sql)
            .map_err(|e| e.to_string())
    }

    fn qualified_column(schema: &str, table_name: &str, column_name: &str) -> String {
        format!(
            "{}.{}",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(column_name)
        )
    }

    fn comment_on_column_sql(
        schema: &str,
        table_name: &str,
        column_name: &str,
        comment: &str,
    ) -> String {
        format!(
            "COMMENT ON COLUMN {} IS {}",
            Self::qualified_column(schema, table_name, column_name),
            Self::quote_literal(comment)
        )
    }

    fn comment_on_table_sql(schema: &str, table_name: &str, comment: &str) -> String {
        format!(
            "COMMENT ON TABLE {} IS {}",
            Self::qualified_table(schema, table_name),
            Self::quote_literal(comment)
        )
    }

    fn value_to_param(value: &Value) -> DamengParam {
        match value {
            Value::None => DamengParam::Null,
            Value::Int8(v) => DamengParam::I8(*v),
            Value::Int16(v) => DamengParam::I16(*v),
            Value::Int32(v) => DamengParam::I32(*v),
            Value::Int64(v) => DamengParam::I64(*v),
            Value::UnsignedInt8(v) => DamengParam::I16(*v as i16),
            Value::UnsignedInt16(v) => DamengParam::I32(*v as i32),
            Value::UnsignedInt32(v) => DamengParam::I64(*v as i64),
            Value::UnsignedInt64(v) => {
                if *v <= i64::MAX as u64 {
                    DamengParam::I64(*v as i64)
                } else {
                    DamengParam::Text(v.to_string())
                }
            }
            Value::Float(v) => DamengParam::F32(*v),
            Value::Double(v) => DamengParam::F64(*v),
            Value::Bit(v) => DamengParam::I64(*v as i64),
            Value::Decimal(v)
            | Value::Time(v)
            | Value::Date(v)
            | Value::DateTime(v)
            | Value::String(v)
            | Value::Json(v) => DamengParam::Text(v.clone()),
            Value::Blob(v) => DamengParam::Bytes(v.clone()),
            Value::Timestamp(_) | Value::Year(_) => DamengParam::Text(value.resolve_string()),
        }
    }

    fn dameng_column_definition(
        mysql_type_token: &str,
        mysql_column_definition: &str,
        nullable_sql: &str,
        auto_increment: bool,
    ) -> String {
        let mut parts = vec![Self::map_mysql_type_to_dameng_for_column(
            mysql_type_token,
            auto_increment,
        )];
        if !nullable_sql.trim().is_empty() {
            parts.push(nullable_sql.to_string());
        }
        if let Some(default_sql) =
            Self::mysql_column_default_sql(mysql_type_token, mysql_column_definition)
        {
            parts.push(default_sql);
        }
        parts.join(" ")
    }

    #[cfg(test)]
    fn dameng_modify_column_definition(mysql_type_token: &str) -> String {
        Self::dameng_modify_column_definition_with_nullable(mysql_type_token, "")
    }

    fn dameng_modify_column_definition_with_nullable(
        mysql_type_token: &str,
        nullable_sql: &str,
    ) -> String {
        let mut parts = vec![Self::map_mysql_type_to_dameng_for_column(
            mysql_type_token,
            false,
        )];
        if !nullable_sql.trim().is_empty() {
            parts.push(nullable_sql.to_string());
        }
        parts.join(" ")
    }

    fn mysql_column_default_sql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
    ) -> Option<String> {
        let default_value = Self::mysql_default_value_expression(mysql_column_definition)?;
        if Self::mysql_default_is_null(default_value.as_str()) {
            return None;
        }

        let t = mysql_type_token.to_ascii_lowercase();
        if (t.starts_with("timestamp") || t.starts_with("datetime"))
            && Self::mysql_default_is_current_timestamp(default_value.as_str())
        {
            return Some("DEFAULT CURRENT_TIMESTAMP".to_string());
        }

        let dameng_type = Self::map_mysql_type_to_dameng(mysql_type_token);
        if dameng_type.eq_ignore_ascii_case("CLOB") || dameng_type.eq_ignore_ascii_case("BLOB") {
            return None;
        }

        let default_literal =
            Self::mysql_default_literal_sql(mysql_type_token, default_value.as_str())?;
        Some(format!("DEFAULT {}", default_literal))
    }

    fn mysql_column_comments(table_info: &TableInfoVo) -> HashMap<String, String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        let mut comments = HashMap::new();
        for src_col in &table_info.columns {
            let key = src_col.to_ascii_lowercase();
            let Some(def) = defs.get(key.as_str()) else {
                continue;
            };
            let Some(comment) = Self::mysql_column_comment(def.as_str()) else {
                continue;
            };
            comments.insert(
                Self::target_column_name(src_col).to_ascii_lowercase(),
                comment,
            );
        }
        comments
    }

    fn mysql_column_comment(mysql_column_definition: &str) -> Option<String> {
        let comment_pos = Self::find_keyword_outside_quotes(mysql_column_definition, 0, "comment")?;
        let rest = mysql_column_definition[comment_pos + "comment".len()..].trim_start();
        Self::mysql_string_literal_content(rest)
    }

    fn mysql_current_timestamp_default_sql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
    ) -> Option<&'static str> {
        let t = mysql_type_token.to_ascii_lowercase();
        if !t.starts_with("timestamp") && !t.starts_with("datetime") {
            return None;
        }
        let default_value = Self::mysql_default_value_expression(mysql_column_definition)?;
        if Self::mysql_default_is_current_timestamp(default_value.as_str()) {
            Some("DEFAULT CURRENT_TIMESTAMP")
        } else {
            None
        }
    }

    fn mysql_default_value_expression(definition: &str) -> Option<String> {
        let default_pos = Self::find_keyword_outside_quotes(definition, 0, "default")?;
        let rest = definition[default_pos + "default".len()..].trim_start();
        Self::take_mysql_default_expression(rest).map(ToString::to_string)
    }

    fn take_mysql_default_expression(rest: &str) -> Option<&str> {
        let rest = rest.trim_start();
        if rest.is_empty() {
            return None;
        }

        let bytes = rest.as_bytes();
        let mut quote: Option<u8> = None;
        let mut paren_depth = 0u32;
        let mut pos = 0usize;
        let mut end = bytes.len();

        while pos < bytes.len() {
            let byte = bytes[pos];
            if let Some(quote_byte) = quote {
                if byte == b'\\' {
                    pos = (pos + 2).min(bytes.len());
                    continue;
                }
                if byte == quote_byte {
                    if bytes.get(pos + 1) == Some(&quote_byte) {
                        pos += 2;
                    } else {
                        quote = None;
                        pos += 1;
                    }
                } else {
                    pos += 1;
                }
                continue;
            }

            match byte {
                b'\'' | b'"' | b'`' => {
                    quote = Some(byte);
                    pos += 1;
                }
                b'(' => {
                    paren_depth += 1;
                    pos += 1;
                }
                b')' => {
                    paren_depth = paren_depth.saturating_sub(1);
                    pos += 1;
                }
                b if b.is_ascii_whitespace() && paren_depth == 0 => {
                    let next = Self::skip_ascii_whitespace(rest, pos);
                    if next < bytes.len() && Self::is_mysql_default_tail_keyword(rest, next) {
                        end = pos;
                        break;
                    }
                    pos += 1;
                }
                _ => pos += 1,
            }
        }

        let value = rest[..end].trim_end();
        if value.is_empty() { None } else { Some(value) }
    }

    fn skip_ascii_whitespace(value: &str, start: usize) -> usize {
        let bytes = value.as_bytes();
        let mut pos = start;
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        pos
    }

    fn is_mysql_default_tail_keyword(value: &str, pos: usize) -> bool {
        const TAIL_KEYWORDS: &[&str] = &[
            "comment",
            "on",
            "not",
            "null",
            "auto_increment",
            "primary",
            "unique",
            "collate",
            "character",
            "charset",
            "check",
            "constraint",
            "references",
            "visible",
            "invisible",
        ];
        TAIL_KEYWORDS
            .iter()
            .any(|keyword| Self::starts_with_keyword(value, pos, keyword))
    }

    fn mysql_default_is_null(default_value: &str) -> bool {
        default_value.trim().eq_ignore_ascii_case("null")
    }

    fn mysql_default_is_current_timestamp(default_value: &str) -> bool {
        let default_lower = default_value.trim().to_ascii_lowercase();
        let Some(tail) = default_lower.strip_prefix("current_timestamp") else {
            return false;
        };
        Self::current_timestamp_default_tail_ok(tail)
    }

    fn current_timestamp_default_tail_ok(tail: &str) -> bool {
        if tail.is_empty() {
            return true;
        }
        if tail.chars().next().is_some_and(char::is_whitespace) {
            return true;
        }
        if let Some(rest) = tail.strip_prefix('(') {
            let Some(end) = rest.find(')') else {
                return false;
            };
            return rest[..end].chars().all(|c| c.is_ascii_digit());
        }
        !tail
            .as_bytes()
            .first()
            .is_some_and(|byte| Self::is_ascii_identifier_byte(*byte))
    }

    fn mysql_default_literal_sql(mysql_type_token: &str, default_value: &str) -> Option<String> {
        let t = mysql_type_token.to_ascii_lowercase();
        if Self::mysql_numeric_or_boolean_type(t.as_str()) {
            return Self::mysql_numeric_default_literal_sql(default_value);
        }
        if t.starts_with("varchar") || t.starts_with("char") {
            let value = Self::mysql_string_literal_content(default_value)?;
            return Some(Self::quote_literal(value.as_str()));
        }
        None
    }

    fn mysql_numeric_or_boolean_type(mysql_type_token: &str) -> bool {
        mysql_type_token.starts_with("tinyint")
            || mysql_type_token.starts_with("smallint")
            || mysql_type_token.starts_with("mediumint")
            || mysql_type_token.starts_with("int")
            || mysql_type_token.starts_with("integer")
            || mysql_type_token.starts_with("bigint")
            || mysql_type_token.starts_with("float")
            || mysql_type_token.starts_with("double")
            || mysql_type_token.starts_with("real")
            || mysql_type_token.starts_with("decimal")
            || mysql_type_token.starts_with("numeric")
            || mysql_type_token.starts_with("bit")
            || mysql_type_token.starts_with("bool")
            || mysql_type_token.starts_with("boolean")
    }

    fn mysql_numeric_default_literal_sql(default_value: &str) -> Option<String> {
        let value = default_value.trim();
        if let Some(bit_value) = Self::mysql_bit_literal_to_decimal(value) {
            return Some(bit_value);
        }

        let unquoted =
            Self::mysql_string_literal_content(value).unwrap_or_else(|| value.to_string());
        let normalized = unquoted.trim();
        if normalized.eq_ignore_ascii_case("true") {
            return Some("1".to_string());
        }
        if normalized.eq_ignore_ascii_case("false") {
            return Some("0".to_string());
        }
        if Self::is_numeric_literal(normalized) {
            return Some(normalized.to_string());
        }
        None
    }

    fn mysql_bit_literal_to_decimal(value: &str) -> Option<String> {
        let value = value.trim();
        if value.len() < 4 || !value[..2].eq_ignore_ascii_case("b'") || !value.ends_with('\'') {
            return None;
        }
        let bits = &value[2..value.len() - 1];
        if bits.is_empty() || !bits.bytes().all(|b| matches!(b, b'0' | b'1')) {
            return None;
        }
        u128::from_str_radix(bits, 2).ok().map(|v| v.to_string())
    }

    fn is_numeric_literal(value: &str) -> bool {
        let value = value.trim();
        if value.is_empty() {
            return false;
        }
        let mut has_digit = false;
        for (idx, byte) in value.bytes().enumerate() {
            if byte.is_ascii_digit() {
                has_digit = true;
                continue;
            }
            match byte {
                b'+' | b'-' if idx == 0 => {}
                b'+' | b'-'
                    if matches!(value.as_bytes().get(idx.wrapping_sub(1)), Some(b'e' | b'E')) => {}
                b'.' | b'e' | b'E' => {}
                _ => return false,
            }
        }
        has_digit && value.parse::<f64>().is_ok()
    }

    fn mysql_string_literal_content(default_value: &str) -> Option<String> {
        let value = default_value.trim();
        let bytes = value.as_bytes();
        let quote_pos = if matches!(bytes.first(), Some(b'\'' | b'"')) {
            0
        } else if bytes.len() >= 2
            && matches!(bytes[0], b'n' | b'N')
            && matches!(bytes[1], b'\'' | b'"')
        {
            1
        } else if bytes.first() == Some(&b'_') {
            value.find(['\'', '"']).filter(|idx| {
                value[..*idx]
                    .bytes()
                    .all(|b| b == b'_' || b.is_ascii_alphanumeric())
            })?
        } else {
            return None;
        };

        let quote = bytes[quote_pos];
        let mut result = Vec::new();
        let mut pos = quote_pos + 1;
        while pos < bytes.len() {
            let byte = bytes[pos];
            if byte == b'\\' {
                let next = bytes.get(pos + 1)?;
                result.push(*next);
                pos += 2;
                continue;
            }
            if byte == quote {
                if bytes.get(pos + 1) == Some(&quote) {
                    result.push(quote);
                    pos += 2;
                    continue;
                }
                if value[pos + 1..].trim().is_empty() {
                    return Some(String::from_utf8_lossy(result.as_slice()).into_owned());
                }
                return None;
            }
            result.push(byte);
            pos += 1;
        }
        None
    }

    fn dameng_default_is_current_timestamp(default_value: Option<&str>) -> bool {
        let Some(default_value) = default_value else {
            return false;
        };
        let normalized = default_value
            .trim()
            .trim_matches(|c| c == '(' || c == ')')
            .to_ascii_uppercase();
        normalized == "CURRENT_TIMESTAMP" || normalized.starts_with("CURRENT_TIMESTAMP(")
    }

    fn dameng_default_matches_mysql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
        default_value: Option<&str>,
    ) -> bool {
        let Some(expected_default) =
            Self::mysql_column_default_value_sql(mysql_type_token, mysql_column_definition)
        else {
            return true;
        };
        let Some(default_value) = default_value else {
            return false;
        };
        Self::normalize_default_for_compare(expected_default.as_str())
            == Self::normalize_default_for_compare(default_value)
    }

    fn existing_column_default_mismatch(
        mysql_type_token: &str,
        mysql_column_definition: &str,
        existing_col: &DamengColumnInfo,
    ) -> bool {
        !Self::dameng_default_matches_mysql(
            mysql_type_token,
            mysql_column_definition,
            existing_col.default_value.as_deref(),
        ) || (Self::mysql_current_timestamp_default_sql(mysql_type_token, mysql_column_definition)
            .is_some()
            && !Self::dameng_default_is_current_timestamp(existing_col.default_value.as_deref()))
    }

    fn mysql_column_default_value_sql(
        mysql_type_token: &str,
        mysql_column_definition: &str,
    ) -> Option<String> {
        Self::mysql_column_default_sql(mysql_type_token, mysql_column_definition).and_then(|sql| {
            sql.trim_start()
                .strip_prefix("DEFAULT")
                .map(|value| value.trim_start().to_string())
        })
    }

    fn normalize_default_for_compare(default_value: &str) -> String {
        let value = default_value.trim();
        let value = if Self::starts_with_keyword(value, 0, "default") {
            value["default".len()..].trim_start()
        } else {
            value
        };
        if Self::mysql_default_is_current_timestamp(value) {
            return "CURRENT_TIMESTAMP".to_string();
        }
        let value = if value.starts_with('(') && value.ends_with(')') && value.len() > 2 {
            value[1..value.len() - 1].trim()
        } else {
            value
        };
        if Self::mysql_default_is_current_timestamp(value) {
            return "CURRENT_TIMESTAMP".to_string();
        }
        if let Some(string_value) = Self::mysql_string_literal_content(value) {
            return Self::quote_literal(string_value.as_str());
        }
        if Self::is_numeric_literal(value) {
            return value.to_string();
        }
        value.to_ascii_uppercase()
    }

    fn starts_with_keyword(value: &str, pos: usize, keyword: &str) -> bool {
        let bytes = value.as_bytes();
        let keyword_bytes = keyword.as_bytes();
        if pos + keyword_bytes.len() > bytes.len() {
            return false;
        }
        if !bytes[pos..pos + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes) {
            return false;
        }
        let before_ok = pos == 0 || !Self::is_ascii_identifier_byte(bytes[pos - 1]);
        let after_pos = pos + keyword_bytes.len();
        let after_ok =
            after_pos >= bytes.len() || !Self::is_ascii_identifier_byte(bytes[after_pos]);
        before_ok && after_ok
    }

    fn find_keyword_outside_quotes(value: &str, start: usize, keyword: &str) -> Option<usize> {
        let bytes = value.as_bytes();
        let mut quote = None;
        let mut pos = start;
        while pos < bytes.len() {
            let byte = bytes[pos];
            if let Some(quote_byte) = quote {
                if byte == quote_byte {
                    if bytes.get(pos + 1) == Some(&quote_byte) {
                        pos += 2;
                    } else {
                        quote = None;
                        pos += 1;
                    }
                } else {
                    pos += 1;
                }
                continue;
            }
            if matches!(byte, b'`' | b'\'' | b'"') {
                quote = Some(byte);
                pos += 1;
                continue;
            }
            if Self::starts_with_keyword(value, pos, keyword) {
                return Some(pos);
            }
            pos += 1;
        }
        None
    }

    fn is_ascii_identifier_byte(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_'
    }

    fn map_mysql_type_to_dameng(mysql_type_token: &str) -> String {
        Self::map_mysql_type_to_dameng_for_column(mysql_type_token, false)
    }

    fn map_mysql_type_to_dameng_for_column(mysql_type_token: &str, auto_increment: bool) -> String {
        let t = mysql_type_token.to_ascii_lowercase();
        let dameng_type = if t.starts_with("tinyint") {
            if t.contains("unsigned") {
                "SMALLINT".to_string()
            } else {
                "TINYINT".to_string()
            }
        } else if t.starts_with("bool") || t.starts_with("boolean") {
            "TINYINT".to_string()
        } else if t.starts_with("bit") {
            "BIGINT".to_string()
        } else if t.starts_with("smallint") {
            if t.contains("unsigned") {
                "INT".to_string()
            } else {
                "SMALLINT".to_string()
            }
        } else if t.starts_with("mediumint") || t.starts_with("int") || t.starts_with("integer") {
            if t.contains("unsigned") {
                "BIGINT".to_string()
            } else {
                "INT".to_string()
            }
        } else if t.starts_with("bigint") {
            if t.contains("unsigned") {
                "DECIMAL(20,0)".to_string()
            } else {
                "BIGINT".to_string()
            }
        } else if t.starts_with("float") {
            "FLOAT".to_string()
        } else if t.starts_with("double") || t.starts_with("real") {
            "DOUBLE".to_string()
        } else if t.starts_with("decimal") || t.starts_with("numeric") {
            Self::map_mysql_decimal_type_to_dameng(mysql_type_token)
        } else if t.starts_with("datetime") || t.starts_with("timestamp") {
            "TIMESTAMP".to_string()
        } else if t.starts_with("date") {
            "DATE".to_string()
        } else if t.starts_with("time") {
            "TIME".to_string()
        } else if t.starts_with("varchar") {
            Self::map_mysql_char_type_to_dameng(
                "VARCHAR",
                mysql_type_token,
                DAMENG_INLINE_STRING_CHAR_LIMIT,
            )
        } else if t.starts_with("char") {
            Self::map_mysql_char_type_to_dameng(
                "CHAR",
                mysql_type_token,
                DAMENG_INLINE_STRING_CHAR_LIMIT,
            )
        } else if t.contains("text")
            || t.starts_with("json")
            || t.starts_with("enum")
            || t.starts_with("set")
        {
            "CLOB".to_string()
        } else if t.contains("blob") || t.contains("binary") {
            "BLOB".to_string()
        } else {
            "VARCHAR(255 CHAR)".to_string()
        };

        if auto_increment && Self::dameng_type_supports_identity(dameng_type.as_str()) {
            format!("{} IDENTITY(1,1)", dameng_type)
        } else {
            dameng_type
        }
    }

    fn dameng_type_supports_identity(dameng_type: &str) -> bool {
        let t = dameng_type.to_ascii_uppercase();
        t == "TINYINT"
            || t == "SMALLINT"
            || t == "INT"
            || t == "BIGINT"
            || t.starts_with("DECIMAL")
            || t.starts_with("NUMERIC")
    }

    fn source_blob_column_keys(table_info: &TableInfoVo) -> HashSet<String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        table_info
            .columns
            .iter()
            .filter_map(|column| {
                let key = column.to_ascii_lowercase();
                let def = defs.get(key.as_str())?;
                let mysql_type = mysql_type_token_from_column_definition(def.as_str())?;
                let dameng_type = Self::map_mysql_type_to_dameng(mysql_type.as_str());
                if dameng_type.eq_ignore_ascii_case("BLOB") {
                    Some(key)
                } else {
                    None
                }
            })
            .collect()
    }

    fn source_blob_column_keys_for_records(
        table_info: &TableInfoVo,
        columns: &[String],
        records: &[DataBuffer],
    ) -> HashSet<String> {
        let mut keys = Self::source_blob_column_keys(table_info);
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        for column in columns {
            let key = column.to_ascii_lowercase();
            if defs.contains_key(key.as_str()) {
                continue;
            }
            if records
                .iter()
                .any(|record| matches!(record.after.get(column), Value::Blob(_)))
            {
                keys.insert(key);
            }
        }
        keys
    }

    fn source_clob_column_keys(table_info: &TableInfoVo) -> HashSet<String> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        table_info
            .columns
            .iter()
            .filter_map(|column| {
                let key = column.to_ascii_lowercase();
                let def = defs.get(key.as_str())?;
                let mysql_type = mysql_type_token_from_column_definition(def.as_str())?;
                let dameng_type = Self::map_mysql_type_to_dameng(mysql_type.as_str());
                if dameng_type.eq_ignore_ascii_case("CLOB") {
                    Some(key)
                } else {
                    None
                }
            })
            .collect()
    }

    fn source_clob_column_keys_for_records(
        table_info: &TableInfoVo,
        columns: &[String],
        records: &[DataBuffer],
    ) -> HashSet<String> {
        let mut keys = Self::source_clob_column_keys(table_info);
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        for column in columns {
            let key = column.to_ascii_lowercase();
            if defs.contains_key(key.as_str()) {
                continue;
            }
            if records
                .iter()
                .any(|record| matches!(record.after.get(column), Value::String(_) | Value::Json(_)))
            {
                keys.insert(key);
            }
        }
        keys
    }

    fn is_blob_column_key(blob_column_keys: &HashSet<String>, column_name: &str) -> bool {
        blob_column_keys.contains(column_name.to_ascii_lowercase().as_str())
    }

    fn is_clob_column_key(clob_column_keys: &HashSet<String>, column_name: &str) -> bool {
        clob_column_keys.contains(column_name.to_ascii_lowercase().as_str())
    }

    #[cfg(test)]
    fn dml_value_sql(table_info: &TableInfoVo, column_name: &str, value: &Value) -> &'static str {
        let blob_column_keys = Self::source_blob_column_keys(table_info);
        Self::dml_value_sql_with_blob_columns(&blob_column_keys, column_name, value)
    }

    fn dml_value_sql_with_blob_columns(
        blob_column_keys: &HashSet<String>,
        column_name: &str,
        value: &Value,
    ) -> &'static str {
        if Self::is_blob_column_key(blob_column_keys, column_name)
            && matches!(value, Value::Blob(_) | Value::String(_) | Value::Json(_))
        {
            "EMPTY_BLOB()"
        } else {
            "?"
        }
    }

    #[cfg(test)]
    fn dml_value_param(
        table_info: &TableInfoVo,
        column_name: &str,
        value: &Value,
    ) -> Option<DamengParam> {
        let blob_column_keys = Self::source_blob_column_keys(table_info);
        Self::dml_value_param_with_blob_columns(&blob_column_keys, column_name, value)
    }

    fn dml_value_param_with_blob_columns(
        blob_column_keys: &HashSet<String>,
        column_name: &str,
        value: &Value,
    ) -> Option<DamengParam> {
        if Self::is_blob_column_key(blob_column_keys, column_name)
            && matches!(value, Value::Blob(_) | Value::String(_) | Value::Json(_))
        {
            None
        } else if let Value::Blob(v) = value {
            Some(DamengParam::Text(String::from_utf8_lossy(v).to_string()))
        } else {
            Some(Self::value_to_param(value))
        }
    }

    fn bytes_to_hex(value: &[u8]) -> String {
        value.iter().map(|b| format!("{:02X}", b)).collect()
    }

    fn string_chunks(value: &str, chunk_chars: usize) -> Vec<String> {
        let chunk_chars = chunk_chars.max(1);
        let mut chunks = Vec::new();
        let mut current = String::new();
        let mut current_len = 0usize;
        for ch in value.chars() {
            current.push(ch);
            current_len += 1;
            if current_len >= chunk_chars {
                chunks.push(std::mem::take(&mut current));
                current_len = 0;
            }
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    fn reset_blob_sql(schema: &str, table_name: &str, pk_name: &str, column_name: &str) -> String {
        format!(
            "UPDATE {} SET {} = EMPTY_BLOB() WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(column_name),
            Self::quote_column_ident(pk_name)
        )
    }

    fn append_blob_chunk_sql(
        schema: &str,
        table_name: &str,
        pk_name: &str,
        column_name: &str,
        chunk_len: usize,
    ) -> String {
        format!(
            "DECLARE\n  v_lob BLOB;\nBEGIN\n  SELECT {} INTO v_lob FROM {} WHERE {} = ? FOR UPDATE;\n  DBMS_LOB.WRITEAPPEND(v_lob, {}, HEXTORAW(?));\nEND;",
            Self::quote_column_ident(column_name),
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name),
            chunk_len
        )
    }

    fn reset_clob_sql(schema: &str, table_name: &str, pk_name: &str, column_name: &str) -> String {
        format!(
            "UPDATE {} SET {} = EMPTY_CLOB() WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(column_name),
            Self::quote_column_ident(pk_name)
        )
    }

    fn append_clob_chunk_sql(
        schema: &str,
        table_name: &str,
        pk_name: &str,
        column_name: &str,
        chunk_len: usize,
    ) -> String {
        format!(
            "DECLARE\n  v_lob CLOB;\nBEGIN\n  SELECT {} INTO v_lob FROM {} WHERE {} = ? FOR UPDATE;\n  DBMS_LOB.WRITEAPPEND(v_lob, {}, ?);\nEND;",
            Self::quote_column_ident(column_name),
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name),
            chunk_len
        )
    }

    fn validate_merged_target_schema(
        table_info_list: &[TableInfoVo],
        fallback_schema: &str,
    ) -> Result<(), String> {
        let mut signatures: HashMap<String, (String, Vec<(String, String)>)> = HashMap::new();
        for table_info in table_info_list {
            let schema = Self::table_info_target_schema(table_info, fallback_schema);
            let target_key =
                Self::target_table_key(schema.as_str(), table_info.table_name.as_str())
                    .to_ascii_lowercase();
            let signature = Self::table_schema_signature(table_info);
            match signatures.get(&target_key) {
                None => {
                    signatures.insert(
                        target_key,
                        (Self::table_primary_key_signature(table_info), signature),
                    );
                }
                Some((pk, existing_signature)) => {
                    if pk != &Self::table_primary_key_signature(table_info)
                        || existing_signature != &signature
                    {
                        return Err(format!(
                            "Dameng multi_mode target table schema mismatch: {}",
                            Self::target_table_key(schema.as_str(), table_info.table_name.as_str())
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn table_primary_key_signature(table_info: &TableInfoVo) -> String {
        Self::source_primary_key_columns(table_info)
            .iter()
            .map(|column| Self::target_column_name(column).to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn validate_target_column_names(
        table_info_list: &[TableInfoVo],
        fallback_schema: &str,
    ) -> Result<(), String> {
        for table_info in table_info_list {
            let schema = Self::table_info_target_schema(table_info, fallback_schema);
            let table_key = Self::target_table_key(schema.as_str(), table_info.table_name.as_str());
            let mut seen: HashMap<String, (String, String)> = HashMap::new();
            for source_column in &table_info.columns {
                let target_column = Self::target_column_name(source_column);
                let target_key = target_column.to_ascii_lowercase();
                if let Some((existing_source, existing_target)) =
                    seen.insert(target_key, (source_column.clone(), target_column.clone()))
                {
                    return Err(format!(
                        "Dameng target column name conflict: {} source columns {} and {} both map to target column {}",
                        table_key, existing_source, source_column, existing_target
                    ));
                }
            }
        }
        Ok(())
    }

    fn table_schema_signature(table_info: &TableInfoVo) -> Vec<(String, String)> {
        let defs =
            extract_mysql_create_table_column_definitions(table_info.create_table_sql.as_str());
        table_info
            .columns
            .iter()
            .map(|column| {
                let key = column.to_ascii_lowercase();
                let type_token = defs
                    .get(&key)
                    .and_then(|def| {
                        let mysql_type = mysql_type_token_from_column_definition(def.as_str())?;
                        let auto_increment =
                            mysql_column_is_auto_increment_from_definition(def.as_str());
                        let nullable_sql = "";
                        Some(Self::dameng_column_definition(
                            mysql_type.as_str(),
                            def.as_str(),
                            nullable_sql,
                            auto_increment,
                        ))
                    })
                    .unwrap_or_else(|| "__missing_definition__".to_string())
                    .to_ascii_lowercase();
                (
                    Self::target_column_name(column).to_ascii_lowercase(),
                    type_token,
                )
            })
            .collect()
    }

    fn map_mysql_char_type_to_dameng(
        dameng_type: &str,
        mysql_type_token: &str,
        char_limit: u32,
    ) -> String {
        match Self::mysql_type_length(mysql_type_token) {
            Some(len) if len > char_limit => "CLOB".to_string(),
            Some(len) => format!("{}({} CHAR)", dameng_type, len.max(1)),
            None => format!("{}(255 CHAR)", dameng_type),
        }
    }

    fn map_mysql_decimal_type_to_dameng(mysql_type_token: &str) -> String {
        let Some((precision, scale)) = Self::mysql_decimal_precision_scale(mysql_type_token) else {
            return "DECIMAL".to_string();
        };
        let precision = precision.max(1);
        let scale = scale.min(precision);
        if precision <= DAMENG_DECIMAL_MAX_PRECISION {
            return format!("DECIMAL({},{})", precision, scale);
        }

        let scale = scale.min(DAMENG_DECIMAL_MAX_PRECISION);
        format!("DECIMAL({},{})", DAMENG_DECIMAL_MAX_PRECISION, scale)
    }

    fn mysql_decimal_precision_scale(mysql_type_token: &str) -> Option<(u32, u32)> {
        let start = mysql_type_token.find('(')? + 1;
        let end = mysql_type_token[start..].find(')')? + start;
        let mut parts = mysql_type_token[start..end].split(',');
        let precision = parts.next()?.trim().parse::<u32>().ok()?;
        let scale = parts
            .next()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0);
        Some((precision, scale))
    }

    fn mysql_type_length(mysql_type_token: &str) -> Option<u32> {
        let start = mysql_type_token.find('(')? + 1;
        let end = mysql_type_token[start..].find(')')? + start;
        mysql_type_token[start..end]
            .split(',')
            .next()?
            .trim()
            .parse::<u32>()
            .ok()
    }

    fn should_modify_existing_column(
        mysql_type_token: &str,
        _mysql_column_definition: &str,
        existing_col: &DamengColumnInfo,
    ) -> bool {
        let expected_type = Self::map_mysql_type_to_dameng(mysql_type_token);
        if existing_col.data_type.eq_ignore_ascii_case("CLOB")
            && Self::is_dameng_text_type(expected_type.as_str())
        {
            return false;
        }

        if expected_type == "CLOB" {
            return !existing_col.data_type.eq_ignore_ascii_case("CLOB");
        }

        if expected_type == "BLOB" {
            return !existing_col.data_type.eq_ignore_ascii_case("BLOB");
        }

        if Self::is_dameng_bit_type(existing_col.data_type.as_str())
            && Self::is_dameng_numeric_type(expected_type.as_str())
        {
            return true;
        }

        if let Some(expected_rank) = Self::dameng_integer_type_rank(expected_type.as_str()) {
            if let Some(existing_rank) =
                Self::dameng_integer_type_rank(existing_col.data_type.as_str())
            {
                return existing_rank < expected_rank;
            }
            if Self::is_dameng_decimal_type(existing_col.data_type.as_str()) {
                return false;
            }
            return true;
        }

        if Self::is_dameng_decimal_type(expected_type.as_str()) {
            if !Self::is_dameng_decimal_type(existing_col.data_type.as_str()) {
                return true;
            }
            if let Some((expected_precision, expected_scale)) =
                Self::dameng_decimal_precision_scale(expected_type.as_str())
            {
                if existing_col.data_scale.unwrap_or(0) < expected_scale {
                    return true;
                }
                if let Some(existing_precision) = existing_col.data_precision {
                    return existing_precision < expected_precision;
                }
            }
            return false;
        }

        if expected_type == "TIMESTAMP" {
            let existing_type = existing_col.data_type.to_ascii_uppercase();
            return !existing_type.contains("TIMESTAMP") && !existing_type.contains("DATETIME");
        }

        let t = mysql_type_token.to_ascii_lowercase();
        if !t.starts_with("varchar") && !t.starts_with("char") {
            return false;
        }

        let expected_prefix = if t.starts_with("varchar") {
            "VARCHAR"
        } else {
            "CHAR"
        };
        if !existing_col
            .data_type
            .to_ascii_uppercase()
            .starts_with(expected_prefix)
        {
            return true;
        }

        match Self::mysql_type_length(mysql_type_token) {
            Some(expected_chars) => {
                existing_col.char_length < expected_chars
                    || existing_col.data_length <= existing_col.char_length
            }
            None => existing_col.data_length <= existing_col.char_length,
        }
    }

    fn is_dameng_text_type(dameng_type: &str) -> bool {
        let normalized = dameng_type.to_ascii_uppercase();
        normalized == "CLOB" || normalized.starts_with("VARCHAR") || normalized.starts_with("CHAR")
    }

    fn is_dameng_bit_type(dameng_type: &str) -> bool {
        let normalized = dameng_type.to_ascii_uppercase();
        normalized == "BIT" || normalized.starts_with("BIT(")
    }

    fn is_dameng_numeric_type(dameng_type: &str) -> bool {
        let normalized = dameng_type.to_ascii_uppercase();
        normalized == "TINYINT"
            || normalized == "SMALLINT"
            || normalized == "INT"
            || normalized == "INTEGER"
            || normalized == "BIGINT"
            || normalized.starts_with("DECIMAL")
            || normalized.starts_with("NUMERIC")
    }

    fn dameng_integer_type_rank(dameng_type: &str) -> Option<u8> {
        match dameng_type.to_ascii_uppercase().as_str() {
            "TINYINT" => Some(1),
            "SMALLINT" => Some(2),
            "INT" | "INTEGER" => Some(3),
            "BIGINT" => Some(4),
            _ => None,
        }
    }

    fn is_dameng_decimal_type(dameng_type: &str) -> bool {
        let normalized = dameng_type.to_ascii_uppercase();
        normalized.starts_with("DECIMAL")
            || normalized.starts_with("NUMERIC")
            || normalized.starts_with("NUMBER")
    }

    fn dameng_decimal_precision_scale(dameng_type: &str) -> Option<(u32, u32)> {
        let start = dameng_type.find('(')? + 1;
        let end = dameng_type[start..].find(')')? + start;
        let mut parts = dameng_type[start..end].split(',');
        let precision = parts.next()?.trim().parse::<u32>().ok()?;
        let scale = parts
            .next()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0);
        Some((precision, scale))
    }
}

#[async_trait]
impl Sink for DamengSink {
    async fn connect(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.client.lock().await.query("SELECT 1 FROM DUAL")?;
        info!("Connected to Dameng");
        Ok(())
    }

    async fn write_record(
        &mut self,
        record: &DataBuffer,
        mysql_check_point_detail_entity: &Option<MysqlCheckPointDetailEntity>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = self.buffer.lock().await;
        buf.push(record.clone());
        if let Some(s) = mysql_check_point_detail_entity {
            self.checkpoint
                .lock()
                .await
                .insert(s.checkpoint_filepath.to_string(), s.clone());
        }
        if buf.len() >= self.sink_batch_size {
            drop(buf);
            self.flush_with_retry(&FlushByOperation::Signal).await;
        }
        Ok(())
    }

    async fn flush(&self, flush_by_operation: &FlushByOperation) -> Result<(), String> {
        let trigger = format!("{:?}", flush_by_operation);
        let _timer = SINK_FLUSH_DURATION_SECONDS
            .with_label_values(&["dameng", &trigger])
            .start_timer();

        if !*self.initialized.read().await {
            let mut inserted_tables = HashSet::new();
            for table_info in &self.table_info_list {
                let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
                let table_key =
                    Self::target_table_key(schema.as_str(), table_info.table_name.as_str());
                if !inserted_tables.insert(table_key.to_ascii_lowercase()) {
                    continue;
                }
                self.table_info_cache
                    .lock()
                    .await
                    .insert(table_key, table_info.clone());
            }
            let mut col_info = CaseInsensitiveHashMapVecString::new_with_no_arg();
            let mut inserted_columns = HashSet::new();
            for table_info in &self.table_info_list {
                let schema = Self::table_info_target_schema(table_info, self.schema.as_str());
                let table_key =
                    Self::target_table_key(schema.as_str(), table_info.table_name.as_str());
                if !inserted_columns.insert(table_key.to_ascii_lowercase()) {
                    continue;
                }
                let generated_columns = Self::generated_source_column_keys(table_info);
                for col in &table_info.columns {
                    if generated_columns.contains(col.to_ascii_lowercase().as_str()) {
                        continue;
                    }
                    col_info.entry_insert(table_key.as_str(), col.clone());
                }
            }
            *self.columns_cache.lock().await = col_info;
            *self.initialized.write().await = true;
        }

        let mut buf = self.buffer.lock().await;
        if buf.is_empty() {
            return Ok(());
        }
        match flush_by_operation {
            FlushByOperation::Timer => info!("Flushing Dameng Sink by timer... {}", buf.len()),
            FlushByOperation::Init => info!("Flushing Dameng Sink by init... {}", buf.len()),
            FlushByOperation::Signal => info!("Flushing Dameng Sink by signal... {}", buf.len()),
            FlushByOperation::Cdc => info!("Flushing Dameng Sink by cdc... {}", buf.len()),
        }

        let batch = std::mem::take(&mut *buf);
        drop(buf);

        let mut cache_for_roll_back = Vec::with_capacity(batch.len());
        let mut flush_result = Ok(());
        let mut index = 0;
        while index < batch.len() {
            let record = &batch[index];
            let schema = self.record_target_schema(record);
            let table_name = record.table_name.clone();
            let table_key = Self::target_table_key(schema.as_str(), table_name.as_str());
            let pk_name = self.get_pk_name_from_cache(table_key.as_str()).await;
            let base_columns = self.columns_cache.lock().await.get(table_key.as_str());
            let table_info = self.table_info_cache.lock().await.get(table_key.as_str());

            if matches!(&record.op, Operation::CREATE(true))
                && self.init_insert_batch_rows > 1
                && !self.random_check_data_after_init
            {
                let mut end = index + 1;
                while end < batch.len() && end - index < self.init_insert_batch_rows {
                    let next = &batch[end];
                    if !matches!(&next.op, Operation::CREATE(true)) {
                        break;
                    }
                    if next.table_name != table_name || self.record_target_schema(next) != schema {
                        break;
                    }
                    end += 1;
                }

                let records = &batch[index..end];
                let columns =
                    Self::merge_dml_columns_with_records(&table_info, &base_columns, records);
                for item in records {
                    cache_for_roll_back.push(item.clone());
                    SINK_EVENTS_TOTAL
                        .with_label_values(&["dameng", table_key.as_str(), "create"])
                        .inc();
                }
                let result = match self
                    .ensure_dml_columns(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        &columns,
                        records,
                        true,
                    )
                    .await
                {
                    Err(e) => Err(e),
                    Ok(required_defaults) => {
                        let (dml_columns, dml_records) = Self::apply_dml_required_defaults(
                            &columns,
                            records,
                            &required_defaults,
                        );
                        self.cache_dml_columns(table_key.as_str(), &dml_columns)
                            .await;
                        if dml_records.len() > 1 {
                            self.insert_or_update_records_batch(
                                schema.as_str(),
                                table_name.as_str(),
                                table_key.as_str(),
                                pk_name.as_str(),
                                &dml_columns,
                                &dml_records,
                            )
                            .await
                        } else {
                            self.insert_or_update_record(
                                schema.as_str(),
                                table_name.as_str(),
                                table_key.as_str(),
                                pk_name.as_str(),
                                &dml_columns,
                                &dml_records[0],
                            )
                            .await
                        }
                    }
                };

                if let Err(e) = result {
                    error!("Dameng flush error: {}", e);
                    SINK_FLUSH_ERRORS_TOTAL
                        .with_label_values(&["dameng", "write"])
                        .inc();
                    let mut buf = self.buffer.lock().await;
                    for cached_data_buffer in cache_for_roll_back {
                        buf.push(cached_data_buffer);
                    }
                    flush_result = Err(e);
                    break;
                }

                index = end;
                continue;
            }

            cache_for_roll_back.push(record.clone());
            let op_str = match &record.op {
                Operation::CREATE(_) => "create",
                Operation::UPDATE => "update",
                Operation::DELETE => "delete",
                _ => "other",
            };
            SINK_EVENTS_TOTAL
                .with_label_values(&["dameng", table_key.as_str(), op_str])
                .inc();

            let columns = Self::merge_dml_columns_with_records(
                &table_info,
                &base_columns,
                std::slice::from_ref(record),
            );
            let result = match &record.op {
                Operation::CREATE(true) => match self
                    .ensure_dml_columns(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        &columns,
                        std::slice::from_ref(record),
                        true,
                    )
                    .await
                {
                    Err(e) => Err(e),
                    Ok(required_defaults) => {
                        let (dml_columns, dml_records) = Self::apply_dml_required_defaults(
                            &columns,
                            std::slice::from_ref(record),
                            &required_defaults,
                        );
                        self.cache_dml_columns(table_key.as_str(), &dml_columns)
                            .await;
                        let write_result = if self.random_check_data_after_init {
                            self.replace_record_for_random_check_init(
                                schema.as_str(),
                                table_name.as_str(),
                                table_key.as_str(),
                                pk_name.as_str(),
                                &dml_columns,
                                &dml_records[0],
                            )
                            .await
                        } else {
                            self.insert_or_update_record(
                                schema.as_str(),
                                table_name.as_str(),
                                table_key.as_str(),
                                pk_name.as_str(),
                                &dml_columns,
                                &dml_records[0],
                            )
                            .await
                        };
                        let write_result =
                            if write_result.is_ok() && self.random_check_data_after_init {
                                self.ensure_random_check_target_row_exists(
                                    schema.as_str(),
                                    table_name.as_str(),
                                    table_key.as_str(),
                                    pk_name.as_str(),
                                    &dml_records[0],
                                )
                                .await
                            } else {
                                write_result
                            };
                        if write_result.is_ok() && self.random_check_data_after_init {
                            self.record_random_check_init_row(&dml_records[0]).await;
                        }
                        write_result
                    }
                },
                Operation::CREATE(false) => match self
                    .ensure_dml_columns(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        &columns,
                        std::slice::from_ref(record),
                        true,
                    )
                    .await
                {
                    Err(e) => Err(e),
                    Ok(required_defaults) => {
                        let (dml_columns, dml_records) = Self::apply_dml_required_defaults(
                            &columns,
                            std::slice::from_ref(record),
                            &required_defaults,
                        );
                        self.cache_dml_columns(table_key.as_str(), &dml_columns)
                            .await;
                        self.upsert_record(
                            schema.as_str(),
                            table_name.as_str(),
                            table_key.as_str(),
                            pk_name.as_str(),
                            &dml_columns,
                            &dml_records[0],
                        )
                        .await
                    }
                },
                Operation::UPDATE => {
                    let update_columns = Self::merge_dml_columns_with_records(
                        &table_info,
                        &[],
                        std::slice::from_ref(record),
                    );
                    if update_columns.is_empty() {
                        Ok(())
                    } else {
                        match self
                            .ensure_dml_columns(
                                schema.as_str(),
                                table_name.as_str(),
                                table_key.as_str(),
                                &update_columns,
                                std::slice::from_ref(record),
                                false,
                            )
                            .await
                        {
                            Err(e) => Err(e),
                            Ok(_) => {
                                self.cache_dml_columns(table_key.as_str(), &columns).await;
                                self.upsert_record(
                                    schema.as_str(),
                                    table_name.as_str(),
                                    table_key.as_str(),
                                    pk_name.as_str(),
                                    &update_columns,
                                    record,
                                )
                                .await
                            }
                        }
                    }
                }
                Operation::DELETE => {
                    self.delete_record(
                        schema.as_str(),
                        table_name.as_str(),
                        table_key.as_str(),
                        pk_name.as_str(),
                        record,
                    )
                    .await
                }
                _ => Err(format!("unexpected operation {:?}", &record.op)),
            };

            if let Err(e) = result {
                error!("Dameng flush error: {}", e);
                SINK_FLUSH_ERRORS_TOTAL
                    .with_label_values(&["dameng", "write"])
                    .inc();
                let mut buf = self.buffer.lock().await;
                for cached_data_buffer in cache_for_roll_back {
                    buf.push(cached_data_buffer);
                }
                flush_result = Err(e);
                break;
            }
            index += 1;
        }

        match (flush_result, self.disable_active_identity_insert().await) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), Ok(_)) => Err(e),
            (Ok(_), Err(identity_error)) => Err(identity_error),
            (Err(e), Err(identity_error)) => Err(format!(
                "{}; Dameng disable identity_insert failed: {}",
                e, identity_error
            )),
        }
    }

    async fn alter_flush(&mut self) -> Result<(), String> {
        let entries = {
            let checkpoint = self.checkpoint.lock().await;
            checkpoint
                .iter()
                .map(|(key, cp)| (key.clone(), cp.clone()))
                .collect::<Vec<_>>()
        };
        if entries.is_empty() {
            return Ok(());
        }
        self.checkpoint_manager.save_many(&entries).await?;
        self.checkpoint.lock().await.clear();
        trace!("Dameng alter flush done");
        Ok(())
    }

    async fn after_initialization(&mut self) -> Result<(), String> {
        self.flush_with_retry(&FlushByOperation::Init).await;
        if self.random_check_data_after_init {
            info!(
                "Dameng random data check init mode keeps foreign key constraints disabled and skips foreign key synchronization"
            );
            self.run_random_check_after_init_now().await;
        } else {
            self.ensure_secondary_indexes().await?;
            self.ensure_foreign_keys().await?;
            self.enable_foreign_key_constraints_after_init().await;
            self.spawn_random_check_after_init().await;
        }
        Ok(())
    }
}

impl DamengSink {
    async fn upsert_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_key));
        }
        let affected = self
            .update_record(schema, table_name, table_key, pk_name, columns, record)
            .await?;
        if affected > 0 {
            return Ok(());
        }

        self.insert_record(schema, table_name, table_key, pk_name, columns, record)
            .await
    }

    async fn insert_or_update_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_key));
        }
        match self
            .insert_record(schema, table_name, table_key, pk_name, columns, record)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_duplicate_key_error(e.as_str()) => {
                if !Self::has_non_pk_columns(columns, pk_name) {
                    warn!(
                        "Dameng insert duplicate ignored for primary-key-only table: table={} pk={} error={}",
                        table_key, pk_name, e
                    );
                    return Ok(());
                }
                let affected = self
                    .update_record(schema, table_name, table_key, pk_name, columns, record)
                    .await?;
                if affected > 0 {
                    Ok(())
                } else {
                    Err(format!(
                        "Dameng insert duplicate fallback update affected 0 rows: {}",
                        table_key
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn replace_record_for_random_check_init(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Err(format!(
                "Dameng random data check init cannot replace row because pk is empty: {}.{}",
                table_key, pk_name
            ));
        }
        self.delete_record(schema, table_name, table_key, pk_name, record)
            .await?;
        self.insert_record(schema, table_name, table_key, pk_name, columns, record)
            .await?;
        if Self::has_non_pk_columns(columns, pk_name) {
            let affected = self
                .update_record(schema, table_name, table_key, pk_name, columns, record)
                .await?;
            if affected == 0 {
                return Err(format!(
                    "Dameng random data check init replace inserted row but follow-up update affected 0 rows: {} pk={}",
                    table_key,
                    pk.resolve_string()
                ));
            }
        }
        Ok(())
    }

    async fn ensure_random_check_target_row_exists(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        record: &DataBuffer,
    ) -> Result<(), String> {
        if !self.random_check_data_after_init {
            return Ok(());
        }
        let pk = record.get_pk(pk_name);
        if matches!(pk, Value::None) {
            return Err(format!(
                "Dameng random data check init write cannot verify target row because pk is empty: {}.{}",
                table_key, pk_name
            ));
        }
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name)
        );
        let params = vec![Self::value_to_param(pk)];
        let rows = self.query(sql.as_str(), &params).await.map_err(|e| {
            format!(
                "Dameng random data check init write target row verification query failed: table={} pk={} value={} sql={} error={}",
                table_key,
                pk_name,
                pk.resolve_string(),
                sql,
                e
            )
        })?;
        let count = rows
            .first()
            .and_then(|row| {
                row.get::<i64>(0)
                    .ok()
                    .or_else(|| row.get::<i32>(0).ok().map(i64::from))
            })
            .unwrap_or(0);
        if count == 1 {
            return Ok(());
        }
        if count > 1 {
            return Err(format!(
                "Dameng random data check init write produced duplicate target rows: table={} pk={} value={} count={} sql={}",
                table_key,
                pk_name,
                pk.resolve_string(),
                count,
                sql
            ));
        }
        Err(format!(
            "Dameng random data check init write succeeded but target row is not visible by primary key: table={} pk={} value={} sql={}",
            table_key,
            pk_name,
            pk.resolve_string(),
            sql
        ))
    }

    async fn insert_or_update_records_batch(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        records: &[DataBuffer],
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }
        let upsert_first_cache_key = Self::init_upsert_first_cache_key(table_key);
        if self
            .init_upsert_first_tables
            .lock()
            .await
            .contains(upsert_first_cache_key.as_str())
        {
            return self
                .merge_or_update_then_insert_records_batch(
                    schema, table_name, table_key, pk_name, columns, records,
                )
                .await;
        }
        match self
            .insert_records_batch(schema, table_name, table_key, pk_name, columns, records)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_duplicate_key_error(e.as_str()) => {
                warn!(
                    "Dameng init batch insert hit duplicate key, switch table to update-first writes: table={} rows={} error={}",
                    table_key,
                    records.len(),
                    e
                );
                self.init_upsert_first_tables
                    .lock()
                    .await
                    .insert(upsert_first_cache_key);
                self.merge_or_update_then_insert_records_batch(
                    schema, table_name, table_key, pk_name, columns, records,
                )
                .await
            }
            Err(e) => {
                warn!(
                    "Dameng init batch insert failed, fallback to row writes: table={} rows={} error={}",
                    table_key,
                    records.len(),
                    e
                );
                for record in records {
                    self.insert_or_update_record(
                        schema, table_name, table_key, pk_name, columns, record,
                    )
                    .await?;
                }
                Ok(())
            }
        }
    }

    async fn merge_or_update_then_insert_records_batch(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        records: &[DataBuffer],
    ) -> Result<(), String> {
        match self
            .merge_records_batch(schema, table_name, table_key, pk_name, columns, records)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!(
                    "Dameng init batch MERGE failed, fallback to update-first row writes: table={} rows={} error={}",
                    table_key,
                    records.len(),
                    e
                );
                self.update_then_insert_records_batch(
                    schema, table_name, table_key, pk_name, columns, records,
                )
                .await
            }
        }
    }

    async fn update_then_insert_records_batch(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        records: &[DataBuffer],
    ) -> Result<(), String> {
        let mut insert_records = Vec::new();
        let mut updated_count = 0usize;
        for record in records {
            let affected = self
                .update_record(schema, table_name, table_key, pk_name, columns, record)
                .await?;
            if affected > 0 {
                updated_count += 1;
            } else {
                insert_records.push(record.clone());
            }
        }

        if insert_records.is_empty() {
            trace!(
                "Dameng init update-first batch updated all rows: table={} rows={}",
                table_key, updated_count
            );
            return Ok(());
        }

        match self
            .insert_records_batch(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &insert_records,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!(
                    "Dameng init update-first batch insert failed, fallback to row writes: table={} update_rows={} insert_rows={} error={}",
                    table_key,
                    updated_count,
                    insert_records.len(),
                    e
                );
                for record in insert_records {
                    self.insert_or_update_record(
                        schema, table_name, table_key, pk_name, columns, &record,
                    )
                    .await?;
                }
                Ok(())
            }
        }
    }

    fn init_upsert_first_cache_key(table_key: &str) -> String {
        table_key.to_ascii_lowercase()
    }

    fn has_non_pk_columns(columns: &[String], pk_name: &str) -> bool {
        columns.iter().any(|c| !c.eq_ignore_ascii_case(pk_name))
    }

    async fn merge_records_batch(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        records: &[DataBuffer],
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_key));
        }
        if records.is_empty() {
            return Ok(());
        }
        let table_info = self.table_info_cache.lock().await.get(table_key);
        let (merge_sql, merge_params) = Self::batch_merge_sql_and_params(
            schema,
            table_name,
            pk_name,
            columns,
            &table_info,
            records,
        )?;
        debug!(
            "Dameng batch MERGE: table={} rows={} sql={}",
            table_key,
            records.len(),
            merge_sql
        );

        let identity_insert_cache_key = Self::identity_insert_cache_key(table_key);
        let use_identity_insert = self
            .identity_insert_tables
            .lock()
            .await
            .contains(identity_insert_cache_key.as_str());
        if use_identity_insert {
            self.merge_batch_with_identity_insert(
                schema,
                table_name,
                table_key,
                merge_sql.as_str(),
                &merge_params,
            )
            .await?;
            for record in records {
                self.write_lob_columns(
                    schema,
                    table_name,
                    table_key,
                    pk_name,
                    columns,
                    &table_info,
                    record,
                )
                .await?;
            }
            return Ok(());
        }

        match self
            .execute_insert_with_params(merge_sql.as_str(), &merge_params)
            .await
        {
            Ok(_) => {}
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.identity_insert_tables
                    .lock()
                    .await
                    .insert(identity_insert_cache_key);
                self.merge_batch_with_identity_insert(
                    schema,
                    table_name,
                    table_key,
                    merge_sql.as_str(),
                    &merge_params,
                )
                .await?;
            }
            Err(e) => return Err(e),
        }
        for record in records {
            self.write_lob_columns(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &table_info,
                record,
            )
            .await?;
        }
        Ok(())
    }

    async fn merge_batch_with_identity_insert(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        merge_sql: &str,
        merge_params: &[DamengParam],
    ) -> Result<(), String> {
        self.ensure_identity_insert_enabled(schema, table_name, table_key)
            .await?;
        match self
            .execute_insert_with_params(merge_sql, merge_params)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.clear_active_identity_insert_state().await;
                self.ensure_identity_insert_enabled(schema, table_name, table_key)
                    .await?;
                self.execute_insert_with_params(merge_sql, merge_params)
                    .await
                    .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    fn batch_merge_sql_and_params(
        schema: &str,
        table_name: &str,
        pk_name: &str,
        columns: &[String],
        table_info: &TableInfoVo,
        records: &[DataBuffer],
    ) -> Result<(String, Vec<DamengParam>), String> {
        if !columns.iter().any(|c| c.eq_ignore_ascii_case(pk_name)) {
            return Err(format!(
                "pk column is missing from batch columns: {}",
                pk_name
            ));
        }
        let update_columns = columns
            .iter()
            .filter(|c| !c.eq_ignore_ascii_case(pk_name))
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            return Err(format!(
                "no non-primary-key columns for Dameng batch MERGE: {}.{}",
                schema, table_name
            ));
        }

        let mut params = Vec::new();
        let blob_column_keys =
            Self::source_blob_column_keys_for_records(table_info, columns, records);
        let using_sql = records
            .iter()
            .enumerate()
            .map(|(row_index, record)| {
                let expressions = columns
                    .iter()
                    .map(|col| {
                        let value_sql = Self::dml_value_sql_with_blob_columns(
                            &blob_column_keys,
                            col,
                            record.after.get(col),
                        );
                        if row_index == 0 {
                            format!("{} AS {}", value_sql, Self::quote_column_ident(col))
                        } else {
                            value_sql.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                params.extend(columns.iter().filter_map(|col| {
                    Self::dml_value_param_with_blob_columns(
                        &blob_column_keys,
                        col,
                        record.after.get(col),
                    )
                }));
                format!("SELECT {} FROM DUAL", expressions)
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let pk_sql = Self::quote_column_ident(pk_name);
        let update_sql = update_columns
            .iter()
            .map(|col| {
                let col_sql = Self::quote_column_ident(col);
                format!("t.{} = s.{}", col_sql, col_sql)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let insert_cols_sql = columns
            .iter()
            .map(|col| Self::quote_column_ident(col))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_values_sql = columns
            .iter()
            .map(|col| format!("s.{}", Self::quote_column_ident(col)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "MERGE INTO {} t USING ({}) s ON (t.{} = s.{}) WHEN MATCHED THEN UPDATE SET {} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            Self::qualified_table(schema, table_name),
            using_sql,
            pk_sql,
            pk_sql,
            update_sql,
            insert_cols_sql,
            insert_values_sql
        );
        Ok((sql, params))
    }

    async fn insert_records_batch(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        records: &[DataBuffer],
    ) -> Result<(), String> {
        if columns.is_empty() {
            return Err(format!("columns is empty: {}", table_key));
        }
        if records.is_empty() {
            return Ok(());
        }
        let table_info = self.table_info_cache.lock().await.get(table_key);
        let (insert_sql, insert_params) =
            Self::batch_insert_sql_and_params(schema, table_name, columns, &table_info, records);
        debug!(
            "Dameng batch INSERT: table={} rows={} sql={}",
            table_key,
            records.len(),
            insert_sql
        );

        let identity_insert_cache_key = Self::identity_insert_cache_key(table_key);
        let use_identity_insert = self
            .identity_insert_tables
            .lock()
            .await
            .contains(identity_insert_cache_key.as_str());
        if use_identity_insert {
            self.insert_batch_with_identity_insert(
                schema,
                table_name,
                table_key,
                insert_sql.as_str(),
                &insert_params,
            )
            .await?;
            for record in records {
                self.write_lob_columns(
                    schema,
                    table_name,
                    table_key,
                    pk_name,
                    columns,
                    &table_info,
                    record,
                )
                .await?;
            }
            return Ok(());
        }

        match self
            .execute_insert_with_params(insert_sql.as_str(), &insert_params)
            .await
        {
            Ok(_) => {}
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.identity_insert_tables
                    .lock()
                    .await
                    .insert(identity_insert_cache_key);
                self.insert_batch_with_identity_insert(
                    schema,
                    table_name,
                    table_key,
                    insert_sql.as_str(),
                    &insert_params,
                )
                .await?;
            }
            Err(e) => return Err(e),
        }
        for record in records {
            self.write_lob_columns(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &table_info,
                record,
            )
            .await?;
        }
        Ok(())
    }

    async fn insert_batch_with_identity_insert(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        insert_sql: &str,
        insert_params: &[DamengParam],
    ) -> Result<(), String> {
        self.ensure_identity_insert_enabled(schema, table_name, table_key)
            .await?;
        match self
            .execute_insert_with_params(insert_sql, insert_params)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.clear_active_identity_insert_state().await;
                self.ensure_identity_insert_enabled(schema, table_name, table_key)
                    .await?;
                self.execute_insert_with_params(insert_sql, insert_params)
                    .await
                    .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    fn batch_insert_sql_and_params(
        schema: &str,
        table_name: &str,
        columns: &[String],
        table_info: &TableInfoVo,
        records: &[DataBuffer],
    ) -> (String, Vec<DamengParam>) {
        let cols_sql = columns
            .iter()
            .map(|c| Self::quote_column_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let mut params = Vec::new();
        let blob_column_keys =
            Self::source_blob_column_keys_for_records(table_info, columns, records);
        let rows_sql = records
            .iter()
            .map(|record| {
                let placeholders = columns
                    .iter()
                    .map(|c| {
                        Self::dml_value_sql_with_blob_columns(
                            &blob_column_keys,
                            c,
                            record.after.get(c),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                params.extend(columns.iter().filter_map(|col| {
                    Self::dml_value_param_with_blob_columns(
                        &blob_column_keys,
                        col,
                        record.after.get(col),
                    )
                }));
                format!("({})", placeholders)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            Self::qualified_table(schema, table_name),
            cols_sql,
            rows_sql
        );
        (sql, params)
    }

    async fn update_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<u64, String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Err(format!("pk is empty: {}.{}", table_key, pk_name));
        }
        let table_info = self.table_info_cache.lock().await.get(table_key);
        let blob_column_keys = Self::source_blob_column_keys_for_records(
            &table_info,
            columns,
            std::slice::from_ref(record),
        );

        let update_columns = columns
            .iter()
            .filter(|c| !c.eq_ignore_ascii_case(pk_name))
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            return Ok(0);
        }

        let set_sql = update_columns
            .iter()
            .map(|c| {
                format!(
                    "{} = {}",
                    Self::quote_column_ident(c),
                    Self::dml_value_sql_with_blob_columns(
                        &blob_column_keys,
                        c,
                        record.after.get(c)
                    )
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let update_sql = format!(
            "UPDATE {} SET {} WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            set_sql,
            Self::quote_column_ident(pk_name)
        );
        let mut update_params = Vec::with_capacity(update_columns.len() + 1);
        for col in update_columns {
            if let Some(param) = Self::dml_value_param_with_blob_columns(
                &blob_column_keys,
                col,
                record.after.get(col),
            ) {
                update_params.push(param);
            }
        }
        update_params.push(Self::value_to_param(pk));
        debug!("Dameng UPDATE: {}", update_sql);
        let affected = self
            .execute_with_params(update_sql.as_str(), &update_params)
            .await?;
        if affected > 0 {
            self.write_lob_columns(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &table_info,
                record,
            )
            .await?;
        }
        Ok(affected)
    }

    async fn insert_record(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        record: &DataBuffer,
    ) -> Result<(), String> {
        let table_info = self.table_info_cache.lock().await.get(table_key);
        let blob_column_keys = Self::source_blob_column_keys_for_records(
            &table_info,
            columns,
            std::slice::from_ref(record),
        );
        let cols_sql = columns
            .iter()
            .map(|c| Self::quote_column_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = columns
            .iter()
            .map(|c| {
                Self::dml_value_sql_with_blob_columns(&blob_column_keys, c, record.after.get(c))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::qualified_table(schema, table_name),
            cols_sql,
            placeholders
        );
        let insert_params = columns
            .iter()
            .filter_map(|col| {
                Self::dml_value_param_with_blob_columns(
                    &blob_column_keys,
                    col,
                    record.after.get(col),
                )
            })
            .collect::<Vec<_>>();
        debug!("Dameng INSERT: {}", insert_sql);

        let identity_insert_cache_key = Self::identity_insert_cache_key(table_key);
        let use_identity_insert = self
            .identity_insert_tables
            .lock()
            .await
            .contains(identity_insert_cache_key.as_str());
        if use_identity_insert {
            self.insert_with_identity_insert(
                schema,
                table_name,
                table_key,
                insert_sql.as_str(),
                &insert_params,
            )
            .await?;
            self.write_lob_columns(
                schema,
                table_name,
                table_key,
                pk_name,
                columns,
                &table_info,
                record,
            )
            .await?;
            return Ok(());
        }

        match self
            .execute_insert_with_params(insert_sql.as_str(), &insert_params)
            .await
        {
            Ok(_) => {}
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.identity_insert_tables
                    .lock()
                    .await
                    .insert(identity_insert_cache_key);
                self.insert_with_identity_insert(
                    schema,
                    table_name,
                    table_key,
                    insert_sql.as_str(),
                    &insert_params,
                )
                .await?;
            }
            Err(e) => return Err(e),
        }
        self.write_lob_columns(
            schema,
            table_name,
            table_key,
            pk_name,
            columns,
            &table_info,
            record,
        )
        .await?;
        Ok(())
    }

    async fn insert_with_identity_insert(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        insert_sql: &str,
        insert_params: &[DamengParam],
    ) -> Result<(), String> {
        self.ensure_identity_insert_enabled(schema, table_name, table_key)
            .await?;
        match self
            .execute_insert_with_params(insert_sql, insert_params)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if Self::is_identity_insert_required_error(e.as_str()) => {
                self.clear_active_identity_insert_state().await;
                self.ensure_identity_insert_enabled(schema, table_name, table_key)
                    .await?;
                self.execute_insert_with_params(insert_sql, insert_params)
                    .await
                    .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    async fn delete_record(
        &self,
        schema: &str,
        table_name: &str,
        _table_key: &str,
        pk_name: &str,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        if pk.is_none() {
            return Ok(());
        }
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            Self::qualified_table(schema, table_name),
            Self::quote_column_ident(pk_name)
        );
        let params = vec![Self::value_to_param(pk)];
        debug!("Dameng DELETE: {}", sql);
        self.execute_with_params(sql.as_str(), &params).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_lob_columns(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        table_info: &TableInfoVo,
        record: &DataBuffer,
    ) -> Result<(), String> {
        self.write_blob_columns(
            schema, table_name, table_key, pk_name, columns, table_info, record,
        )
        .await?;
        self.write_clob_columns(
            schema, table_name, table_key, pk_name, columns, table_info, record,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_blob_columns(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        table_info: &TableInfoVo,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        let blob_column_keys = Self::source_blob_column_keys_for_records(
            table_info,
            columns,
            std::slice::from_ref(record),
        );
        if pk.is_none()
            && columns.iter().any(|col| {
                Self::is_blob_column_key(&blob_column_keys, col)
                    && matches!(
                        record.after.get(col),
                        Value::Blob(_) | Value::String(_) | Value::Json(_)
                    )
            })
        {
            return Err(format!(
                "pk is empty when writing Dameng BLOB columns: {}.{}",
                table_key, pk_name
            ));
        }

        for col in columns {
            if !Self::is_blob_column_key(&blob_column_keys, col) {
                continue;
            }
            match record.after.get(col) {
                Value::Blob(value) => {
                    self.write_blob_column(schema, table_name, pk_name, pk, col, value)
                        .await?;
                }
                Value::String(value) | Value::Json(value) => {
                    self.write_blob_column(schema, table_name, pk_name, pk, col, value.as_bytes())
                        .await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn write_blob_column(
        &self,
        schema: &str,
        table_name: &str,
        pk_name: &str,
        pk: &Value,
        column_name: &str,
        value: &[u8],
    ) -> Result<(), String> {
        let reset_sql = Self::reset_blob_sql(schema, table_name, pk_name, column_name);
        let reset_params = vec![Self::value_to_param(pk)];
        self.execute_with_params(reset_sql.as_str(), &reset_params)
            .await?;

        for chunk in value.chunks(DAMENG_BLOB_WRITE_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let append_sql =
                Self::append_blob_chunk_sql(schema, table_name, pk_name, column_name, chunk.len());
            let append_params = vec![
                Self::value_to_param(pk),
                DamengParam::Text(Self::bytes_to_hex(chunk)),
            ];
            self.execute_with_params(append_sql.as_str(), &append_params)
                .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_clob_columns(
        &self,
        schema: &str,
        table_name: &str,
        table_key: &str,
        pk_name: &str,
        columns: &[String],
        table_info: &TableInfoVo,
        record: &DataBuffer,
    ) -> Result<(), String> {
        let pk = record.get_pk(pk_name);
        let clob_column_keys = Self::source_clob_column_keys_for_records(
            table_info,
            columns,
            std::slice::from_ref(record),
        );
        if pk.is_none()
            && columns.iter().any(|col| {
                Self::is_clob_column_key(&clob_column_keys, col)
                    && matches!(record.after.get(col), Value::String(_) | Value::Json(_))
            })
        {
            return Err(format!(
                "pk is empty when writing Dameng CLOB columns: {}.{}",
                table_key, pk_name
            ));
        }

        for col in columns {
            if !Self::is_clob_column_key(&clob_column_keys, col) {
                continue;
            }
            match record.after.get(col) {
                Value::String(value) | Value::Json(value) => {
                    self.write_clob_column(schema, table_name, pk_name, pk, col, value)
                        .await?;
                }
                Value::Blob(value) => {
                    let value = String::from_utf8_lossy(value).to_string();
                    self.write_clob_column(schema, table_name, pk_name, pk, col, &value)
                        .await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn write_clob_column(
        &self,
        schema: &str,
        table_name: &str,
        pk_name: &str,
        pk: &Value,
        column_name: &str,
        value: &str,
    ) -> Result<(), String> {
        let reset_sql = Self::reset_clob_sql(schema, table_name, pk_name, column_name);
        let reset_params = vec![Self::value_to_param(pk)];
        self.execute_with_params(reset_sql.as_str(), &reset_params)
            .await?;

        for chunk in Self::string_chunks(value, DAMENG_CLOB_WRITE_CHUNK_CHARS) {
            if chunk.is_empty() {
                continue;
            }
            let append_sql = Self::append_clob_chunk_sql(
                schema,
                table_name,
                pk_name,
                column_name,
                chunk.chars().count(),
            );
            let append_params = vec![Self::value_to_param(pk), DamengParam::Text(chunk)];
            self.execute_with_params(append_sql.as_str(), &append_params)
                .await?;
        }
        Ok(())
    }
}

fn strip_trailing_numeric_schema_suffix(value: &str) -> Option<String> {
    let (base, suffix) = value.rsplit_once('-')?;
    if base.is_empty() || suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(base.to_string())
}

#[cfg(test)]
mod tests {
    use common::case_insensitive_hash_map::CaseInsensitiveHashMap;
    use common::{
        DataBuffer, ForeignKeyInfo, MySqlRoutineKind, Operation, TableIndexInfo, TableInfoVo, Value,
    };
    use dameng::ToDmValue;
    use dameng_types::DmValue;
    use std::collections::{HashMap, HashSet};

    use super::{DamengForeignKeyConstraintInfo, DamengParam, DamengSink};

    #[test]
    fn strips_numeric_schema_suffix_for_view_routes() {
        assert_eq!(
            super::strip_trailing_numeric_schema_suffix("newsee-charge-11"),
            Some("newsee-charge".to_string())
        );
        assert_eq!(
            super::strip_trailing_numeric_schema_suffix("newsee-charge"),
            None
        );
    }

    fn table_info(target_schema: &str, column_type: &str) -> TableInfoVo {
        TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: target_schema.to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: format!(
                "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `name` {} DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
                column_type
            ),
            columns: vec!["id".to_string(), "name".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        }
    }

    #[test]
    fn schema_sql_quotes_identifiers_and_literals() {
        assert_eq!(
            DamengSink::create_schema_sql("TARGET_SCHEMA"),
            "CREATE SCHEMA \"TARGET_SCHEMA\""
        );
        assert_eq!(
            DamengSink::schema_exists_sql("TARGET_SCHEMA"),
            "SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE$ = 'SCH' AND (NAME = 'TARGET_SCHEMA' OR NAME = 'TARGET_SCHEMA')"
        );
        assert_eq!(
            DamengSink::create_schema_sql("target\"schema"),
            "CREATE SCHEMA \"target\"\"schema\""
        );
        assert_eq!(
            DamengSink::schema_exists_sql("newsee-bpm"),
            "SELECT COUNT(*) FROM SYSOBJECTS WHERE TYPE$ = 'SCH' AND (NAME = 'newsee-bpm' OR NAME = 'NEWSEE-BPM')"
        );
    }

    #[test]
    fn qualified_table_uses_target_schema() {
        assert_eq!(
            DamengSink::qualified_table("TARGET_SCHEMA", "orders"),
            "\"TARGET_SCHEMA\".\"orders\""
        );
        assert_eq!(DamengSink::qualified_table("", "orders"), "\"orders\"");
        assert_eq!(
            DamengSink::target_table_key("TARGET_SCHEMA", "orders"),
            "TARGET_SCHEMA.orders"
        );
    }

    #[test]
    fn routine_exists_sql_quotes_schema_kind_and_name() {
        assert_eq!(
            DamengSink::dameng_routine_exists_sql(
                "target_schema",
                MySqlRoutineKind::Function,
                "calc`demo"
            ),
            "SELECT COUNT(*) FROM ALL_OBJECTS WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND OBJECT_TYPE = 'FUNCTION' AND (OBJECT_NAME = 'calc`demo' OR OBJECT_NAME = 'CALC`DEMO')"
        );
        assert_eq!(
            DamengSink::dameng_routine_exists_sql("", MySqlRoutineKind::Procedure, "sync_demo"),
            "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PROCEDURE' AND (OBJECT_NAME = 'sync_demo' OR OBJECT_NAME = 'SYNC_DEMO')"
        );
    }

    #[test]
    fn conflict_routine_name_adds_kind_suffix_and_limits_length() {
        assert_eq!(
            DamengSink::conflict_routine_name(
                "ns_report_classification",
                MySqlRoutineKind::Procedure,
                0
            ),
            "ns_report_classification_procedure"
        );
        assert_eq!(
            DamengSink::conflict_routine_name("calc_total", MySqlRoutineKind::Function, 1),
            "calc_total_function_2"
        );

        let long_name = "a".repeat(140);
        let renamed =
            DamengSink::conflict_routine_name(long_name.as_str(), MySqlRoutineKind::Procedure, 0);
        assert_eq!(renamed.chars().count(), 128);
        assert!(renamed.ends_with("_procedure"));
    }

    #[test]
    fn object_name_exists_sql_checks_any_object_type() {
        assert_eq!(
            DamengSink::dameng_object_name_exists_sql("target_schema", "ns_report_classification"),
            "SELECT COUNT(*) FROM ALL_OBJECTS WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND (OBJECT_NAME = 'ns_report_classification' OR OBJECT_NAME = 'NS_REPORT_CLASSIFICATION')"
        );
        assert_eq!(
            DamengSink::dameng_object_name_exists_sql("", "sync_demo"),
            "SELECT COUNT(*) FROM USER_OBJECTS WHERE (OBJECT_NAME = 'sync_demo' OR OBJECT_NAME = 'SYNC_DEMO')"
        );
    }

    #[test]
    fn view_exists_sql_checks_view_object_type() {
        assert_eq!(
            DamengSink::dameng_view_exists_sql("target_schema", "v_demo"),
            "SELECT COUNT(*) FROM ALL_OBJECTS WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND OBJECT_TYPE = 'VIEW' AND (OBJECT_NAME = 'v_demo' OR OBJECT_NAME = 'V_DEMO')"
        );
        assert_eq!(
            DamengSink::dameng_view_exists_sql("", "v_demo"),
            "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'VIEW' AND (OBJECT_NAME = 'v_demo' OR OBJECT_NAME = 'V_DEMO')"
        );
    }

    #[test]
    fn sql_preview_truncates_on_char_boundary() {
        let sql = format!("SELECT '{}'", "\u{5404}".repeat(600));
        let preview = DamengSink::sql_preview(sql.as_str());

        assert!(preview.ends_with("..."));
        assert!(preview.is_char_boundary(preview.len()));
    }

    #[test]
    fn unsupported_mysql_syntax_error_is_detected() {
        assert!(DamengSink::is_unsupported_mysql_syntax_error(
            "PROCEDURE demo conversion failed: unsupported MySQL syntax D12: INSERT IGNORE"
        ));
        assert!(!DamengSink::is_unsupported_mysql_syntax_error(
            "create Dameng stored routine failed: syntax error near SELECT"
        ));
    }

    #[test]
    fn foreign_key_sql_maps_restrict_to_no_action() {
        let foreign_key = ForeignKeyInfo {
            constraint_name: "fk_child_parent".to_string(),
            table_name: "child".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_table_name: "parent".to_string(),
            referenced_columns: vec!["rOWID".to_string()],
            update_rule: Some("RESTRICT".to_string()),
            delete_rule: Some("CASCADE".to_string()),
        };

        assert_eq!(
            DamengSink::foreign_key_sql("target_schema", &foreign_key).unwrap(),
            "ALTER TABLE \"target_schema\".\"child\" ADD CONSTRAINT \"fk_child_parent\" FOREIGN KEY (\"parent_id\") REFERENCES \"target_schema\".\"parent\" (\"rOWID_\") ON DELETE CASCADE ON UPDATE NO ACTION WITH INDEX"
        );
    }

    #[test]
    fn foreign_key_sql_rejects_mismatched_columns() {
        let foreign_key = ForeignKeyInfo {
            constraint_name: "fk_bad".to_string(),
            table_name: "child".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_table_name: "parent".to_string(),
            referenced_columns: vec![],
            update_rule: None,
            delete_rule: None,
        };

        assert!(DamengSink::foreign_key_sql("target_schema", &foreign_key).is_none());
    }

    #[test]
    fn foreign_key_constraints_schema_metadata_sql_filters_enabled_reference_constraints() {
        assert_eq!(
            DamengSink::foreign_key_constraints_schema_metadata_sql("newsee-equip"),
            "SELECT OWNER || CHR(31) || TABLE_NAME || CHR(31) || CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE (OWNER = 'newsee-equip' OR OWNER = 'NEWSEE-EQUIP') AND CONSTRAINT_TYPE = 'R' AND STATUS = 'ENABLED' ORDER BY OWNER, TABLE_NAME, CONSTRAINT_NAME"
        );
        assert_eq!(
            DamengSink::foreign_key_constraints_schema_metadata_sql(""),
            "SELECT TABLE_NAME || CHR(31) || CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'R' AND STATUS = 'ENABLED' ORDER BY TABLE_NAME, CONSTRAINT_NAME"
        );
    }

    #[test]
    fn parse_foreign_key_constraints_metadata_handles_schema_and_user_rows() {
        assert_eq!(
            DamengSink::parse_foreign_key_constraints_metadata(
                "",
                "ns_equip_area\x1fFK_ns_equip_area_ref_classID"
            ),
            vec![DamengForeignKeyConstraintInfo {
                schema: "".to_string(),
                table_name: "ns_equip_area".to_string(),
                constraint_name: "FK_ns_equip_area_ref_classID".to_string(),
            }]
        );
        assert_eq!(
            DamengSink::parse_foreign_key_constraints_metadata(
                "fallback",
                "newsee-equip\x1fns_equip_area\x1fFK_ns_equip_area_ref_classID"
            ),
            vec![DamengForeignKeyConstraintInfo {
                schema: "newsee-equip".to_string(),
                table_name: "ns_equip_area".to_string(),
                constraint_name: "FK_ns_equip_area_ref_classID".to_string(),
            }]
        );
    }

    #[test]
    fn disable_constraint_sql_quotes_table_and_constraint() {
        assert_eq!(
            DamengSink::disable_constraint_sql(
                "newsee-equip",
                "ns_equip_area",
                "FK_ns_equip_area_ref_classID"
            ),
            "ALTER TABLE \"newsee-equip\".\"ns_equip_area\" DISABLE CONSTRAINT \"FK_ns_equip_area_ref_classID\""
        );
        assert_eq!(
            DamengSink::disable_constraint_sql("", "orders", "fk\"demo"),
            "ALTER TABLE \"orders\" DISABLE CONSTRAINT \"fk\"\"demo\""
        );
        assert_eq!(
            DamengSink::enable_constraint_sql("newsee-equip", "ns_equip_area", "fk_demo"),
            "ALTER TABLE \"newsee-equip\".\"ns_equip_area\" ENABLE CONSTRAINT \"fk_demo\""
        );
    }

    #[test]
    fn create_table_sql_omits_primary_key_for_schema_only_table() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_system_organization_dimen_tree".to_string(),
            pk_column: "".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_system_organization_dimen_tree` (
  `organization_id` bigint NOT NULL,
  `organization_parent_id` bigint NOT NULL,
  `organization_path` varchar(400) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "organization_id".to_string(),
                "organization_parent_id".to_string(),
                "organization_path".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(
            sql.starts_with("CREATE TABLE \"target_schema\".\"ns_system_organization_dimen_tree\"")
        );
        assert!(sql.contains("\"organization_id\" BIGINT NOT NULL"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn create_table_sql_preserves_string_primary_key_for_schema_only_table() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "act_ge_bytearray".to_string(),
            pk_column: "".to_string(),
            create_table_sql: r#"CREATE TABLE `act_ge_bytearray` (
  `ID_` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `REV_` int DEFAULT NULL,
  `NAME_` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `DEPLOYMENT_ID_` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `BYTES_` longblob,
  `GENERATED_` tinyint DEFAULT NULL,
  PRIMARY KEY (`ID_`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 ROW_FORMAT=DYNAMIC"#
                .to_string(),
            columns: vec![
                "ID_".to_string(),
                "REV_".to_string(),
                "NAME_".to_string(),
                "DEPLOYMENT_ID_".to_string(),
                "BYTES_".to_string(),
                "GENERATED_".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"ID_\" VARCHAR(64 CHAR) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"ID_\")"));
    }

    #[test]
    fn create_table_sql_preserves_composite_primary_key_for_schema_only_table() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "tenant_code".to_string(),
            pk_column: "".to_string(),
            create_table_sql: r#"CREATE TABLE `tenant_code` (
  `tenant_id` varchar(64) NOT NULL,
  `code` varchar(64) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`tenant_id`,`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "tenant_id".to_string(),
                "code".to_string(),
                "name".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"tenant_id\" VARCHAR(64 CHAR) NOT NULL"));
        assert!(sql.contains("\"code\" VARCHAR(64 CHAR) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"tenant_id\", \"code\")"));
    }

    #[test]
    fn create_table_sql_preserves_current_timestamp_default_for_time_columns() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `orders` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `sys_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `name` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "sys_time".to_string(),
                "update_time".to_string(),
                "name".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"sys_time\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(sql.contains("\"update_time\" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(sql.contains("\"name\" VARCHAR(32 CHAR) NULL"));
    }

    #[test]
    fn create_table_sql_preserves_supported_literal_defaults() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "biz_building_info".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `biz_building_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `migrate_flag` tinyint NOT NULL DEFAULT '0' COMMENT '是否迁移业务表标志 0未迁移 1已迁移',
  `building_name` varchar(50) DEFAULT '未命名',
  `remark` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "migrate_flag".to_string(),
                "building_name".to_string(),
                "remark".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let sql = DamengSink::create_table_sql("target_schema", &table_info).unwrap();

        assert!(sql.contains("\"migrate_flag\" TINYINT NOT NULL DEFAULT 0"));
        assert!(sql.contains("\"building_name\" VARCHAR(50 CHAR) NULL DEFAULT '未命名'"));
        assert!(sql.contains("\"remark\" VARCHAR(50 CHAR) NULL"));
        assert!(!sql.contains("DEFAULT NULL"));
    }

    #[test]
    fn column_comments_are_converted_to_dameng_comment_sql() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "newsee-owner".to_string(),
            table_name: "biz_building_info".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `biz_building_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `migrate_flag` tinyint NOT NULL DEFAULT '0' COMMENT '是否迁移业务表标志 0未迁移 1已迁移',
  `name` varchar(50) DEFAULT NULL COMMENT '业主''名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "migrate_flag".to_string(),
                "name".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let comments = DamengSink::mysql_column_comments(&table_info);

        assert_eq!(
            comments.get("migrate_flag").map(String::as_str),
            Some("是否迁移业务表标志 0未迁移 1已迁移")
        );
        assert_eq!(comments.get("name").map(String::as_str), Some("业主'名称"));
        assert_eq!(
            DamengSink::comment_on_column_sql(
                "newsee-owner",
                "biz_building_info",
                "migrate_flag",
                comments.get("migrate_flag").unwrap()
            ),
            "COMMENT ON COLUMN \"newsee-owner\".\"biz_building_info\".\"migrate_flag\" IS '是否迁移业务表标志 0未迁移 1已迁移'"
        );
    }

    #[test]
    fn table_comment_sql_quotes_identifier_and_literal() {
        assert_eq!(
            DamengSink::comment_on_table_sql("newsee-owner", "biz_building_info", "业主'表"),
            "COMMENT ON TABLE \"newsee-owner\".\"biz_building_info\" IS '业主''表'"
        );
        assert_eq!(
            DamengSink::table_comment_sql("newsee-owner", "biz_building_info"),
            "SELECT NVL(COMMENTS, '') FROM ALL_TAB_COMMENTS WHERE (OWNER = 'newsee-owner' OR OWNER = 'NEWSEE-OWNER') AND (TABLE_NAME = 'biz_building_info' OR TABLE_NAME = 'BIZ_BUILDING_INFO')"
        );
    }

    #[test]
    fn secondary_index_sql_uses_dameng_types_and_target_columns() {
        let mut table_info = table_info("target_schema", "varchar(64)");
        table_info.indexes = vec![TableIndexInfo {
            index_name: "idx_orders_name".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
        }];

        let sql = DamengSink::secondary_index_sql(
            "target_schema",
            &table_info,
            &table_info.indexes[0],
            "idx_orders_name",
        )
        .unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX \"idx_orders_name\" ON \"target_schema\".\"orders\" (\"name\")"
        );
    }

    #[test]
    fn secondary_index_sql_skips_lob_columns() {
        let table_info = table_info("target_schema", "text");
        let index = TableIndexInfo {
            index_name: "idx_orders_name".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
        };

        assert!(
            DamengSink::secondary_index_sql("target_schema", &table_info, &index, "idx").is_none()
        );
    }

    #[test]
    fn secondary_index_metadata_sql_checks_schema_table_and_name() {
        assert_eq!(
            DamengSink::dameng_table_index_exists_sql("target_schema", "orders", "idx_orders_name"),
            "SELECT COUNT(*) FROM ALL_INDEXES WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND (TABLE_NAME = 'orders' OR TABLE_NAME = 'ORDERS') AND (INDEX_NAME = 'idx_orders_name' OR INDEX_NAME = 'IDX_ORDERS_NAME')"
        );
        assert_eq!(
            DamengSink::dameng_index_name_exists_sql("target_schema", "idx_orders_name"),
            "SELECT COUNT(*) FROM ALL_INDEXES WHERE (OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA') AND (INDEX_NAME = 'idx_orders_name' OR INDEX_NAME = 'IDX_ORDERS_NAME')"
        );
        assert_eq!(
            DamengSink::conflict_index_name("orders", "idx_name", 0),
            "orders_idx_name_idx"
        );
    }

    #[test]
    fn column_comment_update_skips_missing_target_columns() {
        let mut existing_comments = HashMap::new();
        existing_comments.insert("name".to_string(), "旧名称".to_string());
        existing_comments.insert("remark".to_string(), "备注".to_string());

        assert_eq!(
            DamengSink::column_comment_needs_update(
                &existing_comments,
                "third_part_id",
                "第三方ID"
            ),
            None
        );
        assert_eq!(
            DamengSink::column_comment_needs_update(&existing_comments, "remark", "备注"),
            Some(false)
        );
        assert_eq!(
            DamengSink::column_comment_needs_update(&existing_comments, "name", "名称"),
            Some(true)
        );
    }

    #[test]
    fn disallowed_dameng_column_names_are_remapped() {
        assert_eq!(DamengSink::target_column_name("id"), "id");
        assert_eq!(DamengSink::target_column_name("rOWID"), "rOWID_");
        assert_eq!(DamengSink::target_column_name("ROWID"), "ROWID_");
        assert_eq!(DamengSink::target_column_name("trxid"), "trxid_");
        assert_eq!(DamengSink::target_column_name("rownum"), "rownum_");
        assert_eq!(
            DamengSink::target_column_name("VERSIONS_STARTTIME"),
            "VERSIONS_STARTTIME_"
        );
        assert_eq!(
            DamengSink::target_column_name("versions_endtime"),
            "versions_endtime_"
        );
        assert_eq!(
            DamengSink::target_column_name("VERSIONS_STARTTRXID"),
            "VERSIONS_STARTTRXID_"
        );
        assert_eq!(
            DamengSink::target_column_name("versions_endtrxid"),
            "versions_endtrxid_"
        );
        assert_eq!(
            DamengSink::target_column_name("VERSIONS_OPERATION"),
            "VERSIONS_OPERATION_"
        );
        assert_eq!(DamengSink::quote_column_ident("rOWID"), "\"rOWID_\"");
        assert_eq!(DamengSink::quote_column_ident("trxid"), "\"trxid_\"");
    }

    #[test]
    fn dml_columns_include_observed_record_fields() {
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        after.insert("fullPath".to_string(), Value::String("/a/b".to_string()));
        after.insert("name".to_string(), Value::String("n".to_string()));
        let record = DataBuffer::new(
            "orders".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );

        let columns = DamengSink::merge_columns_with_records(&["id".to_string()], &[record]);

        assert_eq!(
            columns,
            vec!["id".to_string(), "fullPath".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn dml_columns_skip_mysql_generated_columns() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `orders` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `path` varchar(100) DEFAULT NULL,
  `fullPath` varchar(120) GENERATED ALWAYS AS (concat(`path`,`id`,_utf8mb3'/')) STORED,
  `name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "path".to_string(),
                "fullPath".to_string(),
                "name".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        after.insert("path".to_string(), Value::String("/a/".to_string()));
        after.insert("fullPath".to_string(), Value::String("/a/1/".to_string()));
        after.insert("name".to_string(), Value::String("n".to_string()));
        let record = DataBuffer::new(
            "orders".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );

        let columns =
            DamengSink::merge_dml_columns_with_records(&table_info, &table_info.columns, &[record]);

        assert_eq!(
            columns,
            vec!["id".to_string(), "path".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn dml_add_column_definition_falls_back_to_record_value() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `orders` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        after.insert("fullPath".to_string(), Value::String("/a/b".to_string()));
        let record = DataBuffer::new(
            "orders".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );

        let (schema_definition, schema_source) =
            DamengSink::dameng_add_column_definition_for_dml(&table_info, "name", &[]);
        let (record_definition, record_source) =
            DamengSink::dameng_add_column_definition_for_dml(&table_info, "fullPath", &[record]);

        assert_eq!(schema_definition, "VARCHAR(20 CHAR) NULL");
        assert_eq!(schema_source, "mysql_schema");
        assert_eq!(record_definition, "CLOB NULL");
        assert_eq!(record_source, "record_value");
    }

    #[test]
    fn dynamic_blob_columns_are_detected_from_records() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "files".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `files` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec!["id".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        after.insert("payload".to_string(), Value::Blob(vec![1, 2, 3]));
        let record = DataBuffer::new(
            "files".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );
        let columns = vec!["id".to_string(), "payload".to_string()];

        let blob_keys =
            DamengSink::source_blob_column_keys_for_records(&table_info, &columns, &[record]);

        assert!(blob_keys.contains("payload"));
    }

    #[test]
    fn schema_text_columns_with_blob_values_are_not_treated_as_blob_columns() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "articles".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `articles` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `title` varchar(255) DEFAULT NULL,
  `content` text,
  `payload` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "title".to_string(),
                "content".to_string(),
                "payload".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        after.insert("title".to_string(), Value::Blob(b"title".to_vec()));
        after.insert("content".to_string(), Value::Blob(b"body".to_vec()));
        after.insert("payload".to_string(), Value::Blob(vec![1, 2, 3]));
        let record = DataBuffer::new(
            "articles".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );
        let columns = vec![
            "id".to_string(),
            "title".to_string(),
            "content".to_string(),
            "payload".to_string(),
        ];

        let blob_keys = DamengSink::source_blob_column_keys_for_records(
            &table_info,
            &columns,
            std::slice::from_ref(&record),
        );
        let clob_keys =
            DamengSink::source_clob_column_keys_for_records(&table_info, &columns, &[record]);

        assert!(blob_keys.contains("payload"));
        assert!(!blob_keys.contains("title"));
        assert!(!blob_keys.contains("content"));
        assert!(clob_keys.contains("content"));
    }

    #[test]
    fn dml_required_defaults_fill_target_only_columns() {
        let required = vec![(
            "checkonType".to_string(),
            DamengSink::dameng_required_column_default_value(&super::DamengRequiredColumnInfo {
                column_name: "checkonType".to_string(),
                data_type: "VARCHAR".to_string(),
                default_value: None,
                identity: false,
            }),
        )];
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        let record = DataBuffer::new(
            "dw_datacenter_dorm_checkon_management".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );

        let (columns, records) = DamengSink::apply_dml_required_defaults(
            &["id".to_string()],
            &[record],
            required.as_slice(),
        );

        assert_eq!(columns, vec!["id".to_string(), "checkonType".to_string()]);
        assert_eq!(
            records[0].after.get("checkonType").resolve_string(),
            String::new()
        );
    }

    #[test]
    fn dml_required_defaults_preserve_source_null_required_columns() {
        let required = vec![(
            "create_time".to_string(),
            DamengSink::dameng_required_column_default_value(&super::DamengRequiredColumnInfo {
                column_name: "create_time".to_string(),
                data_type: "TIMESTAMP".to_string(),
                default_value: None,
                identity: false,
            }),
        )];
        let mut after = CaseInsensitiveHashMap::new_with_no_arg();
        after.insert("id".to_string(), Value::Int64(1));
        after.insert("create_time".to_string(), Value::None);
        after.insert(
            "update_time".to_string(),
            Value::DateTime("2024-01-02 03:04:05".to_string()),
        );
        let record = DataBuffer::new(
            "biz_precinct_info".to_string(),
            CaseInsensitiveHashMap::new_with_no_arg(),
            after,
            Operation::CREATE(true),
            "".to_string(),
            0,
            0,
        );

        let (columns, records) = DamengSink::apply_dml_required_defaults(
            &[
                "id".to_string(),
                "create_time".to_string(),
                "update_time".to_string(),
            ],
            &[record],
            required.as_slice(),
        );

        assert_eq!(
            columns,
            vec![
                "id".to_string(),
                "create_time".to_string(),
                "update_time".to_string()
            ]
        );
        assert_eq!(records[0].after.get("create_time").resolve_string(), "null");
        assert_eq!(
            records[0].after.get("update_time").resolve_string(),
            "2024-01-02 03:04:05"
        );
    }

    #[test]
    fn non_pk_column_detection_handles_primary_key_only_tables() {
        assert!(!DamengSink::has_non_pk_columns(
            &["help_topic_id".to_string()],
            "help_topic_id"
        ));
        assert!(DamengSink::has_non_pk_columns(
            &["help_topic_id".to_string(), "name".to_string()],
            "HELP_TOPIC_ID"
        ));
    }

    #[test]
    fn required_columns_metadata_skips_requested_source_columns() {
        let columns = DamengSink::parse_required_columns_metadata(
            "service_pay_amount\x1fFLOAT\x1f0\x1fN\x1etarget_only\x1fVARCHAR\x1f'x'\x1fN\x1etarget_identity\x1fBIGINT\x1f\x1fY",
        );
        let mut requested = HashSet::new();
        requested.insert("service_pay_amount".to_string());
        let required = columns
            .into_iter()
            .filter_map(|column| {
                let requested_column =
                    requested.contains(column.column_name.to_ascii_lowercase().as_str());
                if column.identity {
                    return None;
                }
                if requested_column {
                    return None;
                }
                if DamengSink::dameng_required_column_has_usable_default(&column) {
                    return None;
                }
                Some((
                    column.column_name.clone(),
                    DamengSink::dameng_required_column_default_value(&column),
                ))
            })
            .collect::<Vec<_>>();

        assert!(required.is_empty());
    }

    #[test]
    fn required_columns_metadata_marks_identity_columns() {
        let columns = DamengSink::parse_required_columns_metadata(
            "id\x1fBIGINT\x1f\x1fY\x1e id\x1fBIGINT\x1f\x1fN",
        );

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column_name, "id");
        assert!(columns[0].identity);
        assert_eq!(columns[1].column_name, " id");
        assert!(!columns[1].identity);
    }

    #[test]
    fn random_check_sql_uses_source_types_and_target_column_names() {
        let table_info = TableInfoVo {
            source_database: "source-db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "orders".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `orders` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(6) DEFAULT NULL,
  `payload` json DEFAULT NULL,
  `raw_data` longblob,
  `flags` bit(8) DEFAULT NULL,
  `rOWID` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "created_at".to_string(),
                "payload".to_string(),
                "raw_data".to_string(),
                "flags".to_string(),
                "rOWID".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let plans = DamengSink::random_check_column_plans(&table_info);

        assert_eq!(plans[0].kind, super::MysqlCompareKind::Integer);
        assert_eq!(plans[1].kind, super::MysqlCompareKind::DateTime);
        assert_eq!(plans[2].kind, super::MysqlCompareKind::Json);
        assert_eq!(plans[3].kind, super::MysqlCompareKind::Binary);
        assert_eq!(plans[4].kind, super::MysqlCompareKind::Bit);
        assert_eq!(plans[5].kind, super::MysqlCompareKind::Varchar);
        assert_eq!(plans[5].target_column, "rOWID_");

        let source_sql = DamengSink::mysql_random_check_sample_sql(&table_info, &plans, 10);
        assert!(source_sql.contains("FROM `source-db`.`orders` ORDER BY RAND() LIMIT 10"));
        assert!(source_sql.contains("DATE_FORMAT(`created_at`, '%Y-%m-%d %H:%i:%s.%f')"));
        assert!(source_sql.contains("UPPER(HEX(`raw_data`))"));
        assert!(source_sql.contains("`id` AS `__cdc_random_check_pk`"));

        let target_sql =
            DamengSink::dameng_random_check_select_sql("target_schema", &table_info, &plans);
        assert!(target_sql.starts_with("SELECT CASE WHEN t.\"id\" IS NULL"));
        assert!(target_sql.contains("FROM \"target_schema\".\"orders\" t"));
        assert!(target_sql.contains("RAWTOHEX(t.\"raw_data\")"));
        assert!(target_sql.contains("TO_CHAR(t.\"created_at\", 'YYYY-MM-DD HH24:MI:SS.FF6')"));
        assert!(target_sql.contains("CAST(CAST(t.\"flags\" AS VARCHAR(4000)) AS CLOB)"));
        assert!(target_sql.contains("TO_CHAR(t.\"rOWID_\")"));
        assert!(target_sql.contains("CASE WHEN t.\"id\" IS NULL THEN 'N;' ELSE 'V' || LENGTH("));
        assert!(target_sql.contains(" AS \"__cdc_payload\" FROM "));
        assert!(target_sql.contains("WHERE t.\"id\" = ?"));
        assert!(!target_sql.contains("__cdc_pk"));
        assert!(!target_sql.contains("UNION ALL"));

        let single_value_sql = DamengSink::dameng_random_check_single_value_select_sql(
            "target_schema",
            &table_info,
            &plans[1],
        );
        assert!(
            single_value_sql.contains("TO_CHAR(t.\"created_at\", 'YYYY-MM-DD HH24:MI:SS.FF6')")
        );
        assert!(single_value_sql.contains(" AS \"__cdc_value\" FROM "));
        assert!(single_value_sql.contains("WHERE t.\"id\" = ?"));
        assert!(!single_value_sql.contains("__cdc_pk"));

        let null_marker_sql = DamengSink::dameng_random_check_null_marker_select_sql(
            "target_schema",
            &table_info,
            &plans[5],
        );
        assert!(
            null_marker_sql
                .contains("CAST(CASE WHEN t.\"rOWID_\" IS NULL THEN 1 ELSE 0 END AS INT)")
        );
        assert!(null_marker_sql.contains(" AS \"__cdc_is_null\" FROM "));
        assert!(null_marker_sql.contains("WHERE t.\"id\" = ?"));

        let text_length_sql = DamengSink::dameng_random_check_text_length_select_sql(
            "target_schema",
            &table_info,
            &plans[2],
        );
        assert!(text_length_sql.contains("DBMS_LOB.GETLENGTH(CAST(t.\"payload\" AS CLOB))"));
        assert!(text_length_sql.contains("CAST(DBMS_LOB.GETLENGTH("));
        assert!(text_length_sql.contains(" AS VARCHAR(4000))"));

        let text_chunk_sql = DamengSink::dameng_random_check_text_chunk_select_sql(
            "target_schema",
            &table_info,
            &plans[2],
            1001,
            1000,
        );
        assert!(text_chunk_sql.contains(
            "CAST(DBMS_LOB.SUBSTR(CAST(t.\"payload\" AS CLOB), 1000, 1001) AS VARCHAR(4000))"
        ));
        assert!(text_chunk_sql.contains("WHERE t.\"id\" = ?"));
        assert!(!text_chunk_sql.contains("__cdc_pk"));
        let binary_chunk_sql = DamengSink::dameng_random_check_binary_chunk_select_sql(
            "target_schema",
            &table_info,
            &plans[3],
            1,
            1000,
        );
        assert!(binary_chunk_sql.contains("RAWTOHEX(DBMS_LOB.SUBSTR(t.\"raw_data\", 1000, 1))"));
        assert!(DamengSink::random_check_uses_chunked_text(plans[2].kind));
        assert!(DamengSink::random_check_uses_chunked_text(plans[5].kind));
        assert!(!DamengSink::random_check_uses_chunked_text(plans[1].kind));
    }

    #[test]
    fn detects_dameng_string_truncation_for_random_check_fallback() {
        assert!(DamengSink::is_dameng_string_truncation_error(
            "query failed: -6108: 字符串截断"
        ));
        assert!(!DamengSink::is_dameng_string_truncation_error(
            "query failed: -6602: Violate unique constraint"
        ));
    }

    #[test]
    fn detects_unstable_payload_errors_for_random_check_fallback() {
        assert!(DamengSink::is_random_check_payload_unstable_error(
            "query failed: -6108: 字符串截断"
        ));
        assert!(DamengSink::is_random_check_payload_unstable_error(
            "Dameng target payload marker is malformed at value_index=0"
        ));
        assert!(DamengSink::is_random_check_payload_unstable_error(
            "Dameng target query returned duplicate rows: pk=7 rows=2"
        ));
        assert!(!DamengSink::is_random_check_payload_unstable_error(
            "query failed: -6602: Violate unique constraint"
        ));
    }

    #[test]
    fn parses_dameng_text_length_values() {
        assert_eq!(DamengSink::parse_dameng_length_value("2919").unwrap(), 2919);
        assert_eq!(
            DamengSink::parse_dameng_length_value("2919.000").unwrap(),
            2919
        );
        assert!(DamengSink::parse_dameng_length_value("2919.5").is_err());
        assert!(DamengSink::parse_dameng_length_value("abc").is_err());
    }

    #[test]
    fn random_check_normalizes_time_json_and_char_values() {
        assert_eq!(
            DamengSink::normalize_compare_string(
                super::MysqlCompareKind::DateTime,
                "2026-06-27 12:13:14.120000000"
            ),
            "2026-06-27 12:13:14.12"
        );
        assert_eq!(
            DamengSink::normalize_compare_string(
                super::MysqlCompareKind::Date,
                "2026-06-27 00:00:00"
            ),
            "2026-06-27"
        );
        assert_eq!(
            DamengSink::normalize_compare_string(super::MysqlCompareKind::Time, "01:02:03.000000"),
            "01:02:03"
        );
        assert_eq!(
            DamengSink::normalize_compare_string(super::MysqlCompareKind::Char, "abc   "),
            "abc"
        );
        assert_eq!(
            DamengSink::normalize_compare_string(super::MysqlCompareKind::Json, r#"{ "b": 1 }"#),
            r#"{"b":1}"#
        );
    }

    #[test]
    fn random_check_payload_parser_handles_nulls_and_multibyte_values() {
        let values =
            DamengSink::parse_random_check_payload("V5:68144N;V7:阀门坏，换阀芯", 3).unwrap();

        assert_eq!(
            values,
            vec![
                Some("68144".to_string()),
                None,
                Some("阀门坏，换阀芯".to_string())
            ]
        );
        assert!(DamengSink::parse_random_check_payload("V3:保丹", 1).is_err());
    }

    #[test]
    fn random_check_compares_numeric_values_by_value() {
        assert!(DamengSink::compare_values_equal(
            super::MysqlCompareKind::Decimal,
            &Some("0.00".to_string()),
            &Some("0".to_string())
        ));
        assert!(DamengSink::compare_values_equal(
            super::MysqlCompareKind::Float,
            &Some("98.00".to_string()),
            &Some("9.8E1".to_string())
        ));
        assert!(DamengSink::compare_values_equal(
            super::MysqlCompareKind::Integer,
            &Some("2".to_string()),
            &Some("2E0".to_string())
        ));
        assert!(!DamengSink::compare_values_equal(
            super::MysqlCompareKind::Integer,
            &Some("0".to_string()),
            &Some("1".to_string())
        ));
        assert!(!DamengSink::compare_values_equal(
            super::MysqlCompareKind::Decimal,
            &Some("0.00".to_string()),
            &Some("1".to_string())
        ));
    }

    #[test]
    fn table_schema_signature_uses_target_column_name() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "line_item".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: "CREATE TABLE `line_item` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `rOWID` varchar(50) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB".to_string(),
            columns: vec!["id".to_string(), "rOWID".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let signature = DamengSink::table_schema_signature(&table_info);

        assert!(signature.contains(&("rowid_".to_string(), "varchar(50 char)".to_string())));
    }

    #[test]
    fn target_column_name_validation_rejects_remap_collision() {
        let mut table_info = table_info("target_schema", "varchar(64)");
        table_info.columns = vec!["id".to_string(), "rOWID".to_string(), "rOWID_".to_string()];

        let err = DamengSink::validate_target_column_names(&[table_info], "").unwrap_err();

        assert!(err.contains("target column name conflict"));
        assert!(err.contains("rOWID"));
        assert!(err.contains("rOWID_"));
    }

    #[test]
    fn metadata_sql_quotes_literals() {
        assert_eq!(
            DamengSink::quote_literal("newsee'system"),
            "'newsee''system'"
        );
        assert_eq!(
            DamengSink::eq_original_or_upper_sql("OWNER", "newsee-system"),
            "(OWNER = 'newsee-system' OR OWNER = 'NEWSEE-SYSTEM')"
        );
    }

    #[test]
    fn columns_metadata_sql_reads_one_row_per_column() {
        let sql = DamengSink::columns_metadata_sql("target_schema", "orders");

        assert!(sql.contains("FROM ALL_TAB_COLUMNS"));
        assert!(sql.contains("ORDER BY COLUMN_ID"));
        assert!(sql.contains("CHR(31)"));
        assert!(sql.contains("DATA_PRECISION"));
        assert!(sql.contains("DATA_SCALE"));
        assert!(sql.contains("NULLABLE"));
        assert!(!sql.contains("LISTAGG"));
        assert!(sql.contains("(OWNER = 'target_schema' OR OWNER = 'TARGET_SCHEMA')"));
        assert!(sql.contains("(TABLE_NAME = 'orders' OR TABLE_NAME = 'ORDERS')"));
    }

    #[test]
    fn parses_dameng_columns_metadata() {
        let cols = DamengSink::parse_columns_metadata(
            "id:BIGINT:8:0:|param:VARCHAR:16000:4000:|sys_time:TIMESTAMP:8:0:CURRENT_TIMESTAMP",
        );

        let param = cols.get("param").unwrap();
        assert_eq!(param.data_type, "VARCHAR");
        assert_eq!(param.data_length, 16000);
        assert_eq!(param.char_length, 4000);
        assert_eq!(param.data_precision, None);
        assert_eq!(param.data_scale, None);
        assert_eq!(param.default_value, None);

        let sys_time = cols.get("sys_time").unwrap();
        assert_eq!(sys_time.data_type, "TIMESTAMP");
        assert_eq!(sys_time.default_value.as_deref(), Some("CURRENT_TIMESTAMP"));

        let row_cols = DamengSink::parse_columns_metadata(
            "incomeType\x1fINT\x1f4\x1f0\x1f10\x1f0\x1f0\x1fN\x1eintegralDeductItems\x1fVARCHAR\x1f1024\x1f256\x1f\x1f\x1f\x1fY",
        );
        let income_type = row_cols.get("incometype").unwrap();
        assert_eq!(income_type.data_type, "INT");
        assert_eq!(income_type.data_precision, Some(10));
        assert_eq!(income_type.data_scale, Some(0));
        assert!(!income_type.nullable);
        assert_eq!(
            row_cols.get("integraldeductitems").unwrap().char_length,
            256
        );
        assert!(row_cols.get("integraldeductitems").unwrap().nullable);
    }

    #[test]
    fn parses_dameng_column_comments_metadata() {
        let comments = DamengSink::parse_column_comments_metadata(
            "migrate_flag\x1f是否迁移业务表标志 0未迁移 1已迁移\x1ename\x1f业主名称",
        );

        assert_eq!(
            comments.get("migrate_flag").map(String::as_str),
            Some("是否迁移业务表标志 0未迁移 1已迁移")
        );
        assert_eq!(comments.get("name").map(String::as_str), Some("业主名称"));
    }

    #[test]
    fn column_comment_sql_prefers_dba_comments_for_schema() {
        let sql = DamengSink::dba_column_comments_sql("newsee-center-pay", "ns_application");

        assert!(sql.contains("FROM DBA_COL_COMMENTS c"));
        assert!(sql.contains("c.OWNER = 'newsee-center-pay'"));
        assert!(sql.contains("c.TABLE_NAME = 'ns_application'"));
        assert!(sql.contains("c.COMMENTS"));
    }

    #[test]
    fn all_column_comment_sql_keeps_user_and_all_fallbacks() {
        let user_sql = DamengSink::all_column_comments_sql("", "ns_application");
        let all_sql = DamengSink::all_column_comments_sql("newsee-center-pay", "ns_application");

        assert!(user_sql.contains("FROM USER_TAB_COLUMNS c"));
        assert!(user_sql.contains("LEFT JOIN USER_COL_COMMENTS cc"));
        assert!(all_sql.contains("FROM ALL_TAB_COLUMNS c"));
        assert!(all_sql.contains("LEFT JOIN ALL_COL_COMMENTS cc"));
    }

    #[test]
    fn blob_values_bind_as_dameng_bytes() {
        let value = Value::Blob(vec![0x1f, 0x8b, 0x08]);
        assert_eq!(
            DamengSink::value_to_param(&value).to_dm_value(),
            DmValue::Bytea(vec![0x1f, 0x8b, 0x08])
        );
    }

    #[test]
    fn bit_values_bind_as_integer_for_dameng_bit_columns() {
        assert_eq!(
            DamengSink::value_to_param(&Value::Bit(1)).to_dm_value(),
            DmValue::BigInt(1)
        );
        assert_eq!(
            DamengSink::value_to_param(&Value::Bit(0)).to_dm_value(),
            DmValue::BigInt(0)
        );
    }

    #[test]
    fn inline_sql_params_replaces_placeholders_outside_quotes() {
        let sql = "INSERT INTO \"S\".\"表\" (\"ID\", \"name\", \"raw\") VALUES (?, ?, HEXTORAW(?))";
        let params = vec![
            DamengParam::I64(7),
            DamengParam::Text("达'鑫".to_string()),
            DamengParam::Text("1F8B".to_string()),
        ];

        let inline_sql = DamengSink::inline_sql_params(sql, &params).unwrap();

        assert_eq!(
            inline_sql,
            "INSERT INTO \"S\".\"表\" (\"ID\", \"name\", \"raw\") VALUES (7, '达''鑫', HEXTORAW('1F8B'))"
        );
    }

    #[test]
    fn inline_sql_params_renders_control_chars_with_chr() {
        let sql = "INSERT INTO \"S\".\"t\" (\"name\") VALUES (?)";
        let params = vec![DamengParam::Text("a\0b\n".to_string())];

        let inline_sql = DamengSink::inline_sql_params(sql, &params).unwrap();

        assert_eq!(
            inline_sql,
            "INSERT INTO \"S\".\"t\" (\"name\") VALUES ('a' || CHR(0) || 'b' || CHR(10))"
        );
    }

    #[test]
    fn non_ascii_text_params_require_inline_sql() {
        assert!(DamengSink::params_require_inline(&[DamengParam::Text(
            "身份证".to_string()
        )]));
        assert!(!DamengSink::params_require_inline(&[
            DamengParam::Text("ascii".to_string()),
            DamengParam::Bytes(vec![0xe8, 0xba])
        ]));
    }

    #[test]
    fn dameng_connection_errors_are_detected_for_random_check_retry() {
        assert!(DamengSink::is_dameng_connection_error(
            "query Dameng target row failed: IO error: Broken pipe (os error 32)"
        ));
        assert!(DamengSink::is_dameng_connection_error(
            "connection reset by peer"
        ));
        assert!(DamengSink::is_dameng_connection_error(
            "protocol error: incomplete protocol data"
        ));
        assert!(DamengSink::is_random_check_payload_unstable_error(
            "query Dameng target row failed: protocol error: incomplete protocol data"
        ));
        assert!(!DamengSink::is_dameng_connection_error(
            "query failed: -6602: duplicate key"
        ));
    }

    #[test]
    fn inline_sql_params_keeps_question_marks_inside_literals_and_identifiers() {
        let sql = "UPDATE \"S\".\"t?\" SET \"name\" = ? WHERE \"note\" = '?'";
        let params = vec![DamengParam::Text("x".to_string())];

        let inline_sql = DamengSink::inline_sql_params(sql, &params).unwrap();

        assert_eq!(
            inline_sql,
            "UPDATE \"S\".\"t?\" SET \"name\" = 'x' WHERE \"note\" = '?'"
        );
    }

    #[test]
    fn inline_sql_params_renders_bytes_as_hextoraw() {
        let inline_sql = DamengSink::inline_sql_params(
            "INSERT INTO \"S\".\"T\" (\"b\") VALUES (?)",
            &[DamengParam::Bytes(vec![0x1f, 0x8b])],
        )
        .unwrap();

        assert_eq!(
            inline_sql,
            "INSERT INTO \"S\".\"T\" (\"b\") VALUES (HEXTORAW('1F8B'))"
        );
    }

    #[test]
    fn detects_identity_insert_required_error() {
        assert!(DamengSink::is_identity_insert_required_error(
            "query failed: -2723: Only if specified in the column list and SET IDENTITY_INSERT is ON"
        ));
        assert!(DamengSink::is_identity_insert_required_error(
            "SET identity_insert is ON"
        ));
        assert!(!DamengSink::is_identity_insert_required_error(
            "query failed: -6609: Violate not null constraint"
        ));
    }

    #[test]
    fn detects_duplicate_key_error() {
        assert!(DamengSink::is_duplicate_key_error(
            "query failed: -6602: Violate unique constraint"
        ));
        assert!(DamengSink::is_duplicate_key_error(
            "Violate UNIQUE CONSTRAINT on primary key"
        ));
        assert!(!DamengSink::is_duplicate_key_error(
            "query failed: -2723: SET IDENTITY_INSERT is ON"
        ));
    }

    #[test]
    fn insert_control_flow_errors_are_not_retried() {
        assert!(DamengSink::is_insert_control_flow_error(
            "query failed: -6602: Violate unique constraint"
        ));
        assert!(DamengSink::is_insert_control_flow_error(
            "query failed: -2723: SET IDENTITY_INSERT is ON"
        ));
        assert!(!DamengSink::is_insert_control_flow_error(
            "query failed: -2665: record too long"
        ));
    }

    #[test]
    fn identity_insert_cache_key_is_case_insensitive() {
        assert_eq!(
            DamengSink::identity_insert_cache_key("Ns_Core_Funcinfo"),
            "ns_core_funcinfo"
        );
    }

    #[test]
    fn identity_insert_cache_key_includes_schema_when_present() {
        assert_eq!(
            DamengSink::identity_insert_cache_key("TARGET_SCHEMA.Ns_Core_Funcinfo"),
            "target_schema.ns_core_funcinfo"
        );
    }

    #[test]
    fn identity_insert_sql_uses_qualified_table() {
        assert_eq!(
            DamengSink::identity_insert_sql("\"S\".\"T\"", true),
            "SET IDENTITY_INSERT \"S\".\"T\" ON"
        );
        assert_eq!(
            DamengSink::identity_insert_sql("\"S\".\"T\"", false),
            "SET IDENTITY_INSERT \"S\".\"T\" OFF"
        );
    }

    #[test]
    fn merged_target_schema_allows_identical_tables() {
        let tables = vec![
            table_info("DST_SCHEMA", "varchar(64)"),
            table_info("DST_SCHEMA", "varchar(64)"),
        ];

        assert!(DamengSink::validate_merged_target_schema(&tables, "").is_ok());
    }

    #[test]
    fn merged_target_schema_rejects_type_mismatch_in_same_schema() {
        let tables = vec![
            table_info("DST_SCHEMA", "varchar(64)"),
            table_info("DST_SCHEMA", "varchar(128)"),
        ];

        let err = DamengSink::validate_merged_target_schema(&tables, "").unwrap_err();

        assert!(err.contains("schema mismatch"));
    }

    #[test]
    fn merged_target_schema_allows_same_table_in_different_schemas() {
        let tables = vec![
            table_info("DST_A", "varchar(64)"),
            table_info("DST_B", "varchar(128)"),
        ];

        assert!(DamengSink::validate_merged_target_schema(&tables, "").is_ok());
    }

    #[test]
    fn mysql_char_types_use_character_semantics() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(50)"),
            "VARCHAR(50 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(0)"),
            "VARCHAR(1 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("char(20)"),
            "CHAR(20 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("char(0)"),
            "CHAR(1 CHAR)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(1000)"),
            "CLOB"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(4000)"),
            "CLOB"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("varchar(9000)"),
            "CLOB"
        );
    }

    #[test]
    fn mysql_decimal_precision_is_limited_for_dameng() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(20,2)"),
            "DECIMAL(20,2)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("numeric(10)"),
            "DECIMAL(10,0)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(50,2)"),
            "DECIMAL(38,2)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(65,30)"),
            "DECIMAL(38,30)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("decimal(65,30) unsigned"),
            "DECIMAL(38,30)"
        );
        assert_eq!(DamengSink::map_mysql_type_to_dameng("decimal"), "DECIMAL");
    }

    #[test]
    fn mysql_tinyint_one_stays_numeric_for_dameng() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng("tinyint(1)"),
            "TINYINT"
        );
        assert_eq!(DamengSink::map_mysql_type_to_dameng("boolean"), "TINYINT");
        assert_eq!(DamengSink::map_mysql_type_to_dameng("bit(1)"), "BIGINT");
    }

    #[test]
    fn auto_increment_integer_columns_use_dameng_identity() {
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("bigint", false),
            "BIGINT"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("bigint", true),
            "BIGINT IDENTITY(1,1)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("int(11) unsigned", true),
            "BIGINT IDENTITY(1,1)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("bigint unsigned", true),
            "DECIMAL(20,0) IDENTITY(1,1)"
        );
        assert_eq!(
            DamengSink::map_mysql_type_to_dameng_for_column("tinyint(1)", true),
            "TINYINT IDENTITY(1,1)"
        );
    }

    #[test]
    fn existing_char_columns_only_modify_when_needed() {
        let byte_semantics = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 50,
            char_length: 50,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let char_semantics = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 200,
            char_length: 50,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        assert!(DamengSink::should_modify_existing_column(
            "varchar(50)",
            "`name` varchar(50) DEFAULT NULL",
            &byte_semantics
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "varchar(50)",
            "`name` varchar(50) DEFAULT NULL",
            &char_semantics
        ));
    }

    #[test]
    fn existing_clob_is_not_narrowed_to_varchar() {
        let clob = super::DamengColumnInfo {
            data_type: "CLOB".to_string(),
            data_length: 2147483647,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };

        assert!(!DamengSink::should_modify_existing_column(
            "varchar(50)",
            "`errorMessage` varchar(50) DEFAULT NULL",
            &clob
        ));
    }

    #[test]
    fn existing_varchar_is_modified_to_clob_for_mysql_text() {
        let varchar = super::DamengColumnInfo {
            data_type: "VARCHAR".to_string(),
            data_length: 800,
            char_length: 200,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };

        assert!(DamengSink::should_modify_existing_column(
            "text",
            "`billType` text COMMENT '票据可开类型'",
            &varchar
        ));
        assert_eq!(DamengSink::dameng_modify_column_definition("text"), "CLOB");
    }

    #[test]
    fn existing_clob_is_modified_to_blob_for_mysql_blob() {
        let clob = super::DamengColumnInfo {
            data_type: "CLOB".to_string(),
            data_length: 2147483647,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let blob = super::DamengColumnInfo {
            data_type: "BLOB".to_string(),
            data_length: 2147483647,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };

        assert!(DamengSink::should_modify_existing_column(
            "longblob",
            "`chargeItemId` longblob",
            &clob
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "longblob",
            "`chargeItemId` longblob",
            &blob
        ));
        assert!(DamengSink::should_recreate_column_for_random_check(
            true, "longblob", &clob
        ));
        assert!(!DamengSink::should_recreate_column_for_random_check(
            false, "longblob", &clob
        ));
        assert!(!DamengSink::should_recreate_column_for_random_check(
            true, "longblob", &blob
        ));
    }

    #[test]
    fn existing_column_modify_definition_uses_type_only() {
        assert_eq!(
            DamengSink::dameng_modify_column_definition("bigint"),
            "BIGINT"
        );
        assert_eq!(
            DamengSink::dameng_modify_column_definition("varchar(50)"),
            "VARCHAR(50 CHAR)"
        );
        assert_eq!(
            DamengSink::dameng_modify_column_definition_with_nullable("varchar(50)", "NULL"),
            "VARCHAR(50 CHAR) NULL"
        );
    }

    #[test]
    fn current_timestamp_default_detection_handles_supported_time_columns() {
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "timestamp",
                "`sys_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            ),
            Some("DEFAULT CURRENT_TIMESTAMP")
        );
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "datetime",
                "`update_time` datetime DEFAULT CURRENT_TIMESTAMP(3)",
            ),
            Some("DEFAULT CURRENT_TIMESTAMP")
        );
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "varchar(64)",
                "`name` varchar(64) DEFAULT CURRENT_TIMESTAMP",
            ),
            None
        );
        assert_eq!(
            DamengSink::mysql_current_timestamp_default_sql(
                "timestamp",
                "`sys_time` timestamp NOT NULL COMMENT 'DEFAULT CURRENT_TIMESTAMP'",
            ),
            None
        );
    }

    #[test]
    fn current_timestamp_default_mismatch_is_skipped_for_existing_columns() {
        let missing_default = super::DamengColumnInfo {
            data_type: "TIMESTAMP".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let matching_default = super::DamengColumnInfo {
            data_type: "TIMESTAMP".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: Some("CURRENT_TIMESTAMP".to_string()),
            nullable: true,
        };
        let def =
            "`sys_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";

        assert!(DamengSink::existing_column_default_mismatch(
            "timestamp",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "timestamp",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "timestamp",
            def,
            &matching_default
        ));
    }

    #[test]
    fn existing_date_column_modifies_to_timestamp_for_mysql_datetime() {
        let existing_date = super::DamengColumnInfo {
            data_type: "DATE".to_string(),
            data_length: 3,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let existing_timestamp = super::DamengColumnInfo {
            data_type: "TIMESTAMP".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };

        assert!(DamengSink::should_modify_existing_column(
            "timestamp",
            "`creationTime` timestamp NOT NULL",
            &existing_date
        ));
        assert!(DamengSink::should_modify_existing_column(
            "datetime",
            "`creationTime` datetime NOT NULL",
            &existing_date
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "timestamp",
            "`creationTime` timestamp NOT NULL",
            &existing_timestamp
        ));
    }

    #[test]
    fn literal_default_mismatch_is_skipped_for_existing_columns() {
        let missing_default = super::DamengColumnInfo {
            data_type: "TINYINT".to_string(),
            data_length: 1,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let matching_default = super::DamengColumnInfo {
            data_type: "TINYINT".to_string(),
            data_length: 1,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: Some("0".to_string()),
            nullable: true,
        };
        let def = "`migrate_flag` tinyint NOT NULL DEFAULT '0' COMMENT '是否迁移业务表标志 0未迁移 1已迁移'";

        assert!(DamengSink::existing_column_default_mismatch(
            "tinyint",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "tinyint",
            def,
            &missing_default
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "tinyint",
            def,
            &matching_default
        ));
    }

    #[test]
    fn existing_bit_column_modifies_to_expected_numeric_type() {
        let existing_bit = super::DamengColumnInfo {
            data_type: "BIT".to_string(),
            data_length: 1,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };

        assert!(DamengSink::should_modify_existing_column(
            "tinyint(1)",
            "`flag` tinyint(1) NOT NULL",
            &existing_bit
        ));
    }

    #[test]
    fn existing_narrow_integer_column_modifies_to_source_width() {
        let existing_tinyint = super::DamengColumnInfo {
            data_type: "TINYINT".to_string(),
            data_length: 1,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: Some("0".to_string()),
            nullable: true,
        };
        let existing_smallint = super::DamengColumnInfo {
            data_type: "SMALLINT".to_string(),
            data_length: 2,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: Some("0".to_string()),
            nullable: true,
        };
        let existing_bigint = super::DamengColumnInfo {
            data_type: "BIGINT".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let existing_decimal = super::DamengColumnInfo {
            data_type: "DECIMAL".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: Some(10),
            data_scale: Some(0),
            default_value: None,
            nullable: true,
        };

        assert!(DamengSink::should_modify_existing_column(
            "int",
            "`voucher_type` int DEFAULT '0'",
            &existing_tinyint
        ));
        assert!(DamengSink::should_modify_existing_column(
            "int",
            "`voucher_type` int DEFAULT '0'",
            &existing_smallint
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "int",
            "`voucher_type` int DEFAULT '0'",
            &existing_bigint
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "int",
            "`voucher_type` int DEFAULT '0'",
            &existing_decimal
        ));
    }

    #[test]
    fn existing_numeric_column_modifies_to_mysql_decimal_shape() {
        let existing_bigint = super::DamengColumnInfo {
            data_type: "BIGINT".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: None,
            data_scale: None,
            default_value: None,
            nullable: true,
        };
        let existing_decimal_scale_zero = super::DamengColumnInfo {
            data_type: "DECIMAL".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: Some(10),
            data_scale: Some(0),
            default_value: None,
            nullable: true,
        };
        let existing_decimal = super::DamengColumnInfo {
            data_type: "DECIMAL".to_string(),
            data_length: 8,
            char_length: 0,
            data_precision: Some(10),
            data_scale: Some(2),
            default_value: None,
            nullable: true,
        };

        assert!(DamengSink::should_modify_existing_column(
            "decimal(10,2)",
            "`allowanceSum` decimal(10,2) DEFAULT NULL",
            &existing_bigint
        ));
        assert!(DamengSink::should_modify_existing_column(
            "decimal(10,2)",
            "`allowanceSum` decimal(10,2) DEFAULT NULL",
            &existing_decimal_scale_zero
        ));
        assert!(!DamengSink::should_modify_existing_column(
            "decimal(10,2)",
            "`allowanceSum` decimal(10,2) DEFAULT NULL",
            &existing_decimal
        ));
    }

    #[test]
    fn primary_key_and_auto_increment_columns_are_protected_from_auto_modify() {
        let primary_key_columns = vec!["id".to_string()];

        assert!(DamengSink::is_protected_source_column_for_auto_modify(
            &primary_key_columns,
            "ID",
            "`id` bigint NOT NULL DEFAULT 0"
        ));
        assert!(DamengSink::is_protected_source_column_for_auto_modify(
            &[],
            "id",
            "`id` bigint NOT NULL AUTO_INCREMENT"
        ));
        assert!(!DamengSink::is_protected_source_column_for_auto_modify(
            &primary_key_columns,
            "migrate_flag",
            "`migrate_flag` tinyint NOT NULL DEFAULT '0'"
        ));
    }

    #[test]
    fn lob_columns_use_dameng_dml_expressions() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_system_log".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_system_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `param` varchar(4000) DEFAULT NULL,
  `modul` varchar(100) DEFAULT NULL,
  `opration` varchar(1000) DEFAULT NULL,
  `operatorLogo` blob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "param".to_string(),
                "modul".to_string(),
                "opration".to_string(),
                "operatorLogo".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "param", &Value::String("x".to_string())),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "opration", &Value::String("x".to_string())),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "modul", &Value::String("x".to_string())),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "operatorLogo", &Value::Blob(vec![1, 2, 3])),
            "EMPTY_BLOB()"
        );
        assert_eq!(
            DamengSink::dml_value_sql(
                &table_info,
                "operatorLogo",
                &Value::String("logo".to_string())
            ),
            "EMPTY_BLOB()"
        );
        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "operatorLogo", &Value::None),
            "?"
        );
    }

    #[test]
    fn blob_columns_are_written_as_hex_chunks() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_soss_operator".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_soss_operator` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `operatorLogo` blob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec!["id".to_string(), "operatorLogo".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let value = Value::Blob(vec![0x1f, 0x8b, 0x08]);
        assert!(DamengSink::dml_value_param(&table_info, "operatorLogo", &value).is_none());
        assert!(
            DamengSink::dml_value_param(
                &table_info,
                "operatorLogo",
                &Value::String("车位物业费".to_string())
            )
            .is_none()
        );
        assert_eq!(
            DamengSink::dml_value_param(&table_info, "operatorLogo", &Value::None)
                .unwrap()
                .to_dm_value(),
            DmValue::Null
        );
        assert_eq!(DamengSink::bytes_to_hex(&[0x1f, 0x8b, 0x08]), "1F8B08");
        assert_eq!(
            DamengSink::reset_blob_sql("target_schema", "ns_soss_operator", "id", "operatorLogo"),
            "UPDATE \"target_schema\".\"ns_soss_operator\" SET \"operatorLogo\" = EMPTY_BLOB() WHERE \"id\" = ?"
        );
        assert_eq!(
            DamengSink::append_blob_chunk_sql(
                "target_schema",
                "ns_soss_operator",
                "id",
                "operatorLogo",
                3
            ),
            "DECLARE\n  v_lob BLOB;\nBEGIN\n  SELECT \"operatorLogo\" INTO v_lob FROM \"target_schema\".\"ns_soss_operator\" WHERE \"id\" = ? FOR UPDATE;\n  DBMS_LOB.WRITEAPPEND(v_lob, 3, HEXTORAW(?));\nEND;"
        );
    }

    #[test]
    fn init_batch_insert_sql_groups_rows_and_keeps_blob_placeholders() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_soss_operator".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_soss_operator` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(20) DEFAULT NULL,
  `operatorLogo` blob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "operatorLogo".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let columns = table_info.columns.clone();

        let mut row1 = CaseInsensitiveHashMap::new_with_no_arg();
        row1.insert("id".to_string(), Value::Int64(1));
        row1.insert("name".to_string(), Value::String("a".to_string()));
        row1.insert("operatorLogo".to_string(), Value::Blob(vec![1, 2, 3]));
        let mut row2 = CaseInsensitiveHashMap::new_with_no_arg();
        row2.insert("id".to_string(), Value::Int64(2));
        row2.insert("name".to_string(), Value::String("b".to_string()));
        row2.insert("operatorLogo".to_string(), Value::None);
        let records = vec![
            DataBuffer::new(
                "ns_soss_operator".to_string(),
                CaseInsensitiveHashMap::new_with_no_arg(),
                row1,
                Operation::CREATE(true),
                "".to_string(),
                0,
                0,
            ),
            DataBuffer::new(
                "ns_soss_operator".to_string(),
                CaseInsensitiveHashMap::new_with_no_arg(),
                row2,
                Operation::CREATE(true),
                "".to_string(),
                0,
                0,
            ),
        ];

        let (sql, params) = DamengSink::batch_insert_sql_and_params(
            "target_schema",
            "ns_soss_operator",
            &columns,
            &table_info,
            &records,
        );

        assert_eq!(
            sql,
            "INSERT INTO \"target_schema\".\"ns_soss_operator\" (\"id\", \"name\", \"operatorLogo\") VALUES (?, ?, EMPTY_BLOB()), (?, ?, ?)"
        );
        assert_eq!(params.len(), 5);
        assert_eq!(params[0].to_dm_value(), DmValue::BigInt(1));
        assert_eq!(params[1].to_dm_value(), DmValue::Text("a".to_string()));
        assert_eq!(params[2].to_dm_value(), DmValue::BigInt(2));
        assert_eq!(params[3].to_dm_value(), DmValue::Text("b".to_string()));
        assert_eq!(params[4].to_dm_value(), DmValue::Null);
    }

    #[test]
    fn init_batch_merge_sql_upserts_rows_and_keeps_blob_placeholders() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "ns_soss_operator".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `ns_soss_operator` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(20) DEFAULT NULL,
  `operatorLogo` blob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "operatorLogo".to_string(),
            ],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };
        let columns = table_info.columns.clone();

        let mut row1 = CaseInsensitiveHashMap::new_with_no_arg();
        row1.insert("id".to_string(), Value::Int64(1));
        row1.insert("name".to_string(), Value::String("a".to_string()));
        row1.insert("operatorLogo".to_string(), Value::Blob(vec![1, 2, 3]));
        let mut row2 = CaseInsensitiveHashMap::new_with_no_arg();
        row2.insert("id".to_string(), Value::Int64(2));
        row2.insert("name".to_string(), Value::String("b".to_string()));
        row2.insert("operatorLogo".to_string(), Value::None);
        let records = vec![
            DataBuffer::new(
                "ns_soss_operator".to_string(),
                CaseInsensitiveHashMap::new_with_no_arg(),
                row1,
                Operation::CREATE(true),
                "".to_string(),
                0,
                0,
            ),
            DataBuffer::new(
                "ns_soss_operator".to_string(),
                CaseInsensitiveHashMap::new_with_no_arg(),
                row2,
                Operation::CREATE(true),
                "".to_string(),
                0,
                0,
            ),
        ];

        let (sql, params) = DamengSink::batch_merge_sql_and_params(
            "target_schema",
            "ns_soss_operator",
            "id",
            &columns,
            &table_info,
            &records,
        )
        .unwrap();

        assert_eq!(
            sql,
            "MERGE INTO \"target_schema\".\"ns_soss_operator\" t USING (SELECT ? AS \"id\", ? AS \"name\", EMPTY_BLOB() AS \"operatorLogo\" FROM DUAL UNION ALL SELECT ?, ?, ? FROM DUAL) s ON (t.\"id\" = s.\"id\") WHEN MATCHED THEN UPDATE SET t.\"name\" = s.\"name\", t.\"operatorLogo\" = s.\"operatorLogo\" WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"operatorLogo\") VALUES (s.\"id\", s.\"name\", s.\"operatorLogo\")"
        );
        assert_eq!(params.len(), 5);
        assert_eq!(params[0].to_dm_value(), DmValue::BigInt(1));
        assert_eq!(params[1].to_dm_value(), DmValue::Text("a".to_string()));
        assert_eq!(params[2].to_dm_value(), DmValue::BigInt(2));
        assert_eq!(params[3].to_dm_value(), DmValue::Text("b".to_string()));
        assert_eq!(params[4].to_dm_value(), DmValue::Null);
    }

    #[test]
    fn text_columns_with_blob_runtime_values_bind_as_text() {
        let table_info = TableInfoVo {
            source_database: "source_db".to_string(),
            target_database: "target_schema".to_string(),
            table_name: "o2o_bbs_activity".to_string(),
            pk_column: "id".to_string(),
            create_table_sql: r#"CREATE TABLE `o2o_bbs_activity` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `content` longtext DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"#
                .to_string(),
            columns: vec!["id".to_string(), "content".to_string()],
            table_comment: String::new(),
            indexes: vec![],
            foreign_keys: vec![],
        };

        let value = Value::Blob("正文内容".as_bytes().to_vec());

        assert_eq!(
            DamengSink::dml_value_sql(&table_info, "content", &value),
            "?"
        );
        assert_eq!(
            DamengSink::dml_value_param(&table_info, "content", &value)
                .unwrap()
                .to_dm_value(),
            DmValue::Text("正文内容".to_string())
        );
    }
}
