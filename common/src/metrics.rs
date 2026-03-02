use lazy_static::lazy_static;
use prometheus::{
    HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, register_histogram_vec,
    register_int_counter_vec, register_int_gauge_vec,
};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Source Metrics
    pub static ref SOURCE_EVENTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        Opts::new("cdc_source_events_total", "Total number of events read from source"),
        &["source_type", "table", "operation"]
    ).expect("metric can be created");

    pub static ref SOURCE_LAG_POSITION: IntGaugeVec = register_int_gauge_vec!(
        Opts::new("cdc_source_lag_position", "Current binlog position lag"),
        &["source_type", "table"]
    ).expect("metric can be created");

    // Sink Metrics
    pub static ref SINK_EVENTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        Opts::new("cdc_sink_events_total", "Total number of events written to sink"),
        &["sink_type", "table", "operation"]
    ).expect("metric can be created");

    pub static ref SINK_FLUSH_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "cdc_sink_flush_duration_seconds",
        "Duration of sink flush operations in seconds",
        &["sink_type", "trigger"]
    ).expect("metric can be created");

    pub static ref SINK_FLUSH_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        Opts::new("cdc_sink_flush_errors_total", "Total number of sink flush errors"),
        &["sink_type", "error_type"]
    ).expect("metric can be created");

    // General Metrics
    pub static ref APP_RESTART_COUNT: IntCounterVec = register_int_counter_vec!(
        Opts::new("cdc_app_restart_count", "Total number of application/component restarts"),
        &["component"]
    ).expect("metric can be created");
}

pub fn register_custom_metrics() {
    // This function can be used to explicitly trigger lazy_static initialization if needed,
    // or to register additional metrics dynamically.
    // For lazy_static, accessing any metric will initialize it.
    // We also need to register these to the default registry if we want to use the default one,
    // but here we are using a custom REGISTRY.
    // However, to make it easy for the main app to expose, we might want to use the default registry
    // or expose our custom registry.
    // Let's stick to the default registry for simplicity if possible, but the `prometheus` crate
    // usually recommends a custom one for libraries or structured apps.
    // But `register_*` macros use the DEFAULT registry by default unless we use `register_*_with_registry`.
    // Wait, `register_int_counter_vec!` registers to the DEFAULT registry.
    // So `REGISTRY` above is actually unused if I use the macros without `_with_registry`.
    
    // Let's check `prometheus` crate docs behavior.
    // `register_int_counter_vec!` -> registers to `default_registry()`.
    // So we don't need a custom `REGISTRY` unless we want isolation.
    // For a simple app, default registry is fine.
}
