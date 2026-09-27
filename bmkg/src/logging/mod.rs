use tracing_subscriber::EnvFilter;

/// `level`/`format` are already validated in `config.rs`; a bad `EnvFilter` falls back to `info`.
pub fn init(level: &str, format: &str) {
    let filter = EnvFilter::try_new(level.to_lowercase()).unwrap_or_else(|_| EnvFilter::new("info"));

    if format == "json" {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            // Event fields flatten to the top level of the JSON object; span context is off.
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();
    }
}
