use tracing_subscriber::EnvFilter;

/// `level`/`format` are already validated in `config.rs` (a fixed set of levels, `json`/`text`
/// only), so this never fails in practice; a bad `EnvFilter` falls back to `info` rather than panic.
///
/// Deliberately does not attach a constant `service` field to every line the way PVMBG's Logback
/// config does: `tracing-subscriber`'s JSON formatter has no built-in "extra static field" knob, and
/// a single-service log stream does not need one to disambiguate itself. `docker compose logs`
/// already tags each line by container/service name unless `--no-log-prefix` is passed.
pub fn init(level: &str, format: &str) {
    let filter = EnvFilter::try_new(level.to_lowercase()).unwrap_or_else(|_| EnvFilter::new("info"));

    if format == "json" {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            // Event fields (event="...", status=..., etc.) flatten to the top level of the JSON
            // object; span context is switched off since nothing in this codebase relies on it
            // (every field is passed explicitly at each log call site instead).
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();
    }
}
