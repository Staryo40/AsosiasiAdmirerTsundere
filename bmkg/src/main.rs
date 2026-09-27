mod config;
mod logging;
mod models;
mod plugins;
mod routing;
mod seed;
mod state;
#[cfg(test)]
mod test_support;

use config::AppConfig;
use plugins::delay::DelayRange;
use seed::EventGenerator;
use state::AppState;

#[tokio::main]
async fn main() {
    let config = match AppConfig::load() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    logging::init(&config.log_level, &config.log_format);

    let seed_count = match parse_seed_count_arg() {
        Ok(count) => count,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    };

    let state = AppState::new();
    let mut generator = EventGenerator::new(42);
    let now = chrono::Utc::now();
    match seed_count {
        Some(count) => seed::seed_count(&state, &mut generator, count, now).await,
        None => seed::seed_default(&state, &mut generator, now).await,
    }

    let seeded = state.seismic_event_count().await;

    let generator_interval = std::time::Duration::from_millis(config.generator_interval_ms);
    tokio::spawn(seed::run_live_generator(state.clone(), generator, generator_interval));

    let api_key: std::sync::Arc<str> = config.api_key.as_str().into();
    let delay = DelayRange { min_ms: config.response_delay_min_ms, max_ms: config.response_delay_max_ms };
    let app = routing::build_router(state, api_key, delay);

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("Failed to bind to {addr}: {err}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        event = "startup",
        host = %addr,
        seed_records = seeded,
        generator_interval_ms = config.generator_interval_ms,
        delay_min_ms = config.response_delay_min_ms,
        delay_max_ms = config.response_delay_max_ms,
        log_level = %config.log_level,
        log_format = %config.log_format,
        "BMKG is running on http://{addr} ({seeded} seismic events seeded)"
    );

    let make_service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
    if let Err(err) = axum::serve(listener, make_service).with_graceful_shutdown(shutdown_signal()).await {
        eprintln!("Server error: {err}");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install a SIGTERM handler");
    let ctrl_c = async { tokio::signal::ctrl_c().await.expect("failed to install a SIGINT (Ctrl+C) handler") };

    tokio::select! {
        _ = terminate.recv() => {}
        _ = ctrl_c => {}
    }
    tracing::info!(event = "shutdown", "received shutdown signal, stopping gracefully");
}

/// A bare number (`cargo run -- 10`) seeds exactly that many records instead of the full default history.
fn parse_seed_count_arg() -> Result<Option<usize>, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => Ok(None),
        [only] => only
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("Invalid seed size '{only}': expected a single non-negative integer")),
        _ => Err(format!("Invalid arguments '{}': expected a single non-negative integer", args.join(" "))),
    }
}
