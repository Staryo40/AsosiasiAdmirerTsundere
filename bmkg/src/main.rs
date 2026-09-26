mod config;
mod models;
mod seed;
mod state;

use config::AppConfig;
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

    // Temporary: no server yet. This is replaced by the real startup log line once logging is wired up.
    println!("BMKG configuration loaded: {config:?}; seeded {} seismic events", state.seismic_event_count().await);
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
