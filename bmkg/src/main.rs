mod config;

use config::AppConfig;

#[tokio::main]
async fn main() {
    let config = match AppConfig::load() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    // Temporary: no server yet. This is replaced by the real startup log line once logging is wired up.
    println!("BMKG configuration loaded: {config:?}");
}
