use axum::extract::{ConnectInfo, Request};
use axum::middleware::Next;
use axum::response::Response;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::Level;

use crate::plugins::correlation::CORRELATION_ID_HEADER;
use crate::plugins::delay::DelayMs;

const MAX_LOGGED_PATH: usize = 256;

/// One line per request, once the response is ready. Never logs headers, bodies, or query strings.
pub async fn log_access(ConnectInfo(client_addr): ConnectInfo<SocketAddr>, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path: String = request.uri().path().chars().take(MAX_LOGGED_PATH).collect();
    let started = Instant::now();

    let response = next.run(request).await;

    let status = response.status().as_u16();
    // Cast to u64: tracing-serde has no record_u128 override, so a u128 field logs as a string.
    let duration_ms = started.elapsed().as_millis() as u64;
    let delay_ms = response.extensions().get::<DelayMs>().map_or(0, |d| d.0);
    let correlation_id = response
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    let client_ip = client_addr.ip().to_string();
    let message = format!("{method} {path} {status} {duration_ms}ms");

    macro_rules! log_at {
        ($level:ident) => {
            tracing::$level!(
                event = "http_request",
                method = %method,
                path = %path,
                status,
                duration_ms,
                delay_ms,
                client_ip = %client_ip,
                correlation_id = %correlation_id,
                "{message}"
            )
        };
    }

    // tracing macros need a compile-time level, hence the match instead of one dynamic call.
    match access_level(&path, status) {
        Level::DEBUG => log_at!(debug),
        Level::WARN => log_at!(warn),
        Level::ERROR => log_at!(error),
        _ => log_at!(info),
    }

    response
}

/// A healthy `/health` is DEBUG (polled constantly); rejections are WARN; ERROR is for a genuine bug.
fn access_level(path: &str, status: u16) -> Level {
    match () {
        _ if path == "/health" && status < 400 => Level::DEBUG,
        _ if status >= 500 => Level::ERROR,
        _ if status >= 400 => Level::WARN,
        _ => Level::INFO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_healthy_health_check_is_debug_but_other_paths_at_200_are_info() {
        assert_eq!(access_level("/health", 200), Level::DEBUG);
        assert_eq!(access_level("/seismic-events", 200), Level::INFO);
    }

    #[test]
    fn client_errors_are_warn_and_server_errors_are_error() {
        assert_eq!(access_level("/seismic-events", 401), Level::WARN);
        assert_eq!(access_level("/seismic-events", 404), Level::WARN);
        assert_eq!(access_level("/seismic-events", 500), Level::ERROR);
    }

    #[test]
    fn a_health_check_that_somehow_fails_is_not_silently_downgraded_to_debug() {
        assert_eq!(access_level("/health", 500), Level::ERROR);
    }
}
