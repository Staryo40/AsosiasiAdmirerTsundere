use axum::extract::{ConnectInfo, Request};
use axum::middleware::Next;
use axum::response::Response;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::Level;

use crate::plugins::correlation::CORRELATION_ID_HEADER;
use crate::plugins::delay::DelayMs;

const MAX_LOGGED_PATH: usize = 256;

/// One line per request, once the response is ready. Wraps the whole router (via `.layer` in
/// routes.rs, applied outermost of the three cross-cutting layers), so it also sees 404s, 401s and
/// anything the delay/correlation-id layers tagged onto the response. Never logs headers, bodies,
/// or query strings.
pub async fn log_access(ConnectInfo(client_addr): ConnectInfo<SocketAddr>, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path: String = request.uri().path().chars().take(MAX_LOGGED_PATH).collect();
    let started = Instant::now();

    let response = next.run(request).await;

    let status = response.status().as_u16();
    // `tracing-serde`'s JSON visitor has no `record_u128` override, so a `u128` field falls back
    // to `record_debug` and gets serialized as a *string*, not a number; a request never legitimately
    // takes anywhere near u64::MAX milliseconds, so this cast is exact in every real case.
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

    // Runtime level dispatch is not something the `tracing` macros support directly (the level is
    // baked into each callsite's static metadata), hence the small match instead of one dynamic call.
    match access_level(&path, status) {
        Level::DEBUG => log_at!(debug),
        Level::WARN => log_at!(warn),
        Level::ERROR => log_at!(error),
        _ => log_at!(info),
    }

    response
}

/// A healthy health check is expected to be polled constantly, so it is DEBUG. Everything else BMKG
/// can return is either a normal read (INFO) or a rejected/bad request (WARN); BMKG has no outage
/// mode and no server-side failure mode of its own, so there is no real 5xx case to reserve ERROR for
/// short of a genuine bug, which is exactly what ERROR should be reserved for.
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
