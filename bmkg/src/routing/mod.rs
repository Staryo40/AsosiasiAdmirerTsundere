use std::sync::Arc;

use axum::extract::{Query, State};
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::plugins::access_log::log_access;
use crate::plugins::auth::require_api_key;
use crate::plugins::correlation::propagate_correlation_id;
use crate::plugins::delay::{add_response_delay, DelayRange};
use crate::plugins::errors::ApiError;
use crate::state::AppState;

// Layer order matters: access log (outermost), correlation id, delay, then auth (innermost).
pub fn build_router(state: AppState, api_key: Arc<str>, delay: DelayRange) -> Router {
    let protected = Router::new()
        .route("/seismic-events", get(seismic_events))
        .route("/tsunami-warnings", get(tsunami_warnings))
        .route_layer(middleware::from_fn_with_state(api_key, require_api_key));

    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
        .layer(middleware::from_fn_with_state(delay, add_response_delay))
        .layer(middleware::from_fn(propagate_correlation_id))
        .layer(middleware::from_fn(log_access))
}

async fn health() -> impl IntoResponse {
    "OK"
}

#[derive(Deserialize)]
pub struct SinceQuery {
    since: Option<String>,
}

// `since` is parsed by hand so a bad value gets our own JSON error, not axum's default rejection text.
fn parse_since(raw: Option<String>) -> Result<Option<DateTime<Utc>>, ApiError> {
    match raw {
        None => Ok(None),
        Some(raw) => DateTime::parse_from_rfc3339(&raw)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(|_| {
                ApiError::BadRequest(format!("'since' must be an ISO-8601 timestamp, e.g. 2026-01-01T00:00:00Z (got '{raw}')"))
            }),
    }
}

async fn seismic_events(State(state): State<AppState>, Query(query): Query<SinceQuery>) -> Result<impl IntoResponse, ApiError> {
    let since = parse_since(query.since)?;
    Ok(Json(state.seismic_events_since(since).await))
}

async fn tsunami_warnings(State(state): State<AppState>, Query(query): Query<SinceQuery>) -> Result<impl IntoResponse, ApiError> {
    let since = parse_since(query.since)?;
    Ok(Json(state.tsunami_warnings_since(since).await))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::auth::API_KEY_HEADER;
    use crate::plugins::correlation::CORRELATION_ID_HEADER;
    use crate::models::{SeismicEvent, ThreatLevel, TsunamiWarning};
    use crate::test_support::LogCapture;
    use chrono::TimeZone;
    use serde_json::Value;
    use tokio::net::TcpListener;

    const TEST_KEY: &str = "test-key-for-routes";

    // Starts a real server on an OS-assigned port; no delay, since these tests are not about timing.
    async fn spawn(state: AppState) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_router(state, Arc::from(TEST_KEY), DelayRange { min_ms: 0, max_ms: 0 });
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// A request to a protected endpoint carrying the correct `X-BMKG-Key`.
    async fn authorized_get(base: &str, path: &str) -> reqwest::Response {
        reqwest::Client::new().get(format!("{base}{path}")).header(API_KEY_HEADER, TEST_KEY).send().await.unwrap()
    }

    // Like `spawn`, but with a real delay, for tests that check delay in the fully composed app.
    async fn spawn_with_delay(state: AppState, delay: DelayRange) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_router(state, Arc::from(TEST_KEY), delay);
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn event(id: &str, occurred_at: DateTime<Utc>) -> SeismicEvent {
        SeismicEvent {
            event_id: id.to_string(),
            magnitude: 5.5,
            depth_km: 10.0,
            epicenter_lat: -6.2,
            epicenter_lon: 106.8,
            region_name: "Selat Sunda".to_string(),
            occurred_at,
            potential_tsunami: false,
        }
    }

    fn warning(id: &str, issued_at: DateTime<Utc>) -> TsunamiWarning {
        TsunamiWarning {
            warning_id: id.to_string(),
            related_event_id: "evt-1".to_string(),
            threat_level: ThreatLevel::Siaga,
            affected_zones: vec!["Pesisir Selat Sunda".to_string()],
            estimated_arrival: issued_at,
            issued_at,
        }
    }

    fn at(minute: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + chrono::Duration::minutes(minute)
    }

    #[tokio::test]
    async fn health_answers_ok() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::get(format!("{base}/health")).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "OK");
    }

    #[tokio::test]
    async fn seismic_events_with_no_since_returns_everything() {
        let state = AppState::new();
        state.add_seismic_event(event("a", at(0))).await;
        state.add_seismic_event(event("b", at(1))).await;
        let base = spawn(state).await;

        let response = authorized_get(&base, "/seismic-events").await;
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn seismic_events_since_filters_strictly_after() {
        let state = AppState::new();
        state.add_seismic_event(event("old", at(0))).await;
        state.add_seismic_event(event("new", at(10))).await;
        let base = spawn(state).await;

        let response = authorized_get(&base, "/seismic-events?since=2026-01-01T00:00:00Z").await;
        let body: Value = response.json().await.unwrap();
        let ids: Vec<&str> = body.as_array().unwrap().iter().map(|e| e["event_id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["new"]);
    }

    #[tokio::test]
    async fn a_malformed_since_is_400_with_a_json_error() {
        let base = spawn(AppState::new()).await;
        let response = authorized_get(&base, "/seismic-events?since=not-a-timestamp").await;

        assert_eq!(response.status(), 400);
        let body: Value = response.json().await.unwrap();
        assert!(body["error"].as_str().unwrap().contains("ISO-8601"), "{body}");
    }

    #[tokio::test]
    async fn a_date_only_since_without_a_time_is_400() {
        let base = spawn(AppState::new()).await;
        let response = authorized_get(&base, "/seismic-events?since=2026-01-01").await;

        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn seismic_event_fields_round_trip_through_json() {
        let state = AppState::new();
        state.add_seismic_event(event("evt-000001", at(0))).await;
        let base = spawn(state).await;

        let response = authorized_get(&base, "/seismic-events").await;
        let body: Value = response.json().await.unwrap();
        let first = &body.as_array().unwrap()[0];
        assert_eq!(first["event_id"], "evt-000001");
        assert_eq!(first["region_name"], "Selat Sunda");
        assert_eq!(first["potential_tsunami"], false);
        assert_eq!(first["occurred_at"], "2026-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn tsunami_warnings_with_no_since_returns_everything() {
        let state = AppState::new();
        state.add_tsunami_warning(warning("tsw-000001", at(0))).await;
        let base = spawn(state).await;

        let response = authorized_get(&base, "/tsunami-warnings").await;
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 1);
        assert_eq!(body[0]["warning_id"], "tsw-000001");
        assert_eq!(body[0]["threat_level"], "Siaga");
    }

    #[tokio::test]
    async fn tsunami_warnings_since_filters_by_issued_at() {
        let state = AppState::new();
        state.add_tsunami_warning(warning("old", at(0))).await;
        state.add_tsunami_warning(warning("new", at(10))).await;
        let base = spawn(state).await;

        let response = authorized_get(&base, "/tsunami-warnings?since=2026-01-01T00:05:00Z").await;
        let body: Value = response.json().await.unwrap();
        let ids: Vec<&str> = body.as_array().unwrap().iter().map(|w| w["warning_id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["new"]);
    }

    #[tokio::test]
    async fn seismic_events_and_tsunami_warnings_are_independent_endpoints() {
        let state = AppState::new();
        state.add_seismic_event(event("evt-1", at(0))).await;
        let base = spawn(state).await;

        let warnings = authorized_get(&base, "/tsunami-warnings").await;
        let body: Value = warnings.json().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 0, "a seismic event alone must not appear as a tsunami warning");
    }

    #[tokio::test]
    async fn health_needs_no_key_at_all() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::get(format!("{base}/health")).await.unwrap();

        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn seismic_events_without_a_key_is_401_with_a_json_error() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::get(format!("{base}/seismic-events")).await.unwrap();

        assert_eq!(response.status(), 401);
        let body: Value = response.json().await.unwrap();
        assert!(body["error"].as_str().is_some(), "{body}");
    }

    #[tokio::test]
    async fn seismic_events_with_the_wrong_key_is_401() {
        let base = spawn(AppState::new()).await;
        let response =
            reqwest::Client::new().get(format!("{base}/seismic-events")).header(API_KEY_HEADER, "wrong-key").send().await.unwrap();

        assert_eq!(response.status(), 401);
    }

    #[tokio::test]
    async fn tsunami_warnings_also_requires_the_key() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::get(format!("{base}/tsunami-warnings")).await.unwrap();

        assert_eq!(response.status(), 401);
    }

    #[tokio::test]
    async fn an_empty_key_header_is_rejected() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::Client::new().get(format!("{base}/seismic-events")).header(API_KEY_HEADER, "").send().await.unwrap();

        assert_eq!(response.status(), 401);
    }

    #[tokio::test]
    async fn a_pvmbg_style_bearer_token_sent_as_the_bmkg_key_is_rejected() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::Client::new()
            .get(format!("{base}/seismic-events"))
            .header(API_KEY_HEADER, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJyZWFkZXIifQ.forged-signature")
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 401);
    }

    #[tokio::test]
    async fn a_key_that_only_differs_by_case_is_rejected() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::Client::new()
            .get(format!("{base}/seismic-events"))
            .header(API_KEY_HEADER, TEST_KEY.to_uppercase())
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 401);
    }

    #[tokio::test]
    async fn the_correct_key_grants_access_to_both_protected_routes() {
        let base = spawn(AppState::new()).await;

        assert_eq!(authorized_get(&base, "/seismic-events").await.status(), 200);
        assert_eq!(authorized_get(&base, "/tsunami-warnings").await.status(), 200);
    }

    #[tokio::test]
    async fn a_request_rejected_for_a_missing_key_is_still_delayed_like_a_real_one() {
        let base = spawn_with_delay(AppState::new(), DelayRange { min_ms: 100, max_ms: 100 }).await;

        let start = std::time::Instant::now();
        let response = reqwest::get(format!("{base}/seismic-events")).await.unwrap();

        assert_eq!(response.status(), 401);
        assert!(start.elapsed().as_millis() >= 90, "a rejected request was not delayed");
    }

    #[tokio::test]
    async fn health_is_delayed_too_in_the_full_composed_app() {
        let base = spawn_with_delay(AppState::new(), DelayRange { min_ms: 100, max_ms: 100 }).await;

        let start = std::time::Instant::now();
        let response = reqwest::get(format!("{base}/health")).await.unwrap();

        assert_eq!(response.status(), 200);
        assert!(start.elapsed().as_millis() >= 90);
    }

    // --- correlation id ---------------------------------------------------

    #[tokio::test]
    async fn a_valid_correlation_id_is_echoed_back() {
        let base = spawn(AppState::new()).await;
        let response =
            reqwest::Client::new().get(format!("{base}/health")).header(CORRELATION_ID_HEADER, "poll-cycle-7f3c").send().await.unwrap();

        assert_eq!(response.headers()[CORRELATION_ID_HEADER], "poll-cycle-7f3c");
    }

    #[tokio::test]
    async fn a_correlation_id_is_generated_when_none_is_sent() {
        let base = spawn(AppState::new()).await;
        let uuid = regex_like_uuid();

        let first = reqwest::get(format!("{base}/health")).await.unwrap();
        let second = reqwest::get(format!("{base}/health")).await.unwrap();
        let first_id = first.headers()[CORRELATION_ID_HEADER].to_str().unwrap().to_string();
        let second_id = second.headers()[CORRELATION_ID_HEADER].to_str().unwrap().to_string();

        assert!(uuid(&first_id), "'{first_id}' does not look like a UUID");
        assert!(uuid(&second_id));
        assert_ne!(first_id, second_id, "every request should get its own generated id");
    }

    #[tokio::test]
    async fn an_invalid_correlation_id_is_replaced_with_a_generated_one() {
        let base = spawn(AppState::new()).await;
        let uuid = regex_like_uuid();

        for bad in ["has space", "slash/id", "{\"level\":\"FAKE\"}", &"a".repeat(65)] {
            let response = reqwest::Client::new().get(format!("{base}/health")).header(CORRELATION_ID_HEADER, bad).send().await.unwrap();
            let echoed = response.headers()[CORRELATION_ID_HEADER].to_str().unwrap();
            assert!(uuid(echoed), "'{bad}' was not replaced, got '{echoed}'");
        }
    }

    fn regex_like_uuid() -> impl Fn(&str) -> bool {
        |s: &str| {
            let bytes = s.as_bytes();
            s.len() == 36
                && bytes[8] == b'-'
                && bytes[13] == b'-'
                && bytes[18] == b'-'
                && bytes[23] == b'-'
                && bytes.iter().enumerate().all(|(i, b)| matches!(i, 8 | 13 | 18 | 23) || b.is_ascii_hexdigit())
        }
    }

    // --- access log ---------------------------------------------------------

    #[tokio::test]
    async fn the_access_log_line_has_the_expected_fields() {
        let capture = LogCapture::new();
        let base = spawn(AppState::new()).await;

        let response =
            reqwest::Client::new().get(format!("{base}/seismic-events")).header(API_KEY_HEADER, TEST_KEY).header(CORRELATION_ID_HEADER, "trace-1").send().await.unwrap();
        assert_eq!(response.status(), 200);

        let line = capture.events_named("http_request").into_iter().find(|l| l["path"] == "/seismic-events").unwrap();
        assert_eq!(line["method"], "GET");
        assert_eq!(line["status"], 200);
        assert_eq!(line["level"], "INFO");
        assert_eq!(line["correlation_id"], "trace-1");
        assert!(line["duration_ms"].as_u64().is_some());
        assert_eq!(line["delay_ms"], 0);
        assert!(!line["client_ip"].as_str().unwrap().is_empty());
        assert!(line["message"].as_str().unwrap().starts_with("GET /seismic-events 200"), "{line}");
    }

    #[tokio::test]
    async fn a_healthy_health_check_is_logged_at_debug_only() {
        let capture = LogCapture::new();
        let base = spawn(AppState::new()).await;

        reqwest::get(format!("{base}/health")).await.unwrap();

        let line = capture.events_named("http_request").into_iter().find(|l| l["path"] == "/health").unwrap();
        assert_eq!(line["level"], "DEBUG");
    }

    #[tokio::test]
    async fn a_rejected_request_is_logged_at_warn_with_the_reason() {
        let capture = LogCapture::new();
        let base = spawn(AppState::new()).await;

        reqwest::Client::new().get(format!("{base}/seismic-events")).header(CORRELATION_ID_HEADER, "trace-missing").send().await.unwrap();
        reqwest::Client::new()
            .get(format!("{base}/tsunami-warnings"))
            .header(API_KEY_HEADER, "wrong")
            .header(CORRELATION_ID_HEADER, "trace-wrong")
            .send()
            .await
            .unwrap();

        let access = capture.events_named("http_request");
        assert!(access.iter().all(|l| l["path"] != "/seismic-events" || l["level"] == "WARN"));

        let rejections = capture.events_named("auth_rejected");
        assert_eq!(rejections.len(), 2);
        assert_eq!(rejections[0]["reason"], "missing_key");
        assert_eq!(rejections[0]["correlation_id"], "trace-missing");
        assert_eq!(rejections[0]["level"], "WARN");
        assert_eq!(rejections[1]["reason"], "invalid_key");
        assert_eq!(rejections[1]["correlation_id"], "trace-wrong");
    }

    #[tokio::test]
    async fn the_configured_key_is_never_written_to_the_log() {
        let capture = LogCapture::new();
        let base = spawn(AppState::new()).await;

        authorized_get(&base, "/seismic-events").await;
        reqwest::Client::new().get(format!("{base}/seismic-events")).header(API_KEY_HEADER, "guessed-wrong-key").send().await.unwrap();

        let raw = format!("{:?}", capture.lines());
        assert!(!raw.contains(TEST_KEY), "the real API key appeared in the logs");
        assert!(!raw.contains("guessed-wrong-key"), "a guessed key appeared in the logs");
    }

    #[tokio::test]
    async fn a_hostile_correlation_id_cannot_forge_or_break_a_log_line() {
        let capture = LogCapture::new();
        let base = spawn(AppState::new()).await;
        // No raw newline: reqwest's own HeaderValue builder refuses to send one.
        let hostile = "x\"y\" {\"level\":\"FAKE\",\"event\":\"http_request\"}";

        reqwest::Client::new().get(format!("{base}/health")).header(CORRELATION_ID_HEADER, hostile).send().await.unwrap();

        // `lines()` already asserts every line parses as JSON; getting here proves nothing was corrupted.
        let line = capture.events_named("http_request").into_iter().find(|l| l["path"] == "/health").unwrap();
        assert_ne!(line["correlation_id"], hostile, "the hostile id should have been replaced");
        assert_eq!(line["level"], "DEBUG");
    }
}
