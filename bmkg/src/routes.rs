use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::errors::ApiError;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/seismic-events", get(seismic_events))
        .route("/tsunami-warnings", get(tsunami_warnings))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    "OK"
}

#[derive(Deserialize)]
pub struct SinceQuery {
    since: Option<String>,
}

/// Parsed by hand (not by axum's query deserializer) so a bad value gets our own `{"error": ...}`
/// body and message, matching PVMBG's `since` handling, instead of axum's default rejection text.
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
    use crate::models::{SeismicEvent, ThreatLevel, TsunamiWarning};
    use chrono::TimeZone;
    use serde_json::Value;
    use tokio::net::TcpListener;

    /// Starts a real server on an OS-assigned port and returns its base URL, mirroring how
    /// PVMBG's tests run the whole app rather than poking at handlers directly.
    async fn spawn(state: AppState) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
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

        let response = reqwest::get(format!("{base}/seismic-events")).await.unwrap();
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

        let response = reqwest::get(format!("{base}/seismic-events?since=2026-01-01T00:00:00Z")).await.unwrap();
        let body: Value = response.json().await.unwrap();
        let ids: Vec<&str> = body.as_array().unwrap().iter().map(|e| e["event_id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["new"]);
    }

    #[tokio::test]
    async fn a_malformed_since_is_400_with_a_json_error() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::get(format!("{base}/seismic-events?since=not-a-timestamp")).await.unwrap();

        assert_eq!(response.status(), 400);
        let body: Value = response.json().await.unwrap();
        assert!(body["error"].as_str().unwrap().contains("ISO-8601"), "{body}");
    }

    #[tokio::test]
    async fn a_date_only_since_without_a_time_is_400() {
        let base = spawn(AppState::new()).await;
        let response = reqwest::get(format!("{base}/seismic-events?since=2026-01-01")).await.unwrap();

        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn seismic_event_fields_round_trip_through_json() {
        let state = AppState::new();
        state.add_seismic_event(event("evt-000001", at(0))).await;
        let base = spawn(state).await;

        let response = reqwest::get(format!("{base}/seismic-events")).await.unwrap();
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

        let response = reqwest::get(format!("{base}/tsunami-warnings")).await.unwrap();
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

        let response = reqwest::get(format!("{base}/tsunami-warnings?since=2026-01-01T00:05:00Z")).await.unwrap();
        let body: Value = response.json().await.unwrap();
        let ids: Vec<&str> = body.as_array().unwrap().iter().map(|w| w["warning_id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["new"]);
    }

    #[tokio::test]
    async fn seismic_events_and_tsunami_warnings_are_independent_endpoints() {
        let state = AppState::new();
        state.add_seismic_event(event("evt-1", at(0))).await;
        let base = spawn(state).await;

        let warnings = reqwest::get(format!("{base}/tsunami-warnings")).await.unwrap();
        let body: Value = warnings.json().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 0, "a seismic event alone must not appear as a tsunami warning");
    }
}
