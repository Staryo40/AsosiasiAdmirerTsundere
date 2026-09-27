use std::sync::Arc;

use axum::extract::{Extension, Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

use crate::plugins::correlation::CorrelationId;
use crate::plugins::errors::ApiError;

pub const API_KEY_HEADER: &str = "X-BMKG-Key";

/// Checks `X-BMKG-Key` against the one configured key. Relies on the correlation-id layer having run first.
pub async fn require_api_key(
    State(expected_key): State<Arc<str>>,
    headers: HeaderMap,
    Extension(CorrelationId(correlation_id)): Extension<CorrelationId>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let provided = headers.get(API_KEY_HEADER).and_then(|value| value.to_str().ok());

    let reason = match provided {
        Some(key) if key_matches(key, &expected_key) => None,
        Some(_) => Some("invalid_key"),
        None => Some("missing_key"),
    };

    match reason {
        None => Ok(next.run(request).await),
        Some(reason) => {
            tracing::warn!(event = "auth_rejected", reason, correlation_id = %correlation_id, "auth_rejected reason={reason}");
            Err(ApiError::Unauthorized("missing or invalid API key".to_string()))
        }
    }
}

/// Constant-time comparison, so probing wrong keys learns nothing from timing.
fn key_matches(provided: &str, expected: &str) -> bool {
    provided.len() == expected.len() && provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_keys_match() {
        assert!(key_matches("same-key", "same-key"));
    }

    #[test]
    fn different_keys_of_the_same_length_do_not_match() {
        assert!(!key_matches("aaaaaaaa", "bbbbbbbb"));
    }

    #[test]
    fn keys_of_different_lengths_do_not_match() {
        assert!(!key_matches("short", "much-longer-key"));
    }

    #[test]
    fn an_empty_provided_key_never_matches_a_real_one() {
        assert!(!key_matches("", "real-key"));
    }
}
