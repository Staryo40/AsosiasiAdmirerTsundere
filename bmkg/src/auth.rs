use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

use crate::errors::ApiError;

pub const API_KEY_HEADER: &str = "X-BMKG-Key";

/// Checks `X-BMKG-Key` against the one configured key. BMKG has a single caller (the BNPB
/// gateway), so unlike PVMBG there is no login step and no per-client scope: the header either
/// matches or it does not. A PVMBG-style Bearer token sent here is just a string that will never
/// match, so it is rejected the same way as any other wrong value.
pub async fn require_api_key(
    State(expected_key): State<Arc<str>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let provided = headers.get(API_KEY_HEADER).and_then(|value| value.to_str().ok());

    let is_valid = match provided {
        Some(key) => key_matches(key, &expected_key),
        None => false,
    };

    if is_valid {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::Unauthorized("missing or invalid API key".to_string()))
    }
}

/// Constant-time comparison, so a caller probing many wrong keys cannot learn anything from how
/// long the check takes. Lengths are compared first only to size the byte comparison; `ct_eq`
/// itself still runs over the full (equal) length either way.
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
