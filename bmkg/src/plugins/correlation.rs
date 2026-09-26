use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use uuid::Uuid;

pub const CORRELATION_ID_HEADER: &str = "X-Correlation-ID";

/// Carried on the request so anything deeper in the middleware stack (namely the auth check) can
/// log with the same id, and echoed onto the response so the access log and the caller both see it.
#[derive(Debug, Clone)]
pub struct CorrelationId(pub String);

/// Same rule as PVMBG's: short and plain, so a hostile header cannot flood the logs or smuggle odd
/// characters into them. Anything else is replaced by a generated id, never logged.
fn is_valid(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

pub async fn propagate_correlation_id(headers: HeaderMap, mut request: Request, next: Next) -> Response {
    let id = headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_valid(value))
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    request.extensions_mut().insert(CorrelationId(id.clone()));

    let mut response = next.run(request).await;
    // `id` is always alphanumeric/._- or a UUID, so this never fails; skip rather than panic if it somehow did.
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(CORRELATION_ID_HEADER, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_of_one_and_sixty_four_characters_are_valid() {
        assert!(is_valid("x"));
        assert!(is_valid(&"a".repeat(64)));
    }

    #[test]
    fn an_id_of_sixty_five_characters_is_invalid() {
        assert!(!is_valid(&"a".repeat(65)));
    }

    #[test]
    fn an_empty_id_is_invalid() {
        assert!(!is_valid(""));
    }

    #[test]
    fn dots_underscores_and_hyphens_are_allowed() {
        assert!(is_valid("poll-cycle_7f3c.1"));
    }

    #[test]
    fn spaces_slashes_and_quotes_are_rejected() {
        for bad in ["has space", "slash/id", "quote\"id", "{\"level\":\"FAKE\"}"] {
            assert!(!is_valid(bad), "'{bad}' should not be valid");
        }
    }
}
