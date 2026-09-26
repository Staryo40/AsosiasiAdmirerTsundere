use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

/// Every error a handler can return. Each variant maps to one status code and a `{"error": "..."}`
/// JSON body, so failures always look the same shape as PVMBG's, whatever went wrong.
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}
