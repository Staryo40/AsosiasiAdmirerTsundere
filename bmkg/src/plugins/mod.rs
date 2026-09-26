//! Cross-cutting axum middleware and the shared error-response type, mirroring PVMBG's
//! `plugins/` package (Auth, CorrelationId, ResponseDelay, AccessLog, StatusPages).
pub mod access_log;
pub mod auth;
pub mod correlation;
pub mod delay;
pub mod errors;
