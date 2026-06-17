use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use store::StoreError;

/// The single error type every handler returns. `ResponseError` maps it to a status code
/// and a small JSON body, so a handler never panics its way to a 500 — it returns an error.
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("conflict")]
    #[allow(dead_code)] // reserved for Phase 1 idempotency
    Conflict,
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Conflict => StatusCode::CONFLICT,
            ApiError::Internal(_) | ApiError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        // Never leak internal/DB detail to clients; log it server-side instead.
        let public_message = match self {
            ApiError::NotFound => "not found",
            ApiError::BadRequest(m) => m.as_str(),
            ApiError::Unauthorized => "unauthorized",
            ApiError::Conflict => "conflict",
            ApiError::Internal(detail) => {
                tracing::error!(error = %detail, "internal error");
                "internal error"
            }
            ApiError::Store(err) => {
                tracing::error!(error = %err, "store error");
                "internal error"
            }
        };
        HttpResponse::build(self.status_code()).json(serde_json::json!({ "error": public_message }))
    }
}
