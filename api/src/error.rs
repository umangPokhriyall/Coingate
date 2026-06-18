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
    Conflict,
    /// Idempotency-key replay against an in-progress request whose lease has not expired:
    /// `409 Conflict` + `Retry-After: <seconds>` (Stripe semantics — never block, never
    /// double-execute). Carries the remaining lease in seconds.
    #[error("conflict: retry after {0}s")]
    RetryAfter(u64),
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl ApiError {
    /// Construct a [`ApiError::RetryAfter`] from a remaining-lease duration in seconds.
    pub fn retry_after(seconds: u64) -> Self {
        ApiError::RetryAfter(seconds)
    }
}

/// A failure inside the sans-IO idempotency store (a pool/diesel error or a malformed key row)
/// is an internal server error — logged server-side, opaque to the client.
impl From<idempotency::StoreError> for ApiError {
    fn from(e: idempotency::StoreError) -> Self {
        ApiError::Internal(format!("idempotency store: {e}"))
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Conflict | ApiError::RetryAfter(_) => StatusCode::CONFLICT,
            ApiError::Internal(_) | ApiError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        // 409 + Retry-After: the one error that carries a header.
        if let ApiError::RetryAfter(seconds) = self {
            return HttpResponse::build(StatusCode::CONFLICT)
                .insert_header(("Retry-After", seconds.to_string()))
                .json(serde_json::json!({ "error": "conflict", "retry_after": seconds }));
        }

        // Never leak internal/DB detail to clients; log it server-side instead.
        let public_message = match self {
            ApiError::NotFound => "not found",
            ApiError::BadRequest(m) => m.as_str(),
            ApiError::Unauthorized => "unauthorized",
            ApiError::Conflict => "conflict",
            // Handled above with its Retry-After header; arm kept for exhaustiveness.
            ApiError::RetryAfter(_) => "conflict",
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
