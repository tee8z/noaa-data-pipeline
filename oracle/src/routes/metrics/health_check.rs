use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use crate::utils::error_chain_fmt;

#[derive(thiserror::Error)]
pub enum HealthError {
    #[error(transparent)]
    UnexpectedError(#[from] anyhow::Error),
}

impl std::fmt::Debug for HealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}

// Tell axum how to convert `HealthError` into a response.
impl IntoResponse for HealthError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}

pub async fn health_check() -> Result<StatusCode, HealthError> {
    // TODO (@tee8z): add call to DB when it's needed here
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {


    use super::*;

    #[tokio::test]
    async fn test_ping() {
        assert_eq!(health_check().await.unwrap(), StatusCode::OK);
    }
}
