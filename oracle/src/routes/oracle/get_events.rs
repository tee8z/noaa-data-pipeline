use crate::{AppError, AppState};
use axum::{extract::State, Json};
use kormir::storage::OracleEventData;
use std::sync::Arc;

pub async fn get_events(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<OracleEventData>>, AppError> {
    let events = state.oracle_service.list_events().await?;
    Ok(Json(events))
}
