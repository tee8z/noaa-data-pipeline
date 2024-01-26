use crate::{AppError, AppState};
use axum::{
    extract::{Path, State},
    Json,
};
use kormir::storage::OracleEventData;
use std::sync::Arc;

pub async fn get_event(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u32>,
) -> Result<Json<Option<OracleEventData>>, AppError> {
    let events = state.oracle_service.get_event(id).await?;
    Ok(Json(events))
}
