use crate::{AppError, AppState};
use axum::{extract::State, Json};
use kormir::bitcoin::XOnlyPublicKey;
use std::sync::Arc;

pub async fn get_pubkey(
    State(state): State<Arc<AppState>>,
) -> Result<Json<XOnlyPublicKey>, AppError> {
    let pubkey = state.oracle_service.get_pubkey();
    Ok(Json(pubkey))
}
