use crate::{AppError, AppState};
use axum::{
    extract::State,
    Json,
};
use std::sync::Arc;

pub async fn create_enum_event(State(state): State<Arc<AppState>>) -> Result<Json<String>, AppError> {
    //oracle
    Ok(Json(String::from("")))
}
