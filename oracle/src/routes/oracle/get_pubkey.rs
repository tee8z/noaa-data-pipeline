use crate::{AppError, AppState};
use anyhow::{anyhow, Error};
use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
    Json,
};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use slog::{error, trace, Logger};
use std::sync::Arc;
use time::{
    format_description::well_known::Rfc3339, macros::format_description, Date, OffsetDateTime,
};
use tokio::fs;

pub async fn get_pubkey(
        State(state): State<Arc<AppState>>,
    ) -> Result<Json<String>, AppError> {
        //oracle
    Ok(Json(String::from("")))
}