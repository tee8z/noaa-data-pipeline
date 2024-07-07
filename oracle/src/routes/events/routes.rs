use crate::{
    oracle::{
        CreateEvent, OracleAnnouncement, OracleAttestation, OracleError, OracleEventData, SignEvent,
    },
    AppState,
};
use axum::{
    extract::{Path, State},
    response::{ErrorResponse, IntoResponse, Response},
    Json,
};
use dlctix::bitcoin::XOnlyPublicKey;
use hyper::StatusCode;
use log::error;
use serde_json::json;
use std::{borrow::Borrow, sync::Arc};
use uuid::Uuid;

#[utoipa::path(
    get,
    path = "/oracle/pubkey",
    responses(
        (status = OK, description = "Successfully retrieved oracle's pubkey data", body = XOnlyPublicKey),
    ))]
pub async fn get_pubkey(
    State(state): State<Arc<AppState>>,
) -> Result<Json<XOnlyPublicKey>, ErrorResponse> {
    Ok(Json(state.oracle.public_key()))
}

#[utoipa::path(
    get,
    path = "/oracle/events",
    responses(
        (status = OK, description = "Successfully retrieved oracle events", body = Vec<OracleEventData>),
    ))]
pub async fn list_events(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<OracleEventData>>, ErrorResponse> {
    state.oracle.list_events().await.map(Json).map_err(|e| {
        error!("error retrieving event data: {}", e);
        e.into()
    })
}

#[utoipa::path(
    post,
    path = "/oracle/events",
    request_body = CreateEvent,
    responses(
        (status = OK, description = "Successfully created oracle weather event", body = OracleAnnouncement),
        (status = BAD_REQUEST, description = "Invalid event to be created"),
    ))]
pub async fn create_event(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateEvent>,
) -> Result<Json<OracleAnnouncement>, ErrorResponse> {
    state
        .oracle
        .create_event(body)
        .await
        .map(Json)
        .map_err(|e| {
            error!("error creating event data: {}", e);
            e.into()
        })
}

#[utoipa::path(
    get,
    path = "/oracle/events/{event_id}",
    params(
        ("event_id" = Uuid, Path, description = "ID of a weather event the oracle is tracking"),
    ),
    responses(
        (status = OK, description = "Successfully retrieved event data", body = OracleEventData),
        (status = NOT_FOUND, description = "Event not found for the provided ID"),
    ))]
pub async fn get_event(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<Uuid>,
) -> Result<Json<OracleEventData>, ErrorResponse> {
    state
        .oracle
        .get_event(&event_id)
        .await
        .map(Json)
        .map_err(|e| {
            error!("error event data: {}", e);
            e.into()
        })
}

#[utoipa::path(
    post,
    path = "/oracle/events/{event_id}/sign",
    params(
        ("event_id" = Uuid, Path, description = "ID of a weather event the oracle is tracking"),
    ),
    request_body = SignEvent,
    responses(
        (status = OK, description = "Successfully signed event data", body = OracleAttestation),
        (status = NOT_FOUND, description = "Event not found for the provided ID"),
    ))]
pub async fn sign_event(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<Uuid>,
    Json(body): Json<SignEvent>,
) -> Result<Json<OracleAttestation>, ErrorResponse> {
    state.oracle.sign_event(body).await.map(Json).map_err(|e| {
        error!("error signing event data: {}", e);
        e.into()
    })
}

impl IntoResponse for OracleError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self.borrow() {
            OracleError::EventNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            OracleError::PrivateKey(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            OracleError::MinOutcome(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            OracleError::EventMaturity(_) => (StatusCode::BAD_REQUEST, self.to_string()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
