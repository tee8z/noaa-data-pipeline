use crate::{
    oracle::{
        AddEventEntry, CreateEvent, OracleAnnouncement, OracleAttestation, OracleError,
        OracleEventData, SignEvent, WeatherEntry,
    },
    AppState,
};
use axum::{
    extract::{Path, State},
    response::{ErrorResponse, IntoResponse, Response},
    Json,
};
use hyper::StatusCode;
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{borrow::Borrow, sync::Arc};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Base64Pubkey {
    /// base64 representation of the compressed DER encoding of the publickey. This consists of a parity
    /// byte at the beginning, which is either `0x02` (even parity) or `0x03` (odd parity),
    /// followed by the big-endian encoding of the point's X-coordinate.
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Pubkey {
    /// nostr npub in string format
    pub key: String,
}

#[utoipa::path(
    get,
    path = "/oracle/pubkey",
    responses(
        (status = OK, description = "Successfully retrieved oracle's pubkey data", body = Base64Pubkey),
    ))]
pub async fn get_pubkey(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Base64Pubkey>, ErrorResponse> {
    Ok(Json(Base64Pubkey {
        key: state.oracle.public_key(),
    }))
}

#[utoipa::path(
    get,
    path = "/oracle/npub",
    responses(
        (status = OK, description = "Successfully retrieved oracle's nostr npub", body = Pubkey),
    ))]
pub async fn get_npub(State(state): State<Arc<AppState>>) -> Result<Json<Pubkey>, ErrorResponse> {
    Ok(Json(Pubkey {
        key: state.oracle.npub()?,
    }))
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
    // TODO: add filtering, the nested entries may end up being a bottleneck
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
        (status = OK, description = "Successfully created oracle weather event", body = OracleEventData),
        (status = BAD_REQUEST, description = "Invalid event to be created"),
    ))]
pub async fn create_event(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateEvent>,
) -> Result<Json<OracleEventData>, ErrorResponse> {
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
    post,
    path = "/oracle/events/entry",
    request_body = AddEventEntry,
    responses(
        (status = OK, description = "Successfully add entry into oracle weather event", body = WeatherEntry),
        (status = BAD_REQUEST, description = "Invalid entry to be created"),
    ))]
pub async fn add_event_entry(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddEventEntry>,
) -> Result<Json<WeatherEntry>, ErrorResponse> {
    state
        .oracle
        .add_event_entry(body)
        .await
        .map(Json)
        .map_err(|e| {
            error!("error adding entry to event: {}", e);
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

impl IntoResponse for OracleError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self.borrow() {
            OracleError::EventNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            OracleError::PrivateKey(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            OracleError::ConvertKey(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            OracleError::Base32Key(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            OracleError::MinOutcome(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            OracleError::EventMaturity(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            OracleError::DataQuery(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            OracleError::MismatchPubkey(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
