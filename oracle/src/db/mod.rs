use std::collections::BTreeMap;

use anyhow::anyhow;
use dlctix::bitcoin::{hashes::sha256, XOnlyPublicKey};
use dlctix::musig2::secp256k1::schnorr::Signature;
use dlctix::musig2::secp256k1::{Message, PublicKey};
use dlctix::secp::{MaybeScalar, Scalar};
use dlctix::EventAnnouncement;
use duckdb::arrow::datatypes::ToByteSlice;
use duckdb::types::{OrderedMap, ToSqlOutput, Type, Value};
use duckdb::{ffi, ErrorCode, Row, ToSql};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, Duration, OffsetDateTime, UtcOffset};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub mod event_data;
pub mod event_db_migrations;
pub mod outcome_generator;
pub mod weather_data;

pub use event_data::*;
pub use event_db_migrations::*;
pub use outcome_generator::*;
pub use weather_data::{Forecast, Observation, Station, WeatherData};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateEvent {
    /// Client needs to provide a valid Uuidv7
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    /// Time at which the attestation will be added to the event, needs to be after the observation date
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    /// Date of when the weather observations occured (midnight UTC), all entries must be made before this time
    pub observation_date: OffsetDateTime,
    /// NOAA observation stations used in this event
    pub locations: Vec<String>,
    /// The number of values that can be selected per entry in the event (default to number_of_locations * 3, (temp_low, temp_high, wind_speed))
    pub number_of_values_per_entry: usize,
    /// Total number of allowed entries into the event
    pub total_allowed_entries: usize,
    /// Total amount of places that are part of the winnings split
    pub number_of_places_win: usize,
    /// Add a coordinator that will use the event entries in a competition
    pub coordinator: Option<CoordinatorInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateEventMessage {
    /// Client needs to provide a valid Uuidv7
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    /// Time at which the attestation will be added to the event, needs to be after the observation date
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    /// Date of when the weather observations occured (midnight UTC), all entries must be made before this time
    pub observation_date: OffsetDateTime,
    /// NOAA observation stations used in this event
    pub locations: Vec<String>,
    /// The number of values that can be selected per entry in the event (default to number_of_locations * 3, (temp_low, temp_high, wind_speed))
    pub number_of_values_per_entry: usize,
    /// Total number of allowed entries into the event
    pub total_allowed_entries: usize,
    /// Total amount of places that are part of the winnings split
    pub number_of_places_win: usize,
}

impl CreateEventMessage {
    pub fn message(&self) -> Result<Message, serde_json::Error> {
        let message_str = serde_json::to_string(self)?;
        let message = Message::from_hashed_data::<sha256::Hash>(message_str.as_bytes());
        Ok(message)
    }
}

impl From<CreateEvent> for CreateEventMessage {
    fn from(value: CreateEvent) -> Self {
        Self {
            id: value.id,
            signing_date: value.signing_date,
            observation_date: value.observation_date,
            locations: value.locations,
            number_of_values_per_entry: value.number_of_values_per_entry,
            total_allowed_entries: value.total_allowed_entries,
            number_of_places_win: value.number_of_places_win,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CoordinatorInfo {
    /// The pubkey of the coordinator
    pub pubkey: String,
    /// The values of the payload signed by the coordinator
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEventData {
    /// Provide UUIDv7 to use for looking up the event
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    /// Date of when the weather observations occured (midnight UTC), all entries must be made before this time
    pub observation_date: OffsetDateTime,
    // NOAA observation stations used in this event
    pub locations: Vec<String>,
    /// The number of values that can be selected per entry in the event (default to number_of_locations * 3, (temp_low, temp_high, wind_speed))
    pub number_of_values_per_entry: i64,
    pub total_allowed_entries: i64,
    pub number_of_places_win: i64,
    /// Used to sign the result of the event being watched
    pub nonce: Scalar,
    /// Used in constructing the dlctix transactions
    pub event_annoucement: EventAnnouncement,
    /// The pubkey of the coordinator
    pub coordinator_pubkey: Option<String>,
}

impl CreateEventData {
    pub fn new(oracle_pubkey: PublicKey, event: CreateEvent) -> Result<Self, anyhow::Error> {
        if event.id.get_version_num() != 7 {
            return Err(anyhow!(
                "Client needs to provide a valid Uuidv7 for event id {}",
                event.id
            ));
        }
        if event.observation_date > event.signing_date {
            return Err(anyhow::anyhow!(
                "Signing date {} needs to be after observation date {}",
                event.signing_date.format(&Rfc3339).unwrap(),
                event.observation_date.format(&Rfc3339).unwrap()
            ));
        }

        let possible_user_outcomes: Vec<BTreeMap<usize, Vec<usize>>> = generate_outcome_matrix(
            event.number_of_values_per_entry,
            event.number_of_places_win,
            event.total_allowed_entries,
        );
        info!("user outcomes: {:?}", possible_user_outcomes);

        let outcome_messages: Vec<Vec<u8>> = generate_outcome_messages(possible_user_outcomes);

        let mut rng = rand::thread_rng();
        let nonce = Scalar::random(&mut rng);
        let nonce_point = nonce.base_point_mul();
        // Manually set expiry to 7 days after the signature should have been proveded so users can get their funds back
        let expiry = event
            .signing_date
            .saturating_add(Duration::DAY * 7)
            .unix_timestamp() as u32;

        // The actual accounement the oracle is going to attest the outcome
        let event_annoucement = EventAnnouncement {
            oracle_pubkey: oracle_pubkey.into(),
            nonce_point,
            outcome_messages,
            expiry: Some(expiry),
        };

        Ok(Self {
            id: event.id,
            observation_date: event.observation_date,
            signing_date: event.signing_date,
            nonce,
            total_allowed_entries: event.total_allowed_entries as i64,
            number_of_places_win: event.number_of_places_win as i64,
            number_of_values_per_entry: event.number_of_values_per_entry as i64,
            locations: event.clone().locations,
            event_annoucement,
            coordinator_pubkey: event
                .coordinator
                .map(|v| Some(v.pubkey))
                .unwrap_or_default(),
        })
    }
}

/// Validates the received messages was created by the provided pubkey
pub fn validate(message: Message, pubkey_str: &str, signature: &str) -> Result<(), anyhow::Error> {
    info!("pubkey: {} signature: {}", pubkey_str, signature);
    let raw_signature: Vec<u8> = hex::decode(signature).unwrap();
    let sig: Signature = Signature::from_slice(raw_signature.as_slice())
        .map_err(|e| anyhow!("invalid signature: {}", e))?;
    let raw_pubkey: Vec<u8> = hex::decode(pubkey_str).unwrap();
    let pubkey: XOnlyPublicKey = XOnlyPublicKey::from_slice(raw_pubkey.as_slice())
        .map_err(|e| anyhow!("invalid pubkey: {}", e))?;
    sig.verify(&message, &pubkey).map_err(|e| {
        anyhow!(
            "invalid signature {} for pubkey {} {}",
            signature,
            pubkey,
            e
        )
    })?;
    Ok(())
}

impl From<CreateEventData> for Event {
    fn from(value: CreateEventData) -> Self {
        Self {
            id: value.id,
            signing_date: value.signing_date,
            observation_date: value.observation_date,
            locations: value.locations,
            total_allowed_entries: value.total_allowed_entries,
            number_of_places_win: value.number_of_places_win,
            number_of_values_per_entry: value.number_of_values_per_entry,
            event_annoucement: value.event_annoucement,
            nonce: value.nonce,
            status: EventStatus::default(),
            entry_ids: vec![],
            entries: vec![],
            weather: vec![],
            attestation: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
pub struct EventFilter {
    // TODO: add more options, proper pagination and search
    pub limit: Option<usize>,
    pub event_ids: Option<Vec<Uuid>>,
}

impl Default for EventFilter {
    fn default() -> Self {
        Self {
            limit: Some(100_usize),
            event_ids: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SignEvent {
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub observation_date: OffsetDateTime,
    pub status: EventStatus,
    pub nonce: Scalar,
    pub event_annoucement: EventAnnouncement,
    pub number_of_places_win: i64,
    pub number_of_values_per_entry: i64,
    pub attestation: Option<MaybeScalar>,
}

impl SignEvent {
    pub fn update_status(&mut self) {
        self.status = get_status(self.observation_date, self.attestation)
    }
}

impl<'a> TryFrom<&Row<'a>> for SignEvent {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        //raw date format 2024-08-11 00:27:39.013046-04
        let sql_time_format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]"
        );
        let mut sign_events = SignEvent {
            id: row
                .get::<usize, String>(0)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            signing_date: row
                .get::<usize, String>(1)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            observation_date: row
                .get::<usize, String>(2)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            status: EventStatus::default(),
            number_of_places_win: row.get::<usize, i64>(3)?,
            number_of_values_per_entry: row.get::<usize, i64>(4)?,
            attestation: row
                .get::<usize, Value>(5)
                .map(|v| {
                    let blob_attestation = match v {
                        Value::Blob(raw) => raw,
                        _ => vec![],
                    };
                    if !blob_attestation.is_empty() {
                        //TODO: handle the conversion more gracefully than unwrap
                        Some(MaybeScalar::from_slice(blob_attestation.to_byte_slice()).unwrap())
                    } else {
                        None
                    }
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(5, Type::Any, Box::new(e)))?,
            nonce: row
                .get::<usize, Value>(6)
                .map(|raw| {
                    let blob = match raw {
                        Value::Blob(val) => val,
                        _ => vec![],
                    };
                    serde_json::from_slice(&blob)
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(6, Type::Any, Box::new(e)))?,
            event_annoucement: row
                .get::<usize, Value>(7)
                .map(|raw| {
                    let blob = match raw {
                        Value::Blob(val) => val,
                        _ => vec![],
                    };
                    serde_json::from_slice(&blob)
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(7, Type::Any, Box::new(e)))?,
        };
        sign_events.update_status();
        Ok(sign_events)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ActiveEvent {
    pub id: Uuid,
    pub locations: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub observation_date: OffsetDateTime,
    pub status: EventStatus,
    pub total_allowed_entries: i64,
    pub total_entries: i64,
    pub number_of_values_per_entry: i64,
    pub number_of_places_win: i64,
    pub attestation: Option<MaybeScalar>,
}

impl ActiveEvent {
    pub fn update_status(&mut self) {
        self.status = get_status(self.observation_date, self.attestation)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub enum EventStatus {
    /// Observation date has not passed yet and entries can be added
    #[default]
    Live,
    /// Currently in the Observation date, entries cannot be added
    Running,
    /// Event Observation window has finished, not yet signed
    Completed,
    /// Event has completed and been signed by the oracle
    Signed,
}

impl std::fmt::Display for EventStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Signed => write!(f, "signed"),
        }
    }
}

impl TryFrom<&str> for EventStatus {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "live" => Ok(EventStatus::Live),
            "running" => Ok(EventStatus::Running),
            "completed" => Ok(EventStatus::Completed),
            "signed" => Ok(EventStatus::Signed),
            val => Err(anyhow!("invalid status: {}", val)),
        }
    }
}

impl TryFrom<String> for EventStatus {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_str() {
            "live" => Ok(EventStatus::Live),
            "running" => Ok(EventStatus::Running),
            "completed" => Ok(EventStatus::Completed),
            "signed" => Ok(EventStatus::Signed),
            val => Err(anyhow!("invalid status: {}", val)),
        }
    }
}

impl<'a> TryFrom<&Row<'a>> for ActiveEvent {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        //raw date format 2024-08-11 00:27:39.013046-04
        let sql_time_format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]"
        );
        let mut active_events = ActiveEvent {
            id: row
                .get::<usize, String>(0)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            signing_date: row
                .get::<usize, String>(1)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            observation_date: row
                .get::<usize, String>(2)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            locations: row
                .get::<usize, Value>(3)
                .map(|locations| {
                    let list_locations = match locations {
                        Value::List(list) => list,
                        _ => vec![],
                    };
                    let mut locations_conv = vec![];
                    for value in list_locations.iter() {
                        if let Value::Text(location) = value {
                            locations_conv.push(location.clone())
                        }
                    }
                    locations_conv
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(3, Type::Any, Box::new(e)))?,
            total_allowed_entries: row.get::<usize, i64>(4)?,
            status: EventStatus::default(),
            total_entries: row.get::<usize, i64>(5)?,
            number_of_places_win: row.get::<usize, i64>(6)?,
            number_of_values_per_entry: row.get::<usize, i64>(7)?,
            attestation: row
                .get::<usize, Value>(8)
                .map(|v| {
                    let blob_attestation = match v {
                        Value::Blob(raw) => raw,
                        _ => vec![],
                    };
                    if !blob_attestation.is_empty() {
                        //TODO: handle the conversion more gracefully than unwrap
                        Some(MaybeScalar::from_slice(blob_attestation.to_byte_slice()).unwrap())
                    } else {
                        None
                    }
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(8, Type::Any, Box::new(e)))?,
        };
        active_events.update_status();
        Ok(active_events)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct EventSummary {
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    /// Date of when the weather observations occured
    pub observation_date: OffsetDateTime,
    /// NOAA observation stations used in this event
    pub locations: Vec<String>,
    /// The number of values that can be selected per entry in the event (default to number_of_locations * 3, (temp_low, temp_high, wind_speed))
    pub number_of_values_per_entry: i64,
    /// Current status of the event, where in the lifecyle are we (LIVE, RUNNING, COMPLETED, SIGNED, defaults to LIVE)
    pub status: EventStatus,
    /// Knowing the total number of entries, how many can place
    /// The dlctix coordinator can determine how many transactions to create
    pub total_allowed_entries: i64,
    /// Needs to all be generated at the start
    pub total_entries: i64,
    pub number_of_places_win: i64,
    /// The forecasted and observed values for each station on the event date
    pub weather: Vec<Weather>,
    /// When added it means the oracle has signed that the current data is the final result
    pub attestation: Option<MaybeScalar>,
    /// Used to sign the result of the event being watched
    pub nonce: Scalar,
}

impl EventSummary {
    pub fn update_status(&mut self) {
        self.status = get_status(self.observation_date, self.attestation)
    }
}

pub fn get_status(
    observation_date: OffsetDateTime,
    attestation: Option<MaybeScalar>,
) -> EventStatus {
    //always have the events run for a single day for now
    if observation_date < OffsetDateTime::now_utc()
        && observation_date.saturating_sub(Duration::days(1)) > OffsetDateTime::now_utc()
        && attestation.is_none()
    {
        return EventStatus::Running;
    }

    if observation_date < OffsetDateTime::now_utc()
        && observation_date.saturating_sub(Duration::days(1)) < OffsetDateTime::now_utc()
        && attestation.is_none()
    {
        return EventStatus::Completed;
    }

    if attestation.is_some() {
        return EventStatus::Signed;
    }
    //default to live
    EventStatus::Live
}

impl<'a> TryFrom<&Row<'a>> for EventSummary {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        //raw date format 2024-08-11 00:27:39.013046-04
        let sql_time_format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]"
        );
        let mut event_summary = EventSummary {
            id: row
                .get::<usize, String>(0)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            signing_date: row
                .get::<usize, String>(1)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            observation_date: row
                .get::<usize, String>(2)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            status: EventStatus::default(),
            locations: row
                .get::<usize, Value>(3)
                .map(|locations| {
                    let list_locations = match locations {
                        Value::List(list) => list,
                        _ => vec![],
                    };
                    let mut locations_conv = vec![];
                    for value in list_locations.iter() {
                        if let Value::Text(location) = value {
                            locations_conv.push(location.clone())
                        }
                    }
                    locations_conv
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(3, Type::Any, Box::new(e)))?,
            total_allowed_entries: row.get::<usize, i64>(4)?,
            total_entries: row.get::<usize, i64>(5)?,
            number_of_places_win: row.get::<usize, i64>(6)?,
            number_of_values_per_entry: row.get::<usize, i64>(7)?,
            attestation: row
                .get::<usize, Value>(8)
                .map(|v| {
                    let blob_attestation = match v {
                        Value::Blob(raw) => raw,
                        _ => vec![],
                    };
                    if !blob_attestation.is_empty() {
                        //TODO: handle the conversion more gracefully than unwrap
                        Some(MaybeScalar::from_slice(blob_attestation.to_byte_slice()).unwrap())
                    } else {
                        None
                    }
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(8, Type::Any, Box::new(e)))?,
            nonce: row
                .get::<usize, Value>(9)
                .map(|raw| {
                    let blob = match raw {
                        Value::Blob(val) => val,
                        _ => vec![],
                    };
                    serde_json::from_slice(&blob)
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(9, Type::Any, Box::new(e)))?,
            weather: vec![],
        };
        event_summary.update_status();
        Ok(event_summary)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct Event {
    pub id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    /// Date of when the weather observations occured
    pub observation_date: OffsetDateTime,
    /// NOAA observation stations used in this event
    pub locations: Vec<String>,
    /// The number of values that can be selected per entry in the event (default to number_of_locations * 3, (temp_low, temp_high, wind_speed))
    pub number_of_values_per_entry: i64,
    /// Current status of the event, where in the lifecyle are we (LIVE, RUNNING, COMPLETED, SIGNED)
    pub status: EventStatus,
    /// Knowing the total number of entries, how many can place
    /// The dlctix coordinator can determine how many transactions to create
    pub total_allowed_entries: i64,
    /// Needs to all be generated at the start
    pub entry_ids: Vec<Uuid>,
    pub number_of_places_win: i64,
    /// All entries into this event, wont be returned until date of observation begins and will be ranked by score
    pub entries: Vec<WeatherEntry>,
    /// The forecasted and observed values for each station on the event date
    pub weather: Vec<Weather>,
    /// Nonce the oracle committed to use as part of signing final results
    pub nonce: Scalar,
    /// Holds the predefined outcomes the oracle will attest to at event complet
    pub event_annoucement: EventAnnouncement,
    /// When added it means the oracle has signed that the current data is the final result
    pub attestation: Option<MaybeScalar>,
}

impl Event {
    pub fn update_status(&mut self) {
        self.status = get_status(self.observation_date, self.attestation)
    }
}

impl<'a> TryFrom<&Row<'a>> for Event {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        //raw date format 2024-08-11 00:27:39.013046-04
        let sql_time_format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond]]][offset_hour]"
        );
        let mut oracle_event_data = Event {
            id: row
                .get::<usize, String>(0)
                .map(|val| {
                    debug!("{}", val.to_string());
                    Uuid::parse_str(&val)
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            signing_date: row
                .get::<usize, String>(1)
                .map(|val| {
                    debug!("{}", val.to_string());
                    OffsetDateTime::parse(&val, &sql_time_format)
                })?
                .map(|val| {
                    debug!("{}", val.to_string());
                    val.to_offset(UtcOffset::UTC)
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            observation_date: row
                .get::<usize, String>(2)
                .map(|val| OffsetDateTime::parse(&val, &sql_time_format))?
                .map(|val| val.to_offset(UtcOffset::UTC))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            event_annoucement: row
                .get::<usize, Value>(3)
                .map(|raw| {
                    let blob = match raw {
                        Value::Blob(val) => val,
                        _ => vec![],
                    };
                    serde_json::from_slice(&blob)
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(3, Type::Any, Box::new(e)))?,
            locations: row
                .get::<usize, Value>(4)
                .map(|locations| {
                    let list_locations = match locations {
                        Value::List(list) => list,
                        _ => vec![],
                    };
                    let mut locations_conv = vec![];
                    for value in list_locations.iter() {
                        if let Value::Text(location) = value {
                            locations_conv.push(location.clone())
                        }
                    }
                    info!("locations: {:?}", locations_conv);
                    locations_conv
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(4, Type::Any, Box::new(e)))?,
            total_allowed_entries: row.get::<usize, i64>(5)?,
            number_of_places_win: row.get::<usize, i64>(6)?,
            number_of_values_per_entry: row.get::<usize, i64>(7)?,
            attestation: row
                .get::<usize, Value>(8)
                .map(|v| {
                    info!("val: {:?}", v);
                    let blob_attestation = match v {
                        Value::Blob(raw) => raw,
                        _ => vec![],
                    };
                    if !blob_attestation.is_empty() {
                        //TODO: handle the conversion more gracefully than unwrap
                        let converted: MaybeScalar =
                            serde_json::from_slice(&blob_attestation).unwrap();
                        Some(converted)
                    } else {
                        None
                    }
                })
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(8, Type::Any, Box::new(e)))?,
            nonce: row
                .get::<usize, Value>(9)
                .map(|raw| {
                    let blob = match raw {
                        Value::Blob(val) => val,
                        _ => vec![],
                    };
                    serde_json::from_slice(&blob)
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(9, Type::Any, Box::new(e)))?,
            status: EventStatus::default(),
            //These nested values have to be made by more quries
            entry_ids: vec![],
            entries: vec![],
            weather: vec![],
        };
        oracle_event_data.update_status();
        Ok(oracle_event_data)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct Weather {
    pub station_id: String,
    pub observed: Option<Observed>,
    pub forecasted: Forecasted,
}

impl<'a> TryFrom<&Row<'a>> for Weather {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let observed: Option<Observed> = row
            .get::<usize, Value>(1)
            .map(|raw_observed| match raw_observed.clone() {
                Value::Struct(observed) => Some(observed.try_into().map_err(|e: anyhow::Error| {
                    duckdb::Error::DuckDBFailure(
                        ffi::Error {
                            code: ErrorCode::TypeMismatch,
                            extended_code: 0,
                        },
                        Some(format!(
                            "error formatting observed: {:?} {}",
                            raw_observed, e
                        )),
                    )
                })),
                _ => None,
            })
            .and_then(|option_inner_result| match option_inner_result {
                Some(inner_result) => inner_result.map(Some),
                None => Ok(None),
            })?;

        let forecasted: Forecasted =
            row.get::<usize, Value>(2)
                .map(|raw_forecasted| match raw_forecasted.clone() {
                    Value::Struct(forecasted) => {
                        forecasted.try_into().map_err(|e: anyhow::Error| {
                            duckdb::Error::DuckDBFailure(
                                ffi::Error {
                                    code: ErrorCode::TypeMismatch,
                                    extended_code: 0,
                                },
                                Some(format!(
                                    "error formatting forecast: {:?} {}",
                                    raw_forecasted, e
                                )),
                            )
                        })
                    }
                    _ => Err(duckdb::Error::DuckDBFailure(
                        ffi::Error {
                            code: ErrorCode::TypeMismatch,
                            extended_code: 0,
                        },
                        None,
                    )),
                })??;
        Ok(Weather {
            station_id: row.get::<usize, String>(0)?,
            forecasted,
            observed,
        })
    }
}

impl TryFrom<&Forecast> for Forecasted {
    type Error = weather_data::Error;
    fn try_from(value: &Forecast) -> Result<Forecasted, Self::Error> {
        let format = format_description!("[year]-[month]-[day]");
        let date = Date::parse(&value.date, format)?;
        let datetime = date.with_hms(0, 0, 0).unwrap();
        let datetime_off = datetime.assume_offset(UtcOffset::from_hms(0, 0, 0).unwrap());
        Ok(Self {
            date: datetime_off,
            temp_low: value.temp_low,
            temp_high: value.temp_high,
            wind_speed: value.wind_speed,
        })
    }
}

impl TryInto<Weather> for &OrderedMap<String, Value> {
    type Error = duckdb::Error;

    fn try_into(self) -> Result<Weather, Self::Error> {
        let values: Vec<&Value> = self.values().collect();

        let station_id = values
            .first()
            .ok_or_else(|| {
                duckdb::Error::DuckDBFailure(
                    ffi::Error {
                        code: ErrorCode::TypeMismatch,
                        extended_code: 0,
                    },
                    Some(String::from("unable to convert station_id")),
                )
            })
            .and_then(|raw_station| match raw_station {
                Value::Text(station) => Ok(station.clone()),
                _ => Err(duckdb::Error::DuckDBFailure(
                    ffi::Error {
                        code: ErrorCode::TypeMismatch,
                        extended_code: 0,
                    },
                    Some(format!(
                        "error converting station id into string: {:?}",
                        raw_station
                    )),
                )),
            })?;
        let observed: Option<Observed> = if let Some(Value::Struct(observed)) = values.get(1) {
            let observed_converted = observed.try_into().map_err(|e| {
                duckdb::Error::DuckDBFailure(
                    ffi::Error {
                        code: ErrorCode::TypeMismatch,
                        extended_code: 0,
                    },
                    Some(format!("error converting observed: {}", e)),
                )
            })?;
            Some(observed_converted)
        } else {
            None
        };
        let forecasted = values
            .get(2)
            .ok_or_else(|| anyhow!("forecasted not found in the map"))
            .and_then(|raw_forecasted| match raw_forecasted {
                Value::Struct(forecasted) => forecasted.try_into(),
                _ => Err(anyhow!(
                    "error converting forecasted into struct: {:?}",
                    raw_forecasted
                )),
            })
            .map_err(|e| {
                duckdb::Error::DuckDBFailure(
                    ffi::Error {
                        code: ErrorCode::TypeMismatch,
                        extended_code: 0,
                    },
                    Some(e.to_string()),
                )
            })?;
        Ok(Weather {
            station_id,
            observed,
            forecasted,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct Observed {
    #[serde(with = "time::serde::rfc3339")]
    pub date: OffsetDateTime,
    pub temp_low: i64,
    pub temp_high: i64,
    pub wind_speed: i64,
}

impl TryFrom<&Observation> for Observed {
    type Error = weather_data::Error;
    fn try_from(value: &Observation) -> Result<Observed, Self::Error> {
        Ok(Self {
            date: OffsetDateTime::parse(&value.start_time, &Rfc3339)?,
            temp_low: value.temp_low.round() as i64,
            temp_high: value.temp_high.round() as i64,
            wind_speed: value.wind_speed,
        })
    }
}

impl TryInto<Observed> for &OrderedMap<String, Value> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Observed, Self::Error> {
        debug!("raw observed: {:?}", self);
        let values: Vec<&Value> = self.values().collect();
        let date = values
            .first()
            .ok_or_else(|| anyhow!("date not found in the map"))
            .and_then(|raw_date| match raw_date {
                Value::Timestamp(duckdb::types::TimeUnit::Microsecond, raw_date) => Ok(raw_date),
                v => Err(anyhow!(
                    "error converting observed date into OffsetDatetime: {:?}, {:?}",
                    raw_date,
                    v
                )),
            })
            .and_then(|timestamp| {
                OffsetDateTime::from_unix_timestamp_nanos((*timestamp as i128) * 1000_i128).map_err(
                    |e| {
                        anyhow!(
                            "error parsing observed date into offsetdatetime: {} {}",
                            timestamp,
                            e
                        )
                    },
                )
            })
            .map(|val| val.to_offset(UtcOffset::UTC))?;

        let temp_low = values
            .get(1)
            .ok_or_else(|| anyhow!("temp_low not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let temp_high = values
            .get(2)
            .ok_or_else(|| anyhow!("temp_high not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let wind_speed = values
            .get(3)
            .ok_or_else(|| anyhow!("wind_speed not found in the map"))
            .and_then(|raw_speed| match raw_speed {
                Value::Int(speed) => Ok(*speed as i64),
                _ => Err(anyhow!(
                    "error converting wind_speed into int: {:?}",
                    raw_speed
                )),
            })?;

        Ok(Observed {
            date,
            temp_low,
            temp_high,
            wind_speed,
        })
    }
}

impl TryInto<Observed> for OrderedMap<String, Value> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Observed, Self::Error> {
        debug!("raw observed: {:?}", self);
        let values: Vec<&Value> = self.values().collect();
        let date = values
            .first()
            .ok_or_else(|| anyhow!("date not found in the map"))
            .and_then(|raw_date| match raw_date {
                Value::Timestamp(duckdb::types::TimeUnit::Microsecond, raw_date) => Ok(raw_date),
                v => Err(anyhow!(
                    "error converting observed date into OffsetDatetime: {:?}, {:?}",
                    raw_date,
                    v
                )),
            })
            .and_then(|timestamp| {
                OffsetDateTime::from_unix_timestamp_nanos((*timestamp as i128) * 1000_i128).map_err(
                    |e| {
                        anyhow!(
                            "error parsing observed date into offsetdatetime: {} {}",
                            timestamp,
                            e
                        )
                    },
                )
            })
            .map(|val| val.to_offset(UtcOffset::UTC))?;

        let temp_low = values
            .get(1)
            .ok_or_else(|| anyhow!("temp_low not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let temp_high = values
            .get(2)
            .ok_or_else(|| anyhow!("temp_high not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let wind_speed = values
            .get(3)
            .ok_or_else(|| anyhow!("wind_speed not found in the map"))
            .and_then(|raw_speed| match raw_speed {
                Value::Int(speed) => Ok(*speed as i64),
                _ => Err(anyhow!(
                    "error converting wind_speed into int: {:?}",
                    raw_speed
                )),
            })?;

        Ok(Observed {
            date,
            temp_low,
            temp_high,
            wind_speed,
        })
    }
}

impl ToSql for Observed {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let ordered_struct: OrderedMap<String, Value> = OrderedMap::from(vec![
            (
                String::from("date"),
                Value::Text(self.date.format(&Rfc3339).unwrap()),
            ),
            (String::from("temp_low"), Value::Int(self.temp_low as i32)),
            (String::from("temp_high"), Value::Int(self.temp_high as i32)),
            (
                String::from("wind_speed"),
                Value::Int(self.wind_speed as i32),
            ),
        ]);
        Ok(ToSqlOutput::Owned(Value::Struct(ordered_struct)))
    }
}

impl ToRawSql for Observed {
    fn to_raw_sql(&self) -> String {
        // Done because the rust library doesn't natively support writing structs to the db just yet,
        // Eventually we should be able to delete this code
        // example of how to write a struct to duckdb: `INSERT INTO t1 VALUES (ROW('a', 42));`
        let mut vals = String::new();
        vals.push_str("ROW('");
        let data_str = self.date.format(&Rfc3339).unwrap();
        vals.push_str(&data_str);
        vals.push_str(r#"',"#);
        vals.push_str(&format!("{}", self.temp_low));
        vals.push(',');
        vals.push_str(&format!("{}", self.temp_high));
        vals.push(',');
        vals.push_str(&format!("{}", self.wind_speed));
        vals.push(')');
        vals
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct Forecasted {
    #[serde(with = "time::serde::rfc3339")]
    pub date: OffsetDateTime,
    pub temp_low: i64,
    pub temp_high: i64,
    pub wind_speed: i64,
}

impl TryInto<Forecasted> for &OrderedMap<String, Value> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Forecasted, Self::Error> {
        let values: Vec<&Value> = self.values().collect();
        let date = values
            .first()
            .ok_or_else(|| anyhow!("date not found in the map"))
            .and_then(|raw_date| match raw_date {
                Value::Timestamp(duckdb::types::TimeUnit::Microsecond, raw_date) => Ok(raw_date),
                _ => Err(anyhow!(
                    "error converting date into OffsetDatetime: {:?}",
                    raw_date
                )),
            })
            .and_then(|timestamp| {
                OffsetDateTime::from_unix_timestamp_nanos((*timestamp as i128) * 1000_i128).map_err(
                    |e| {
                        anyhow!(
                            "error parsing forecast date into offsetdatetime: {} {}",
                            timestamp,
                            e
                        )
                    },
                )
            })
            .map(|val| val.to_offset(UtcOffset::UTC))?;

        let temp_low = values
            .get(1)
            .ok_or_else(|| anyhow!("temp_low not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let temp_high = values
            .get(2)
            .ok_or_else(|| anyhow!("temp_high not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let wind_speed = values
            .get(3)
            .ok_or_else(|| anyhow!("wind_speed not found in the map"))
            .and_then(|raw_speed| match raw_speed {
                Value::Int(speed) => Ok(*speed as i64),
                _ => Err(anyhow!(
                    "error converting wind_speed into int: {:?}",
                    raw_speed
                )),
            })?;

        Ok(Forecasted {
            date,
            temp_low,
            temp_high,
            wind_speed,
        })
    }
}

impl TryInto<Forecasted> for OrderedMap<String, Value> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Forecasted, Self::Error> {
        let values: Vec<&Value> = self.values().collect();
        let date = values
            .first()
            .ok_or_else(|| anyhow!("date not found in the map"))
            .and_then(|raw_date| match raw_date {
                Value::Timestamp(duckdb::types::TimeUnit::Microsecond, raw_date) => Ok(raw_date),
                _ => Err(anyhow!(
                    "error converting date into OffsetDatetime: {:?}",
                    raw_date
                )),
            })
            .and_then(|timestamp| {
                OffsetDateTime::from_unix_timestamp_nanos((*timestamp as i128) * 1000_i128).map_err(
                    |e| {
                        anyhow!(
                            "error parsing forecast date into offsetdatetime: {} {}",
                            timestamp,
                            e
                        )
                    },
                )
            })
            .map(|val| val.to_offset(UtcOffset::UTC))?;

        let temp_low = values
            .get(1)
            .ok_or_else(|| anyhow!("temp_low not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let temp_high = values
            .get(2)
            .ok_or_else(|| anyhow!("temp_high not found in the map"))
            .and_then(|raw_temp| match raw_temp {
                Value::Int(temp) => Ok(*temp as i64),
                _ => Err(anyhow!("error converting temp into int: {:?}", raw_temp)),
            })?;

        let wind_speed = values
            .get(3)
            .ok_or_else(|| anyhow!("wind_speed not found in the map"))
            .and_then(|raw_speed| match raw_speed {
                Value::Int(speed) => Ok(*speed as i64),
                _ => Err(anyhow!(
                    "error converting wind_speed into int: {:?}",
                    raw_speed
                )),
            })?;

        Ok(Forecasted {
            date,
            temp_low,
            temp_high,
            wind_speed,
        })
    }
}

pub trait ToRawSql {
    /// Converts Rust value to raw valid DuckDB sql string (if user input make sure to validate before adding to db)
    fn to_raw_sql(&self) -> String;
}

impl ToRawSql for Forecasted {
    fn to_raw_sql(&self) -> String {
        // Done because the rust library doesn't natively support writing structs to the db just yet,
        // Eventually we should be able to delete this code
        // example of how to write a struct to duckdb: `INSERT INTO t1 VALUES (ROW('a', 42));`
        let mut vals = String::new();
        vals.push_str("ROW('");
        let data_str = self.date.format(&Rfc3339).unwrap();
        vals.push_str(&data_str);
        vals.push_str(r#"',"#);
        vals.push_str(&format!("{}", self.temp_low));
        vals.push(',');
        vals.push_str(&format!("{}", self.temp_high));
        vals.push(',');
        vals.push_str(&format!("{}", self.wind_speed));
        vals.push(')');
        vals
    }
}

impl ToSql for Forecasted {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let ordered_struct: OrderedMap<String, Value> = OrderedMap::from(vec![
            (
                String::from("date"),
                Value::Text(self.date.format(&Rfc3339).unwrap()),
            ),
            (String::from("temp_low"), Value::Int(self.temp_low as i32)),
            (String::from("temp_high"), Value::Int(self.temp_high as i32)),
            (
                String::from("wind_speed"),
                Value::Int(self.wind_speed as i32),
            ),
        ]);
        Ok(ToSqlOutput::Owned(Value::Struct(ordered_struct)))
    }
}

// Once submitted for now don't allow changes
// Decide if we want to add a pubkey for who submitted the entry?
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AddEventEntry {
    /// Client needs to provide a valid Uuidv7
    pub id: Uuid,
    pub event_id: Uuid,
    pub expected_observations: Vec<WeatherChoices>,
    /// Add the coordinator information that pushed this event entry along
    pub coordinator: Option<CoordinatorInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AddEventEntryMessage {
    /// Client needs to provide a valid Uuidv7
    pub id: Uuid,
    pub event_id: Uuid,
    pub expected_observations: Vec<WeatherChoices>,
}

impl AddEventEntryMessage {
    pub fn message(&self) -> Result<Message, serde_json::Error> {
        let message_str = serde_json::to_string(self)?;
        let message = Message::from_hashed_data::<sha256::Hash>(message_str.as_bytes());
        Ok(message)
    }
}

impl From<AddEventEntry> for AddEventEntryMessage {
    fn from(value: AddEventEntry) -> Self {
        AddEventEntryMessage {
            id: value.id,
            event_id: value.event_id,
            expected_observations: value.expected_observations,
        }
    }
}

impl From<AddEventEntry> for WeatherEntry {
    fn from(value: AddEventEntry) -> Self {
        WeatherEntry {
            id: value.id,
            event_id: value.event_id,
            expected_observations: value.expected_observations,
            score: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct WeatherEntry {
    pub id: Uuid,
    pub event_id: Uuid,
    pub expected_observations: Vec<WeatherChoices>,
    /// A score wont appear until the observation_date has begun
    pub score: Option<i64>,
}

impl TryInto<WeatherEntry> for &OrderedMap<String, Value> {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<WeatherEntry, Self::Error> {
        debug!("raw weather entry: {:?}", self);
        let values: Vec<&Value> = self.values().collect();
        let id = values
            .first()
            .ok_or_else(|| anyhow!("id not found in the map"))
            .and_then(|raw_id| match raw_id {
                Value::Text(id) => Ok(id),
                _ => Err(anyhow!(
                    "error converting weather entry id into string: {:?}",
                    raw_id
                )),
            })
            .and_then(|id| {
                Uuid::parse_str(id)
                    .map_err(|e| anyhow!("error converting weather entry id into uuid: {}", e))
            })?;

        let event_id = values
            .get(1)
            .ok_or_else(|| anyhow!("event_id not found in the map"))
            .and_then(|raw_id| match raw_id {
                Value::Text(id) => Ok(id),
                _ => Err(anyhow!(
                    "error converting weather event id into string: {:?}",
                    raw_id
                )),
            })
            .and_then(|id| {
                Uuid::parse_str(id)
                    .map_err(|e| anyhow!("error converting weather event id into uuid: {}", e))
            })?;

        let expected_observations = values
            .get(2)
            .ok_or_else(|| anyhow!("expect_observations not found in the map"))
            .and_then(|raw| match raw {
                Value::List(expected_observations) => Ok(expected_observations),
                _ => Err(anyhow!(
                    "error converting expect_observations into struct: {:?}",
                    raw
                )),
            })
            .and_then(|weather_choices| {
                let mut converted = vec![];
                for weather_choice in weather_choices {
                    let weather_struct_choice = match weather_choice {
                        Value::Struct(weather_choice_struct) => weather_choice_struct.try_into()?,
                        _ => {
                            return Err(anyhow!(
                                "error converting weather_choice into struct: {:?}",
                                weather_choice
                            ))
                        }
                    };
                    converted.push(weather_struct_choice);
                }
                Ok(converted)
            })?;

        let score = values.get(3).and_then(|raw_id| match raw_id {
            Value::Int(id) => Some(*id as i64),
            _ => None,
        });

        Ok(WeatherEntry {
            id,
            event_id,
            score,
            expected_observations,
        })
    }
}

impl<'a> TryFrom<&Row<'a>> for WeatherEntry {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(WeatherEntry {
            id: row
                .get::<usize, String>(0)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            event_id: row
                .get::<usize, String>(1)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            score: row
                .get::<usize, Option<i64>>(2)
                .map(|val| val.filter(|&val| val != 0))?,
            expected_observations: vec![],
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WeatherChoicesWithEntry {
    pub entry_id: Uuid,
    // NOAA weather stations we're using
    pub stations: String,
    pub temp_high: Option<ValueOptions>,
    pub temp_low: Option<ValueOptions>,
    pub wind_speed: Option<ValueOptions>,
}

impl<'a> TryFrom<&Row<'a>> for WeatherChoicesWithEntry {
    type Error = duckdb::Error;
    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(WeatherChoicesWithEntry {
            entry_id: row
                .get::<usize, String>(0)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            stations: row
                .get::<usize, String>(1)
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            temp_low: row
                .get::<usize, Option<String>>(2)
                .map(|raw| raw.and_then(|inner| ValueOptions::try_from(inner).ok()))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            temp_high: row
                .get::<usize, Option<String>>(3)
                .map(|raw| raw.and_then(|inner| ValueOptions::try_from(inner).ok()))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(3, Type::Any, Box::new(e)))?,
            wind_speed: row
                .get::<usize, Option<String>>(4)
                .map(|raw| raw.and_then(|inner| ValueOptions::try_from(inner).ok()))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(4, Type::Any, Box::new(e)))?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct WeatherChoices {
    // NOAA weather stations we're using
    pub stations: String,
    pub temp_high: Option<ValueOptions>,
    pub temp_low: Option<ValueOptions>,
    pub wind_speed: Option<ValueOptions>,
}

impl From<WeatherChoicesWithEntry> for WeatherChoices {
    fn from(value: WeatherChoicesWithEntry) -> Self {
        Self {
            stations: value.stations,
            temp_high: value.temp_high,
            temp_low: value.temp_low,
            wind_speed: value.wind_speed,
        }
    }
}

impl<'a> TryFrom<&Row<'a>> for WeatherChoices {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(WeatherChoices {
            stations: row
                .get::<usize, String>(0)
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            temp_low: row
                .get::<usize, Option<String>>(1)
                .map(|raw| raw.and_then(|inner| ValueOptions::try_from(inner).ok()))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            temp_high: row
                .get::<usize, Option<String>>(2)
                .map(|raw| raw.and_then(|inner| ValueOptions::try_from(inner).ok()))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            wind_speed: row
                .get::<usize, Option<String>>(3)
                .map(|raw| raw.and_then(|inner| ValueOptions::try_from(inner).ok()))
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(3, Type::Any, Box::new(e)))?,
        })
    }
}

impl TryInto<WeatherChoices> for &OrderedMap<String, Value> {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<WeatherChoices, Self::Error> {
        debug!("raw weather choices: {:?}", self);
        let values: Vec<&Value> = self.values().collect();
        let stations = values
            .first()
            .ok_or_else(|| anyhow!("stations not found in the map"))
            .and_then(|raw_station| match raw_station {
                Value::Text(station) => Ok(station.clone()),
                _ => Err(anyhow!(
                    "error converting station id into string: {:?}",
                    raw_station
                )),
            })?;
        let temp_low = values.get(1).and_then(|raw_temp| match raw_temp {
            Value::Text(temp) => ValueOptions::try_from(temp.clone()).ok(),
            _ => None,
        });
        let temp_high = values.get(2).and_then(|raw_temp| match raw_temp {
            Value::Text(temp) => ValueOptions::try_from(temp.clone()).ok(),
            _ => None,
        });
        let wind_speed = values
            .get(3)
            .and_then(|raw_wind_speed| match raw_wind_speed {
                Value::Text(wind_speed) => ValueOptions::try_from(wind_speed.clone()).ok(),
                _ => None,
            });
        Ok(WeatherChoices {
            stations,
            temp_low,
            temp_high,
            wind_speed,
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<Value> for &WeatherChoices {
    fn into(self) -> Value {
        let temp_low = match self.temp_low.clone() {
            Some(val) => Value::Text(val.to_string()),
            None => Value::Null,
        };
        let temp_high = match self.temp_high.clone() {
            Some(val) => Value::Text(val.to_string()),
            None => Value::Null,
        };
        let wind_speed = match self.wind_speed.clone() {
            Some(val) => Value::Text(val.to_string()),
            None => Value::Null,
        };
        let ordered_struct: OrderedMap<String, Value> = OrderedMap::from(vec![
            (String::from("stations"), Value::Text(self.stations.clone())),
            (String::from("temp_low"), temp_low),
            (String::from("temp_high"), temp_high),
            (String::from("wind_speed"), wind_speed),
        ]);
        Value::Struct(ordered_struct)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub enum ValueOptions {
    Over,
    // Par is what was forecasted for this value
    Par,
    Under,
}

impl std::fmt::Display for ValueOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Over => write!(f, "over"),
            Self::Par => write!(f, "par"),
            Self::Under => write!(f, "under"),
        }
    }
}

impl TryFrom<&str> for ValueOptions {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "over" => Ok(ValueOptions::Over),
            "par" => Ok(ValueOptions::Par),
            "under" => Ok(ValueOptions::Under),
            val => Err(anyhow!("invalid option: {}", val)),
        }
    }
}

impl TryFrom<String> for ValueOptions {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_str() {
            "over" => Ok(ValueOptions::Over),
            "par" => Ok(ValueOptions::Par),
            "under" => Ok(ValueOptions::Under),
            val => Err(anyhow!("invalid option: {}", val)),
        }
    }
}
