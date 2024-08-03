use super::run_migrations;
use crate::utc_datetime;
use dlctix::bitcoin::XOnlyPublicKey;
use dlctix::secp::{MaybePoint, MaybeScalar, Point};
use duckdb::types::Type;
use duckdb::{params_from_iter, Connection, Row};
use log::info;
use regex::Regex;
use scooby::postgres::{select, with, Joinable, Select};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateOracleEventData {
    /// Provide UUIDv7 to use for looking up the event
    pub id: Uuid,
    #[serde(with = "utc_datetime")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
    /// Date of when the weather observations occured (midnight UTC), all entries must be made before this time
    pub observation_date: OffsetDateTime,
    /// All entry_ids need to be generated at the events creation
    pub entry_ids: Vec<Uuid>,
    // NOAA observation stations used in this event
    pub locations: Vec<String>,
    pub total_allowed_entries: i64,
    pub number_of_places_win: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EventFilter {
    // TODO: add more options, proper pagination
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OracleEventData {
    pub id: Uuid,
    #[serde(with = "utc_datetime")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
    /// Date of when the weather observations occured
    pub observation_date: OffsetDateTime,
    /// Used in constructing the dlctix transactions
    pub oracle_nonce: Point,

    pub locking_points: Vec<MaybePoint>,
    // NOAA observation stations used in this event
    pub locations: Vec<String>,
    /// Knowing the total number of entries, how many can place
    /// The dlctix coordinator can determine how many transactions to create
    pub total_allowed_entries: i64,
    /// Needs to all be generated at the start
    pub entry_ids: Vec<Uuid>,
    pub number_of_winners: i64,
    /// All entries into this event, wont be returned until date of observation begins and will be ranked by score
    pub entries: Vec<WeatherEntry>,
    /// The forecasted and observed values for each station on the event date
    pub weather: Vec<Weather>,
    /// When added it means the oracle has signed that the current data is the final result
    pub attestation: Option<MaybeScalar>,
}

impl<'a> TryFrom<&Row<'a>> for OracleEventData {
    type Error = duckdb::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(OracleEventData {
            id: row
                .get::<usize, String>(0)
                .map(|val| Uuid::parse_str(&val))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            signing_date: row
                .get::<usize, String>(1)
                .map(|val| OffsetDateTime::parse(&val, &Rfc3339))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(1, Type::Any, Box::new(e)))?,
            observation_date: row
                .get::<usize, String>(2)
                .map(|val| OffsetDateTime::parse(&val, &Rfc3339))?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(2, Type::Any, Box::new(e)))?,
            oracle_nonce: todo!(),
            locking_points: todo!(),
            locations: row
                .get::<usize, Vec<u8>>(0)
                .map(|blob| {
                    bincode::deserialize(&blob)
                        .map_err(|e| duckdb::types::FromSqlError::Other(Box::new(e)))
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            total_allowed_entries: row.get::<usize, i64>(0)?,
            entry_ids: row
                .get::<usize, Vec<u8>>(0)
                .map(|blob| {
                    bincode::deserialize(&blob)
                        .map_err(|e| duckdb::types::FromSqlError::Other(Box::new(e)))
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            number_of_winners: row.get::<usize, i64>(0)?,
            entries: row
                .get::<usize, Vec<u8>>(0)
                .map(|blob| {
                    bincode::deserialize(&blob)
                        .map_err(|e| duckdb::types::FromSqlError::Other(Box::new(e)))
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            weather: row
                .get::<usize, Vec<u8>>(0)
                .map(|blob| {
                    bincode::deserialize(&blob)
                        .map_err(|e| duckdb::types::FromSqlError::Other(Box::new(e)))
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            attestation: todo!(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Weather {
    pub station_id: String,
    pub observed: Observed,
    pub forecasted: Forecasted,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Observed {
    #[serde(with = "utc_datetime")]
    pub date: OffsetDateTime,
    pub temp_low: i64,
    pub temp_high: i64,
    pub wind_speed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Forecasted {
    #[serde(with = "utc_datetime")]
    pub date: OffsetDateTime,
    pub temp_low: i64,
    pub temp_high: i64,
    pub wind_speed: i64,
}

// Once submitted for now don't allow changes
// Decide if we want to add a pubkey for who submitted the entry?
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AddEventEntry {
    pub expected_observations: Vec<WeatherChoices>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WeatherEntry {
    /// Picked at random from the list of possible ids that haven't been used yet
    pub id: Uuid,
    pub event_id: Uuid,
    pub expected_observations: Vec<WeatherChoices>,
    /// A score wont appear until the observation_date has begun
    pub score: Option<i64>,
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
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            expected_observations: row
                .get::<usize, Vec<u8>>(0)
                .map(|blob| {
                    bincode::deserialize(&blob)
                        .map_err(|e| duckdb::types::FromSqlError::Other(Box::new(e)))
                })?
                .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, Type::Any, Box::new(e)))?,
            score: row.get::<usize, Option<i64>>(1)?,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WeatherChoices {
    // NOAA weather stations we're using
    pub stations: String,
    pub temp_high: Option<ValueOptions>,
    pub temp_low: Option<ValueOptions>,
    pub wind_speed: Option<ValueOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum ValueOptions {
    Over,
    // Par is what was forecasted for this value
    Par,
    Under,
}

pub struct EventData {
    // TODO: see if a read/write lock makes more sense here (careful of writer starvation) and be aware of bottleneck around locking that may occur under heavy loads
    // eventually we may need to come up with a pool or other non-locking approach for grabbing the connection, but that shouldn't appear until we hit a decent usage level
    conn: Arc<Mutex<Connection>>,
}

impl EventData {
    pub fn new(path: &str) -> Result<Self, duckdb::Error> {
        let mut conn = Connection::open(format!("{}/events.db3", path))?;
        run_migrations(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn get_stored_public_key(&self) -> Result<XOnlyPublicKey, duckdb::Error> {
        let select = select("pubkey").from("oracle_metadata");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&select.to_string())?;
        let key: Vec<u8> = stmt.query_row([], |row| row.get(0))?;
        //TODO: use a custom error here so we don't need to panic
        let converted_key = XOnlyPublicKey::from_slice(&key).expect("invalid pubkey");
        Ok(converted_key)
    }

    pub async fn add_oracle_metadata(&self, pubkey: XOnlyPublicKey) -> Result<(), duckdb::Error> {
        let pubkey_raw = pubkey.serialize().to_vec();
        //TODO: Add the ability to change the name via config
        let name = String::from("4casttruth");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("INSERT INTO oracle_metadata (pubkey,name) VALUES(?,?)")?;
        stmt.execute([pubkey_raw, name.into()])?;
        Ok(())
    }

    pub async fn update_weather_station_data(&self, weather: Weather) -> Result<(), duckdb::Error> {
        //called by a background process every so often
        todo!()
    }

    pub async fn add_event(
        &self,
        event: CreateOracleEventData,
    ) -> Result<OracleEventData, duckdb::Error> {
        todo!()
    }

    pub async fn add_event_entry(
        &self,
        entry: AddEventEntry,
    ) -> Result<WeatherEntry, duckdb::Error> {
        todo!()
    }

    pub async fn get_weather_entry(&self, entry_id: Uuid) -> Result<WeatherEntry, duckdb::Error> {
        todo!()
    }

    pub async fn filtered_list_events(
        &self,
        filter: EventFilter,
    ) -> Result<Vec<OracleEventData>, duckdb::Error> {
        let event_weather_select = select((
            "weather.event_id as event_id",
            "station_id",
            "date",
            "observed",
            "forecasted",
        ))
        .from(
            "events_weather"
                .join("events")
                .on("events_weather.event_id = events.id")
                .join("weather")
                .on("events_weather.weather_id = weather.id"),
        );

        let event_entries_select =
            select(("events_entries.event_id", "expected_observations", "score")).from(
                "events_weather"
                    .join("events")
                    .on("events_entries.event_id = events.id"),
            );

        let event_select = with("event_weather")
            .as_(event_weather_select)
            .and_with("event_entries")
            .as_(event_entries_select)
            .select((
                "id",
                "total_allowed_entries",
                "number_of_winners",
                "signing_date",
                "observation_date",
                "created_at",
                "updated_at",
                "nonce",
                "attestation_signature",
                "entry_ids",
                "locations",
                "locking_points",
            ))
            .and_select("list_value(SELECT event_id, station_id, date, observed, forecasted FROM event_weather) as weather")
            .and_select("list_value(SELECT event_id, expected_observations, score FROM event_entries) as entries")
            .from("events")
            .limit(filter.limit);

        let conn = self.conn.lock().await;
        let query_str = &event_select.to_string();
        info!("query_str: {}", query_str);
        let mut stmt = conn.prepare(&query_str)?;

        let event_data: Vec<OracleEventData> = stmt.query_map([], |row| row.try_into())?;
        Ok(event_data)
    }

    pub async fn get_oracle_event(&self, id: &Uuid) -> Result<OracleEventData, duckdb::Error> {
        let event_weather_select = select((
            "weather.event_id as event_id",
            "station_id",
            "date",
            "observed",
            "forecasted",
        ))
        .from(
            "events_weather"
                .join("events")
                .on("events_weather.event_id = events.id")
                .join("weather")
                .on("events_weather.weather_id = weather.id"),
        )
        .where_("event_id = $1");

        let event_entries_select =
            select(("events_entries.event_id", "expected_observations", "score"))
                .from(
                    "events_weather"
                        .join("events")
                        .on("events_entries.event_id = events.id"),
                )
                .where_("event_id = $1");

        let event_select = with("event_weather")
            .as_(event_weather_select)
            .and_with("event_entries")
            .as_(event_entries_select)
            .select((
                "id",
                "total_allowed_entries",
                "number_of_winners",
                "signing_date",
                "observation_date",
                "created_at",
                "updated_at",
                "nonce",
                "attestation_signature",
                "entry_ids",
                "locations",
                "locking_points",
            ))
            .and_select("list_value(SELECT event_id, station_id, date, observed, forecasted FROM event_weather) as weather")
            .and_select("list_value(SELECT event_id, expected_observations, score FROM event_entries) as entries")
            .from("events")
            .where_("id = $1");

        let conn = self.conn.lock().await;
        let query_str = &event_select.to_string();
        info!("query_str: {}", query_str);
        let mut stmt = conn.prepare(&query_str)?;
        let sql_params = params_from_iter(vec![id.to_string()]);

        let event_data: OracleEventData = stmt.query_row(sql_params, |row| row.try_into())?;
        Ok(event_data)
    }
    async fn prepare_query(&self, select: Select) -> Result<String, duckdb::Error> {
        let re = Regex::new(r"\$(\d+)").unwrap();
        let binding = select.to_string();
        let fixed_params = re.replace_all(&binding, "?");
        Ok(fixed_params.to_string())
    }
}
