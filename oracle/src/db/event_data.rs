use super::{run_migrations, weather_data, Forecast, Observation};
use crate::oracle::Oracle;
use crate::utc_datetime;
use anyhow::anyhow;
use dlctix::bitcoin::XOnlyPublicKey;
use dlctix::secp::{MaybeScalar, Scalar};
use dlctix::EventAnnouncement;
use duckdb::arrow::datatypes::ToByteSlice;
use duckdb::types::{OrderedMap, ToSqlOutput, Type, Value};
use duckdb::{
    ffi, params, params_from_iter, AccessMode, Config, Connection, ErrorCode, Row, ToSql,
};
use itertools::Itertools;
use log::{debug, info};
use regex::Regex;
use scooby::postgres::{insert_into, select, update, with, Aliasable, Joinable, Parameters};
use serde::{Deserialize, Serialize};
use serde_json::to_vec;
use std::collections::HashMap;
use std::time::Duration as StdDuration;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, Duration, OffsetDateTime, UtcOffset};
use tokio::time::timeout;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

pub struct EventData {
    connection_path: String,
    retry_duration: StdDuration,
    retry_max_attemps: i32,
}

impl EventData {
    pub fn new(path: &str) -> Result<Self, duckdb::Error> {
        let connection_path = format!("{}/events.db3", path);
        let mut conn = Connection::open(connection_path.clone())?;
        run_migrations(&mut conn)?;
        Ok(Self {
            connection_path,
            retry_duration: StdDuration::from_millis(100),
            retry_max_attemps: 5,
        })
    }

    async fn new_readonly_connection(&self) -> Result<Connection, duckdb::Error> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        Connection::open_with_flags(self.connection_path.clone(), config)
    }

    pub async fn new_readonly_connection_retry(&self) -> Result<Connection, duckdb::Error> {
        let mut attempt = 0;
        loop {
            match timeout(self.retry_duration, self.new_readonly_connection()).await {
                Ok(Ok(connection)) => return Ok(connection),
                Ok(Err(e)) => {
                    if attempt >= self.retry_max_attemps
                        || !e.to_string().contains("Could not set lock on file")
                    {
                        return Err(e);
                    }
                    info!("Retrying: {}", e);
                    attempt += 1;
                }
                Err(_) => {
                    return Err(duckdb::Error::DuckDBFailure(
                        duckdb::ffi::Error {
                            code: duckdb::ErrorCode::DatabaseLocked,
                            extended_code: 0,
                        },
                        None,
                    ));
                }
            }
        }
    }

    async fn new_write_connection(&self) -> Result<Connection, duckdb::Error> {
        let config = Config::default().access_mode(AccessMode::ReadWrite)?;
        Connection::open_with_flags(self.connection_path.clone(), config)
    }

    pub async fn new_write_connection_retry(&self) -> Result<Connection, duckdb::Error> {
        let mut attempt = 0;
        loop {
            match timeout(self.retry_duration, self.new_write_connection()).await {
                Ok(Ok(connection)) => return Ok(connection),
                Ok(Err(e)) => {
                    if attempt >= self.retry_max_attemps
                        || !e.to_string().contains("Could not set lock on file")
                    {
                        return Err(e);
                    }
                    info!("Retrying: {}", e);
                    attempt += 1;
                }
                Err(_) => {
                    return Err(duckdb::Error::DuckDBFailure(
                        duckdb::ffi::Error {
                            code: duckdb::ErrorCode::DatabaseLocked,
                            extended_code: 0,
                        },
                        None,
                    ));
                }
            }
        }
    }

    pub async fn get_stored_public_key(&self) -> Result<XOnlyPublicKey, duckdb::Error> {
        let select = select("pubkey").from("oracle_metadata");
        let conn = self.new_readonly_connection_retry().await?;
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
        let conn = self.new_write_connection_retry().await?;
        let mut stmt = conn.prepare("INSERT INTO oracle_metadata (pubkey,name) VALUES(?,?)")?;
        stmt.execute([pubkey_raw, name.into()])?;
        Ok(())
    }

    // Call as an ETL process to update the weather for running events
    pub async fn update_weather_station_data(
        &self,
        event_id: Uuid,
        weather: Vec<Weather>,
    ) -> Result<(), duckdb::Error> {
        //1) grab events that are using this weather data
        //2) add new weather data to table
        let weather_ids = self.add_weather_readings(weather).await?;

        //3) create join between weather and events
        self.batch_add_weather_to_event(event_id, weather_ids)
            .await?;

        Ok(())
    }

    pub async fn add_weather_readings(
        &self,
        weather: Vec<Weather>,
    ) -> Result<Vec<Uuid>, duckdb::Error> {
        let params: Vec<(Uuid, Value, Forecasted, Option<Observed>)> = weather
            .iter()
            .map(|weather| {
                let weather_id = Uuid::now_v7();
                (
                    weather_id,
                    Value::Text(weather.station_id.clone()),
                    weather.forecasted.clone(),
                    weather.observed.clone(),
                )
            })
            .collect();
        let weather_ids: Vec<Uuid> = params.iter().map(|row| row.0).collect();
        let mut param_placeholders = Parameters::new();
        let params_values: Vec<(String, String, String, String)> = params
            .iter()
            .map(|vals| {
                (
                    param_placeholders.next(),
                    param_placeholders.next(),
                    vals.2.to_raw_sql(),
                    vals.3
                        .clone()
                        .map_or("Null".to_string(), |x| x.to_raw_sql()),
                )
            })
            .collect();

        let insert_weather = insert_into("weather")
            .columns(("id", "station_id", "forecasted", "observed"))
            .values(params_values);
        let query_str = self.prepare_query(insert_weather.to_string());
        debug!("query_str: {}", query_str);
        let insert_values: Vec<Value> = params
            .into_iter()
            .flat_map(|(a, b, _, _)| vec![Value::Text(a.to_string()), b])
            .collect();
        debug!("insert values: {:?}", insert_values);

        let conn = self.new_write_connection_retry().await?;
        let mut weather_stmt = conn.prepare(&query_str)?;
        weather_stmt.execute(params_from_iter(insert_values.iter()))?;
        Ok(weather_ids)
    }

    pub async fn batch_add_weather_to_event(
        &self,
        event_id: Uuid,
        weather_ids: Vec<Uuid>,
    ) -> Result<(), duckdb::Error> {
        let params: Vec<(String, Uuid, String)> = weather_ids
            .iter()
            .map(|weather_id| {
                let event_weather_id = Uuid::now_v7().to_string();
                (event_weather_id, event_id, weather_id.to_string())
            })
            .collect();
        let mut param_placeholders = Parameters::new();
        let params_values: Vec<(String, String, String)> = params
            .iter()
            .map(|_| {
                (
                    param_placeholders.next(),
                    param_placeholders.next(),
                    param_placeholders.next(),
                )
            })
            .collect();

        let insert_event_weather = insert_into("events_weather")
            .columns(("id", "event_id", "weather_id"))
            .values(params_values);
        let query_str = self.prepare_query(insert_event_weather.to_string());
        debug!("query_str: {}", query_str);
        let insert_values: Vec<String> = params
            .into_iter()
            .flat_map(|(a, b, c)| vec![a, b.to_string(), c])
            .collect();

        info!("insert values: {:?}", insert_values);

        let conn = self.new_write_connection_retry().await?;
        let mut weather_stmt = conn.prepare(&query_str)?;
        weather_stmt.execute(params_from_iter(insert_values.iter()))?;
        Ok(())
    }
    pub async fn add_event(&self, event: CreateEventData) -> Result<Event, duckdb::Error> {
        let locations_sql = format!("[{}]", event.locations.join(","));

        let signing_date = OffsetDateTime::format(event.signing_date, &Rfc3339)
            .map_err(|e| duckdb::Error::ToSqlConversionFailure(Box::new(e)))?;
        let observation_date = OffsetDateTime::format(event.observation_date, &Rfc3339)
            .map_err(|e| duckdb::Error::ToSqlConversionFailure(Box::new(e)))?;
        let nonce = to_vec(&event.nonce).unwrap();
        let annoucement_bytes = to_vec(&event.event_annoucement).unwrap();
        let conn = self.new_write_connection_retry().await?;
        let mut stmt = conn.prepare(
            "INSERT INTO events (
                id,
                total_allowed_entries,
                number_of_places_win,
                number_of_values_per_entry,
                nonce,
                signing_date,
                observation_date,
                locations,
                event_annoucement) VALUES(?,?,?,?,?,?,?,?,?)",
        )?;
        stmt.execute(params![
            event.id.to_string(),
            event.total_allowed_entries,
            event.number_of_places_win,
            event.number_of_values_per_entry,
            nonce,
            signing_date,
            observation_date,
            locations_sql,
            annoucement_bytes,
        ])?;

        Ok(event.into())
    }

    pub async fn add_event_entry(
        &self,
        entry: WeatherEntry,
    ) -> Result<WeatherEntry, duckdb::Error> {
        //TODO: have these transactions happen in the same transaction
        self.add_entry(entry.clone()).await?;
        self.add_entry_choices(entry.clone()).await?;
        Ok(entry)
    }

    pub async fn add_entry(&self, entry: WeatherEntry) -> Result<(), duckdb::Error> {
        let conn = self.new_write_connection_retry().await?;
        // TODO: add check on the INSERT transaction to verify we never go over number of allowed entries in an event
        // (current worse case is just a couple of extra entries made it into the event, doesn't change how we sign the result)
        let insert_query = "INSERT INTO events_entries (id, event_id) VALUES(?,?)";
        let mut event_stmt = conn.prepare(insert_query)?;

        debug!("query_str: {}", insert_query);
        let insert_values = params![entry.id.to_string(), entry.event_id.to_string()];

        event_stmt.execute(insert_values)?;
        Ok(())
    }

    pub async fn add_entry_choices(&self, entry: WeatherEntry) -> Result<(), duckdb::Error> {
        #[allow(clippy::type_complexity)]
        let params: Vec<(
            Uuid,
            String,
            Option<ValueOptions>,
            Option<ValueOptions>,
            Option<ValueOptions>,
        )> = entry
            .expected_observations
            .iter()
            .map(|weather_choices| {
                (
                    entry.id,
                    weather_choices.stations.clone(),
                    weather_choices.temp_low.clone(),
                    weather_choices.temp_high.clone(),
                    weather_choices.wind_speed.clone(),
                )
            })
            .collect();

        let mut param_placeholders = Parameters::new();
        let params_values: Vec<(String, String, String, String, String)> = params
            .iter()
            .map(|_| {
                (
                    param_placeholders.next(),
                    param_placeholders.next(),
                    param_placeholders.next(),
                    param_placeholders.next(),
                    param_placeholders.next(),
                )
            })
            .collect();

        let insert_event_weather = insert_into("expected_observations")
            .columns(("entry_id", "station", "temp_low", "temp_high", "wind_speed"))
            .values(params_values);
        let query_str = self.prepare_query(insert_event_weather.to_string());
        debug!("query_str: {}", query_str);
        let insert_values: Vec<Value> = params
            .into_iter()
            .flat_map(|(a, b, c, d, e)| {
                let temp_low = match c {
                    Some(c) => Value::Text(c.to_string()),
                    None => Value::Null,
                };
                let temp_high = match d {
                    Some(d) => Value::Text(d.to_string()),
                    None => Value::Null,
                };
                let wind_speed = match e {
                    Some(e) => Value::Text(e.to_string()),
                    None => Value::Null,
                };
                vec![
                    Value::Text(a.to_string()),
                    Value::Text(b),
                    temp_low,
                    temp_high,
                    wind_speed,
                ]
            })
            .collect();

        info!("insert values: {:?}", insert_values);

        let conn = self.new_write_connection_retry().await?;
        let mut weather_stmt = conn.prepare(&query_str)?;
        weather_stmt.execute(params_from_iter(insert_values.iter()))?;
        Ok(())
    }
    pub async fn update_event_attestation(&self, event: &SignEvent) -> Result<(), duckdb::Error> {
        let entry_score_update_query = update("events")
            .set("attestation_signature", "$1")
            .where_("events.id = $2");

        let query_str = self.prepare_query(entry_score_update_query.to_string());
        debug!("query_str: {}", query_str);

        let conn = self.new_write_connection_retry().await?;
        let mut stmt = conn.prepare(&query_str)?;

        let Some(attestation) = event.attestation else {
            return Err(duckdb::Error::InvalidParameterCount(1, 2));
        };
        let attestation_bytes = to_vec(&attestation).unwrap();
        stmt.execute(params![attestation_bytes, event.id.to_string()])?;
        Ok(())
    }

    ///Danger: a raw SQL query is used, input is not escaped with '?'
    pub async fn update_entry_scores(
        &self,
        entry_scores: Vec<(Uuid, i64)>,
    ) -> Result<(), duckdb::Error> {
        let number_entry_scores = entry_scores.len();
        info!("number_entry_scores: {:?}", number_entry_scores);

        let mut entry_score_values = String::new();
        entry_score_values.push_str("VALUES");
        for (index, val) in entry_scores.iter().enumerate() {
            entry_score_values.push_str(&format!("('{}',{})", val.0, val.1));
            if index + 1 < number_entry_scores {
                entry_score_values.push(',');
            }
        }

        info!("entry_score_values: {}", entry_score_values);

        let mut entry_ids = String::new();
        entry_ids.push('(');
        for (index, val) in entry_scores.iter().enumerate() {
            entry_ids.push_str(&format!("'{}'", &val.0.to_string()));
            if index + 1 < number_entry_scores {
                entry_ids.push(',');
            }
        }
        entry_ids.push(')');
        info!("entry_ids: {}", entry_ids);
        let scores_temp_select = select("score")
            .from((entry_score_values).as_("scores(entry_id, score)"))
            .where_("scores.entry_id = events_entries.id::TEXT")
            .to_string();
        let entry_score_update_query = update("events_entries")
            .set("score", format!("({})", scores_temp_select))
            .where_(format!("events_entries.id::TEXT IN {}", entry_ids));

        let query_str = entry_score_update_query.to_string();
        debug!("query_str: {}", query_str);

        let conn = self.new_write_connection_retry().await?;
        let mut stmt = conn.prepare(&query_str)?;
        stmt.execute([])?;
        Ok(())
    }

    pub async fn get_event_weather(&self, event_id: Uuid) -> Result<Vec<Weather>, duckdb::Error> {
        let event_weather = select(("station_id", "observed", "forecasted"))
            .from(
                "events_weather"
                    .join("events")
                    .on("events_weather.event_id = events.id")
                    .join("weather")
                    .on("weather.id = events_weather.weather_id"),
            )
            .where_("event_id = ?");
        let query_str = event_weather.to_string();
        debug!("query_str: {}", query_str);

        let conn = self.new_readonly_connection_retry().await?;
        let mut stmt = conn.prepare(&query_str)?;
        let mut event_weather_rows = stmt.query([event_id.to_string()])?;
        let mut event_weather = vec![];
        while let Some(row) = event_weather_rows.next()? {
            let data: Weather = row.try_into()?;
            event_weather.push(data);
        }
        Ok(vec![])
    }

    pub async fn get_event_weather_entries(
        &self,
        event_id: &Uuid,
    ) -> Result<Vec<WeatherEntry>, duckdb::Error> {
        // Query 1
        let event_entries_select =
            select(("events_entries.id", "events_entries.event_id", "score"))
                .from(
                    "events_entries"
                        .join("events")
                        .on("events_entries.event_id = events.id"),
                )
                .where_("events_entries.event_id = ?")
                .group_by(("events_entries.id", "events_entries.event_id", "score"));

        let query_str = event_entries_select.to_string();
        debug!("query_str: {}", query_str);

        let conn = self.new_readonly_connection_retry().await?;
        let mut stmt = conn.prepare(&query_str)?;
        let mut weather_entry_rows = stmt.query([event_id.to_string()])?;
        let mut weather_entries = vec![];
        while let Some(row) = weather_entry_rows.next()? {
            let data: WeatherEntry = row.try_into()?;
            weather_entries.push(data);
        }

        // Query 2
        let entry_choices = select((
            "entry_id",
            "station",
            "temp_low::TEXT",
            "temp_high::TEXT",
            "wind_speed::TEXT",
        ))
        .from(
            "expected_observations"
                .join("events_entries")
                .on("events_entries.id = expected_observations.entry_id"),
        )
        .where_("events_entries.event_id = $1");
        let entry_choices_query_str = self.prepare_query(entry_choices.to_string());
        debug!("query_str: {}", entry_choices_query_str);
        let mut stmt_choices = conn.prepare(&entry_choices_query_str)?;
        let mut rows = stmt_choices.query([event_id.to_string()])?;

        //Combine query results
        let mut weather_choices: HashMap<Uuid, Vec<WeatherChoices>> = HashMap::new();
        while let Some(row) = rows.next()? {
            let data: WeatherChoicesWithEntry = row.try_into()?;
            if let Some(entry_choices) = weather_choices.get_mut(&data.entry_id) {
                entry_choices.push(data.into());
            } else {
                weather_choices.insert(data.entry_id, vec![data.into()]);
            }
        }

        for weather_entry in weather_entries.iter_mut() {
            if let Some(choices) = weather_choices.get(&weather_entry.id) {
                weather_entry.expected_observations = choices.clone();
            }
        }

        Ok(weather_entries)
    }

    pub async fn get_weather_entry(
        &self,
        event_id: &Uuid,
        entry_id: &Uuid,
    ) -> Result<WeatherEntry, duckdb::Error> {
        // Query 1
        let event_entry = select((
            "events_entries.id as id",
            "events_entries.event_id as event_id",
            "score",
        ))
        .from("events_entries")
        .where_("events_entries.id = $1 AND events_entries.event_id = $2");

        let conn = self.new_readonly_connection_retry().await?;
        let query_str = self.prepare_query(event_entry.to_string());
        debug!("query_str: {}", query_str);

        let mut stmt = conn.prepare(&query_str)?;
        let sql_params_entry = params_from_iter(vec![entry_id.to_string(), event_id.to_string()]);
        let mut weather_entry: WeatherEntry =
            stmt.query_row(sql_params_entry, |row| row.try_into())?;

        // Query 2
        let entry_choices = select((
            "station",
            "temp_low::TEXT",
            "temp_high::TEXT",
            "wind_speed::TEXT",
        ))
        .from("expected_observations")
        .where_("expected_observations.entry_id = $1");
        let entry_choices_query_str = self.prepare_query(entry_choices.to_string());
        debug!("query_str: {}", entry_choices_query_str);
        let sql_params = params_from_iter(vec![entry_id.to_string()]);

        let mut stmt_choices = conn.prepare(&entry_choices_query_str)?;
        let mut rows = stmt_choices.query(sql_params)?;
        let mut weather_choices: Vec<WeatherChoices> = vec![];
        while let Some(row) = rows.next()? {
            let data: WeatherChoices = row.try_into()?;
            weather_choices.push(data);
        }

        weather_entry.expected_observations = weather_choices;
        Ok(weather_entry)
    }

    pub async fn filtered_list_events(
        &self,
        filter: EventFilter,
    ) -> Result<Vec<EventSummary>, duckdb::Error> {
        let mut events = self.get_filtered_event_summarys(filter).await?;
        for event in events.iter_mut() {
            event.weather = self.get_event_weather(event.id).await?;
        }
        Ok(events)
    }

    async fn get_filtered_event_summarys(
        &self,
        filter: EventFilter,
    ) -> Result<Vec<EventSummary>, duckdb::Error> {
        let event_entries_select = select(("Count(id) as total_entries", "event_id"))
            .from("events_entries")
            .group_by("event_id");

        let mut event_select = with("event_entries")
            .as_(event_entries_select)
            .select((
                "id",
                "signing_date::TEXT",
                "observation_date::TEXT",
                "locations",
                "total_allowed_entries",
                "COALESCE(event_entries.total_entries,0) as total_entries",
                "number_of_places_win",
                "number_of_values_per_entry",
                "attestation_signature",
            ))
            .from(
                "events"
                    .left_join("event_entries")
                    .on("event_entries.event_id = events.id"),
            );
        if let Some(ids) = filter.event_ids.clone() {
            let mut event_ids_val = String::new();
            event_ids_val.push('(');
            for (index, _) in ids.iter().enumerate() {
                event_ids_val.push('?');
                if index < ids.len() {
                    event_ids_val.push(',');
                }
            }
            event_ids_val.push(')');
            let where_clause = format!("events.id IN {}", event_ids_val);
            event_select = event_select.clone().where_(where_clause);
        }
        if let Some(limit) = filter.limit {
            event_select = event_select.clone().limit(limit);
        }

        let conn = self.new_readonly_connection_retry().await?;
        let query_str = self.prepare_query(event_select.to_string());
        debug!("query_str: {}", query_str);
        let mut stmt = conn.prepare(&query_str)?;
        let mut rows = if let Some(ids) = filter.event_ids {
            let params: Vec<Value> = ids
                .iter()
                .map(|event_id| Value::Text(event_id.to_string()))
                .collect();
            stmt.query(params_from_iter(params.iter()))
        } else {
            stmt.query([])
        }?;
        let mut event_data: Vec<EventSummary> = vec![];
        while let Some(row) = rows.next()? {
            let data: EventSummary = row.try_into()?;
            event_data.push(data.clone());
        }

        Ok(event_data)
    }

    pub async fn get_event(&self, id: &Uuid) -> Result<Event, duckdb::Error> {
        let mut event = self.get_basic_event(id).await?;
        info!("event: {:?}", event);
        let weather_entries: Vec<WeatherEntry> = self.get_event_weather_entries(id).await?;
        event.entries = weather_entries.clone();
        event.entry_ids = weather_entries.iter().map(|val| val.id).collect();
        let event_weather: Vec<Weather> = self.get_event_weather(event.id).await?;
        event.weather = event_weather;
        info!("events: {:?}", event);
        Ok(event)
    }

    async fn get_basic_event(&self, id: &Uuid) -> Result<Event, duckdb::Error> {
        let event_select = select((
            "id",
            "signing_date::TEXT",
            "observation_date::TEXT",
            "event_annoucement",
            "locations",
            "total_allowed_entries",
            "number_of_places_win",
            "number_of_values_per_entry",
            "attestation_signature",
            "nonce",
        ))
        .from("events")
        .where_("id = $1");

        let conn = self.new_readonly_connection_retry().await?;
        let query_str = self.prepare_query(event_select.to_string());
        debug!("query_str: {}", query_str);
        let mut stmt = conn.prepare(&query_str)?;
        let sql_params = params_from_iter(vec![id.to_string()]);
        stmt.query_row(sql_params, |row| row.try_into())
    }

    pub async fn get_active_events(&self) -> Result<Vec<ActiveEvent>, duckdb::Error> {
        let event_entries_select = select(("Count(id) as total_entries", "event_id"))
            .from("events_entries")
            .group_by("event_id");

        let event_select = with("event_entries")
            .as_(event_entries_select)
            .select((
                "id",
                "signing_date::TEXT",
                "observation_date::TEXT",
                "locations",
                "total_allowed_entries",
                "COALESCE(event_entries.total_entries, 0) as total_entries",
                "number_of_places_win",
                "number_of_values_per_entry",
                "attestation_signature",
            ))
            .from(
                "events"
                    .left_join("event_entries")
                    .on("event_entries.event_id = events.id"),
            )
            .where_("attestation_signature IS NULL"); //Only filter out events that have been signed

        let conn = self.new_readonly_connection_retry().await?;
        let query_str = self.prepare_query(event_select.to_string());
        debug!("query_str: {}", query_str);
        let mut stmt = conn.prepare(&query_str)?;

        let mut rows = stmt.query([])?;
        let mut event_data: Vec<ActiveEvent> = vec![];
        while let Some(row) = rows.next()? {
            let data: ActiveEvent = row.try_into()?;
            event_data.push(data);
        }

        Ok(event_data)
    }

    pub async fn get_events_to_sign(
        &self,
        event_ids: Vec<Uuid>,
    ) -> Result<Vec<SignEvent>, duckdb::Error> {
        let mut event_ids_val = String::new();
        event_ids_val.push('(');
        for (index, _) in event_ids.iter().enumerate() {
            event_ids_val.push('?');
            if index + 1 < event_ids.len() {
                event_ids_val.push(',');
            }
        }
        event_ids_val.push(')');
        let where_clause = format!(
            "attestation_signature IS NULL AND events.id IN {}",
            event_ids_val
        );

        let event_select = select((
            "id",
            "signing_date::TEXT",
            "observation_date::TEXT",
            "number_of_places_win",
            "number_of_values_per_entry",
            "attestation_signature",
            "nonce",
            "event_annoucement",
        ))
        .from("events")
        .where_(where_clause);

        let params: Vec<Value> = event_ids
            .iter()
            .map(|event_id| Value::Text(event_id.to_string()))
            .collect();

        let conn = self.new_readonly_connection_retry().await?;
        let query_str = self.prepare_query(event_select.to_string());
        debug!("query_str: {}", query_str);
        let mut stmt = conn.prepare(&query_str)?;

        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        let mut event_data: Vec<SignEvent> = vec![];
        while let Some(row) = rows.next()? {
            let data: SignEvent = row.try_into()?;
            event_data.push(data);
        }

        Ok(event_data)
    }

    fn prepare_query(&self, query: String) -> String {
        let re = Regex::new(r"\$(\d+)").unwrap();
        let fixed_params = re.replace_all(&query, "?");
        fixed_params.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateEvent {
    /// Client needs to provide a valid Uuidv7
    pub id: Uuid,
    #[serde(with = "utc_datetime")]
    /// Time at which the attestation will be added to the event, needs to be after the observation date
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEventData {
    /// Provide UUIDv7 to use for looking up the event
    pub id: Uuid,
    #[serde(with = "utc_datetime")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
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
}

impl CreateEventData {
    //TODO: use the builder pattern
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        oracle: &Oracle,
        event_id: Uuid,
        observation_date: OffsetDateTime,
        signing_date: OffsetDateTime,
        locations: Vec<String>,
        total_allowed_entries: usize,
        number_of_places_win: usize,
        number_of_values_per_entry: usize,
    ) -> Result<Self, anyhow::Error> {
        if event_id.get_version_num() != 7 {
            return Err(anyhow!(
                "Client needs to provide a valid Uuidv7 for event id {}",
                event_id
            ));
        }
        if observation_date > signing_date {
            return Err(anyhow::anyhow!(
                "Signing date {} needs to be after observation date {}",
                signing_date.format(&Rfc3339).unwrap(),
                observation_date.format(&Rfc3339).unwrap()
            ));
        }

        let public_key = oracle.raw_public_key();
        // number_of_values_per_entry * 2 == max value, create array from max value to 0
        // determine all possible messages that we might sign
        let max_number_of_points_per_value_in_entry = 2;
        let possible_scores: Vec<i64> = (0..=(number_of_values_per_entry
            * max_number_of_points_per_value_in_entry))
            .map(|val| val as i64)
            .collect();

        // allows us to have comps where say the top 3 scores split the pot
        let possible_outcome_rankings: Vec<Vec<i64>> = possible_scores
            .iter()
            .combinations(number_of_places_win)
            .filter(|combination| {
                // Check if the combination is sorted in descending order, if not filter out of possible outcomes
                combination.windows(2).all(|window| window[0] >= window[1])
            })
            .map(|combination| combination.into_iter().cloned().collect())
            .collect();
        info!("outcomes: {:?}", possible_outcome_rankings);
        // holds all possible scoring results of the event
        let outcome_messages: Vec<Vec<u8>> = possible_outcome_rankings
            .into_iter()
            .map(|inner_vec| {
                inner_vec
                    .into_iter()
                    .flat_map(|num| num.to_be_bytes())
                    .collect::<Vec<u8>>()
            })
            .collect();

        let mut rng = rand::thread_rng();
        let nonce = Scalar::random(&mut rng);
        let nonce_point = nonce.base_point_mul();
        // Manually set expiry to 7 days after the signature should have been proveded so users can get their funds back
        let expiry = signing_date
            .saturating_add(Duration::DAY * 7)
            .unix_timestamp() as u32;

        // The actual accounement the oracle is going to attest the outcome
        let event_annoucement = EventAnnouncement {
            oracle_pubkey: public_key.into(),
            nonce_point,
            outcome_messages,
            expiry: Some(expiry),
        };

        Ok(Self {
            id: event_id,
            observation_date,
            signing_date,
            nonce,
            total_allowed_entries: total_allowed_entries as i64,
            number_of_places_win: number_of_places_win as i64,
            number_of_values_per_entry: number_of_values_per_entry as i64,
            locations,
            event_annoucement,
        })
    }
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
    #[serde(with = "utc_datetime")]
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
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
    #[serde(with = "utc_datetime")]
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
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
    #[serde(with = "utc_datetime")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
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
            weather: row
                .get::<usize, Value>(9)
                .map(|raw| {
                    let list_weather = match raw {
                        Value::List(list) => list,
                        _ => vec![],
                    };
                    let mut weather_data = vec![];
                    for value in list_weather.iter() {
                        if let Value::Struct(data) = value {
                            let weather: Weather = match data.try_into() {
                                Ok(val) => val,
                                Err(e) => return Err(e),
                            };
                            weather_data.push(weather)
                        }
                    }
                    Ok(weather_data)
                })?
                .map_err(|e| {
                    duckdb::Error::DuckDBFailure(
                        ffi::Error {
                            code: ErrorCode::TypeMismatch,
                            extended_code: 0,
                        },
                        Some(e.to_string()),
                    )
                })?,
        };
        event_summary.update_status();
        Ok(event_summary)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct Event {
    pub id: Uuid,
    #[serde(with = "utc_datetime")]
    /// Time at which the attestation will be added to the event
    pub signing_date: OffsetDateTime,
    #[serde(with = "utc_datetime")]
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
    #[serde(with = "utc_datetime")]
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
    #[serde(with = "utc_datetime")]
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
