use anyhow::anyhow;
use axum::{
    extract::{Query, State},
    Json,
};
use duckdb::{
    arrow::{
        array::{Float64Array, Int64Array, StringArray},
        record_batch::RecordBatch,
    },
    params_from_iter, Connection, Error, Result,
};
use regex::Regex;
use scooby::postgres::{select, Aliasable, Parameters};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::{drop_suffix, grab_file_names, AppError, AppState, FileParams};

#[derive(Clone, Deserialize)]
pub struct ObservationRequest {
    pub start: Option<String>,
    pub end: Option<String>,
    pub station_ids: String,
}

impl ObservationRequest {
    fn validate(&self) -> Result<(), anyhow::Error> {
        if let Some(start) = self.start.clone() {
            OffsetDateTime::parse(&start, &Rfc3339)
                .map_err(|_| anyhow!("start param value is not a value Rfc3339 datetime"))?;
        }

        if let Some(end) = self.end.clone() {
            OffsetDateTime::parse(&end, &Rfc3339)
                .map_err(|_| anyhow!("end param value is not a value Rfc3339 datetime"))?;
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Observation {
    pub station_id: String,
    pub start_time: String,
    pub end_time: String,
    pub temp_low: f64,
    pub temp_high: f64,
    pub wind_speed: i64,
}

pub async fn observations(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ObservationRequest>,
) -> Result<Json<Vec<Observation>>, AppError> {
    req.validate()?;

    let parquet_files = grab_file_names(
        &state.logger,
        &state.data_dir.clone(),
        FileParams {
            start: req.start.clone(),
            end: req.end.clone(),
            observations: Some(true),
            forecasts: Some(false),
        },
    )
    .await?;
    let file_paths = build_file_paths(state.data_dir.clone(), parquet_files);
    let station_ids: Vec<String> = req.station_ids.split(',').map(|id| id.to_owned()).collect();
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("INSTALL parquet; LOAD parquet;")?;

    let rbs = run_observations_query(&conn, file_paths, &req, station_ids)?;

    let observations: Vec<Observation> = rbs.iter().flat_map(record_batch_to_vec).collect();
    Ok(Json(observations))
}

pub fn build_file_paths(data_dir: String, file_names: Vec<String>) -> Vec<String> {
    file_names
        .iter()
        .map(|file_name| {
            let file_pieces: Vec<String> = file_name.split('_').map(|f| f.to_owned()).collect();
            let created_time = drop_suffix(file_pieces.last().unwrap(), ".parquet");
            let file_generated_at = OffsetDateTime::parse(&created_time, &Rfc3339).unwrap();
            format!("{}/{}/{}", data_dir, file_generated_at.date(), file_name)
        })
        .collect()
}

fn run_observations_query(
    conn: &Connection,
    file_paths: Vec<String>,
    req: &ObservationRequest,
    station_ids: Vec<String>,
) -> Result<Vec<RecordBatch>, Error> {
    let mut placeholders = Parameters::new();
    let mut query = select((
        "station_id",
        "min(generated_at)".as_("start_time"),
        "max(generated_at)".as_("end_time"),
        "min(temperature_value)".as_("temp_low"),
        "max(temperature_value)".as_("temp_high"),
        "max(wind_speed)".as_("wind_speed"),
    ))
    .from(format!(
        "read_parquet(['{}'], union_by_name = true)",
        file_paths.join("', '")
    ));

    let mut values: Vec<String> = vec![];
    if !station_ids.is_empty() {
        query = query.where_(format!(
            "station_id IN ({})",
            placeholders.next_n(station_ids.len())
        ));

        for station_id in station_ids {
            values.push(station_id);
        }
    }
    if let Some(start) = &req.start {
        query = query.where_(format!(
            "generated_at::TIMESTAMPTZ >= {}::TIMESTAMPTZ",
            placeholders.next()
        ));
        values.push(start.to_owned());
    }

    if let Some(end) = &req.end {
        query = query.where_(format!(
            "generated_at::TIMESTAMPTZ <= {}::TIMESTAMPTZ",
            placeholders.next()
        ));
        values.push(end.to_owned());
    }
    query = query.group_by("station_id");

    let re = Regex::new(r"\$(\d+)").unwrap();
    let binding = query.to_string();
    let fixed_params = re.replace_all(&binding, "?");
    let mut stmt = conn.prepare(&fixed_params)?;
    let sql_params = params_from_iter(values.iter());
    let records: Vec<RecordBatch> = stmt.query_arrow(sql_params)?.collect();
    Ok(records)
}

fn record_batch_to_vec(record_batch: &RecordBatch) -> Vec<Observation> {
    let mut observations = Vec::new();
    let station_id_arr = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 0");
    let start_time_arr = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 1");
    let end_time_arr = record_batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 2");
    let temp_low_arr = record_batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Expected Float64Array in column 3");
    let temp_high_arr = record_batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Expected Float64Array in column 4");
    let wind_speed_arr = record_batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array in column 4");

    for row_index in 0..record_batch.num_rows() {
        let station_id = station_id_arr.value(row_index).to_owned();
        let start_time = start_time_arr.value(row_index).to_owned();
        let end_time = end_time_arr.value(row_index).to_owned();
        let temp_low = temp_low_arr.value(row_index);
        let temp_high = temp_high_arr.value(row_index);
        let wind_speed = wind_speed_arr.value(row_index);

        observations.push(Observation {
            station_id,
            start_time,
            end_time,
            temp_low,
            temp_high,
            wind_speed,
        });
    }

    observations
}
