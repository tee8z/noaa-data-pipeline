use axum::{extract::State, Json};
use duckdb::{
    arrow::{
        array::{Float64Array, StringArray},
        record_batch::RecordBatch,
    },
    Connection, Error, Result,
};
use scooby::postgres::select;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};

use super::build_file_paths;
use crate::{grab_file_names, AppError, AppState, FileParams};

#[derive(Serialize, Deserialize)]
pub struct Station {
    pub station_id: String,
    pub station_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

pub async fn get_stations(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Station>>, AppError> {
    let now = OffsetDateTime::now_utc();
    let end = now.format(&Rfc3339)?;
    let start = now
        .saturating_sub(Duration::hours(4_i64))
        .format(&Rfc3339)?;
    let parquet_files = grab_file_names(
        &state.logger,
        &state.data_dir.clone(),
        FileParams {
            start: Some(start),
            end: Some(end),
            observations: Some(true),
            forecasts: Some(false),
        },
    )
    .await?;
    let file_paths = build_file_paths(state.data_dir.clone(), parquet_files);

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("INSTALL parquet; LOAD parquet;")?;

    let rbs = run_stations_query(&conn, file_paths)?;

    let stations: Vec<Station> = rbs.iter().flat_map(record_batch_to_vec).collect();
    Ok(Json(stations))
}

fn run_stations_query(
    conn: &Connection,
    file_paths: Vec<String>,
) -> Result<Vec<RecordBatch>, Error> {
    let mut query = select(("station_id", "station_name", "latitude", "longitude")).from(format!(
        "read_parquet(['{}'], union_by_name = true)",
        file_paths.join("', '")
    ));
    query = query.group_by(("station_id", "station_name", "latitude", "longitude"));

    let mut stmt = conn.prepare(&query.to_string())?;
    let records: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    Ok(records)
}

fn record_batch_to_vec(record_batch: &RecordBatch) -> Vec<Station> {
    let mut stations = Vec::new();
    let station_id_arr = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 0");
    let station_name_arr = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 1");
    let latitude_arr = record_batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Expected Float64Array in column 2");
    let longitude_arr = record_batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Expected Float64Array in column 3");

    for row_index in 0..record_batch.num_rows() {
        let station_id = station_id_arr.value(row_index).to_owned();
        let station_name = station_name_arr.value(row_index).to_owned();
        let latitude = latitude_arr.value(row_index);
        let longitude = longitude_arr.value(row_index);

        stations.push(Station {
            station_id,
            station_name,
            latitude,
            longitude,
        });
    }

    stations
}
