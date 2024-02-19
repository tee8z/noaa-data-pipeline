use anyhow::anyhow;
use axum::{
    extract::{Query, State},
    Json,
};
use duckdb::{
    arrow::array::{Int64Array, RecordBatch, StringArray},
    params_from_iter, Connection, Error, Result,
};
use regex::Regex;
use scooby::postgres::{select, with, Aliasable, Parameters};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};

use crate::{build_file_paths, grab_file_names, AppError, AppState, FileParams};

#[derive(Clone, Deserialize)]
pub struct ForecastRequest {
    pub start: Option<String>,
    pub end: Option<String>,
    pub station_ids: String,
}

#[derive(Serialize, Deserialize)]
pub struct Forecast {
    pub station_id: String,
    pub date: String,
    pub start_time: String,
    pub end_time: String,
    pub temp_low: i64,
    pub temp_high: i64,
    pub wind_speed: i64,
}

impl ForecastRequest {
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

pub async fn forecasts(
    State(state): State<Arc<AppState>>,
    Query(req): Query<ForecastRequest>,
) -> Result<Json<Vec<Forecast>>, AppError> {
    req.validate()?;
    let start_back_one_day = if let Some(start) = req.start.clone() {
        let start_date = OffsetDateTime::parse(&start, &Rfc3339)?;
        start_date
            .saturating_sub(Duration::days(1))
            .format(&Rfc3339)?
    } else {
        OffsetDateTime::now_utc()
            .saturating_sub(Duration::days(1))
            .format(&Rfc3339)?
    };
    let parquet_files = grab_file_names(
        &state.logger,
        &state.data_dir,
        FileParams {
            start: Some(start_back_one_day),
            end: req.end.clone(),
            observations: Some(false),
            forecasts: Some(true),
        },
    )
    .await?;
    let file_paths = build_file_paths(state.data_dir.clone(), parquet_files);
    let station_ids: Vec<String> = req.station_ids.split(',').map(|id| id.to_owned()).collect();
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("INSTALL parquet; LOAD parquet;")?;

    let rbs = run_forecasts_query(&conn, file_paths, &req, station_ids)?;

    let forecasts: Vec<Forecast> = rbs.iter().flat_map(record_batch_to_vec).collect();
    Ok(Json(forecasts))
}

fn run_forecasts_query(
    conn: &Connection,
    file_paths: Vec<String>,
    req: &ForecastRequest,
    station_ids: Vec<String>,
) -> Result<Vec<RecordBatch>, Error> {
    let mut placeholders = Parameters::new();

    let mut daily_forecasts = select((
        "station_id",
        "DATE_TRUNC('day', begin_time::TIMESTAMP)::TEXT".as_("date"),
        "MIN(begin_time)".as_("start_time"),
        "MAX(end_time)".as_("end_time"),
        "MIN(min_temp)".as_("temp_low"),
        "MAX(max_temp)".as_("temp_high"),
        "MAX(wind_speed)".as_("wind_speed"),
    ))
    .from(format!(
        "read_parquet(['{}'], union_by_name = true)",
        file_paths.join("', '")
    ));

    let mut values: Vec<String> = vec![];
    if !station_ids.is_empty() {
        daily_forecasts = daily_forecasts.where_(format!(
            "station_id IN ({})",
            placeholders.next_n(station_ids.len())
        ));

        for station_id in station_ids {
            values.push(station_id);
        }
    }
    if let Some(start) = &req.start {
        daily_forecasts = daily_forecasts.where_(format!(
            "(DATE_TRUNC('day', begin_time::TIMESTAMP)::TIMESTAMPTZ) >= {}::TIMESTAMPTZ",
            placeholders.next()
        ));
        values.push(start.to_owned());
    }

    if let Some(end) = &req.end {
        daily_forecasts = daily_forecasts.where_(format!(
            "(DATE_TRUNC('day', begin_time::TIMESTAMP) + INTERVAL '1 day')::TIMESTAMPTZ <= {}::TIMESTAMPTZ",
            placeholders.next()
        ));
        values.push(end.to_owned());
    }
    daily_forecasts = daily_forecasts.group_by(("station_id", "begin_time"));

    let query = with("daily_forecasts")
        .as_(daily_forecasts)
        .select((
            "station_id",
            "date",
            "MIN(start_time)".as_("start_time"),
            "MAX(end_time)".as_("end_time"),
            "MIN(temp_low)".as_("temp_low"),
            "MAX(temp_high)".as_("temp_high"),
            "MAX(wind_speed)".as_("wind_speed"),
        ))
        .from("daily_forecasts")
        .group_by(("station_id", "date"))
        .to_string();

    let re = Regex::new(r"\$(\d+)").unwrap();
    let binding = query.to_string();
    let fixed_params = re.replace_all(&binding, "?");
    let mut stmt = conn.prepare(&fixed_params)?;
    let sql_params = params_from_iter(values.iter());
    let records: Vec<RecordBatch> = stmt.query_arrow(sql_params)?.collect();
    Ok(records)
}

fn record_batch_to_vec(record_batch: &RecordBatch) -> Vec<Forecast> {
    let mut forecasts = Vec::new();
    let station_id_arr = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 0");
    let date_arr = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 1");
    let start_time_arr = record_batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 2");
    let end_time_arr = record_batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray in column 3");
    let temp_low_arr = record_batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array in column 4");
    let temp_high_arr = record_batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array in column 5");
    let wind_speed_arr = record_batch
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array in column 6");

    for row_index in 0..record_batch.num_rows() {
        let station_id = station_id_arr.value(row_index).to_owned();
        let date = date_arr.value(row_index).to_owned();
        let start_time = start_time_arr.value(row_index).to_owned();
        let end_time = end_time_arr.value(row_index).to_owned();
        let temp_low = temp_low_arr.value(row_index);
        let temp_high = temp_high_arr.value(row_index);
        let wind_speed = wind_speed_arr.value(row_index);

        forecasts.push(Forecast {
            station_id,
            date,
            start_time,
            end_time,
            temp_low,
            temp_high,
            wind_speed,
        });
    }

    forecasts
}
