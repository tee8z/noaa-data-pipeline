use crate::AppState;
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

// Make our own error that wraps `anyhow::Error`.
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error getting file names: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Serialize)]
pub struct Files {
    pub file_names: Vec<String>,
}

#[derive(Clone, Deserialize)]
pub struct FileParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub observations: Option<bool>,
    pub forecasts: Option<bool>,
}

pub async fn files(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FileParams>,
) -> Result<Json<Files>, AppError> {
    validate_params(&params)?;
    let file_names = grab_file_names(&state.logger, &state.data_dir, params)
        .await
        .map_err(|e| {
            error!(state.logger, "error getting filenames: {}", e);
            e
        })?;
    let files = Files { file_names };
    Ok(Json(files))
}

fn validate_params(params: &FileParams) -> Result<(), anyhow::Error> {
    if let Some(start) = params.start.clone() {
        OffsetDateTime::parse(&start, &Rfc3339)
            .map_err(|_| anyhow!("start param value is not a value Rfc3339 datetime"))?;
    }

    if let Some(end) = params.end.clone() {
        OffsetDateTime::parse(&end, &Rfc3339)
            .map_err(|_| anyhow!("end param value is not a value Rfc3339 datetime"))?;
    }

    Ok(())
}
//Body::from_stream
pub async fn grab_file_names(
    logger: &Logger,
    data_dir: &str,
    params: FileParams,
) -> Result<Vec<String>, Error> {
    let mut files_names = vec![];
    if let Ok(mut entries) = fs::read_dir(data_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if let Some(date) = entry.file_name().to_str() {
                let format = format_description!("[year]-[month]-[day]");
                let directory_date = Date::parse(date, &format).map_err(|e| {
                    anyhow!(
                        "error stored directory name does not have a valid date in name: {}",
                        e
                    )
                })?;
                if !is_date_in_range(directory_date, &params) {
                    continue;
                }

                if let Ok(mut subentries) = fs::read_dir(path).await {
                    while let Ok(Some(subentries)) = subentries.next_entry().await {
                        if let Some(filename) = add_filename(logger, subentries, &params)? {
                            files_names.push(filename);
                        }
                    }
                }
            }
        }
    }
    Ok(files_names)
}

fn add_filename(
    logger: &Logger,
    entry: tokio::fs::DirEntry,
    params: &FileParams,
) -> Result<Option<String>, Error> {
    if let Some(filename) = entry.file_name().to_str() {
        let file_pieces: Vec<String> = filename.split('_').map(|f| f.to_owned()).collect();
        let created_time = drop_suffix(file_pieces.last().unwrap(), ".parquet");
        trace!(logger, "parsed file time:{}", created_time);

        let file_generated_at = OffsetDateTime::parse(&created_time, &Rfc3339).map_err(|e| {
            anyhow!(
                "error stored filename does not have a valid rfc3339 datetime in name: {}",
                e
            )
        })?;
        let valid_time_range = is_time_in_range(file_generated_at, params);
        let file_data_type = file_pieces.first().unwrap();
        trace!(logger, "parsed file type:{}", file_data_type);

        if let Some(observations) = params.observations {
            if observations && file_data_type.eq("observations") && valid_time_range {
                return Ok(Some(filename.to_owned()));
            }
        }

        if let Some(forecasts) = params.forecasts {
            if forecasts && file_data_type.eq("forecasts") && valid_time_range {
                return Ok(Some(filename.to_owned()));
            }
        }

        if params.forecasts.is_none() && params.observations.is_none() && valid_time_range {
            return Ok(Some(filename.to_owned()));
        }
    }
    Ok(None)
}

pub fn drop_suffix(input: &str, suffix: &str) -> String {
    if let Some(stripped) = input.strip_suffix(suffix) {
        stripped.to_string()
    } else {
        input.to_string()
    }
}

fn is_date_in_range(compare_to: Date, params: &FileParams) -> bool {
    if let Some(start) = params.start.clone() {
        return match OffsetDateTime::parse(&start, &Rfc3339) {
            Ok(start) => compare_to >= start.date(),
            Err(_) => false,
        };
    }

    if let Some(end) = params.end.clone() {
        return match OffsetDateTime::parse(&end, &Rfc3339) {
            Ok(end) => compare_to <= end.date(),
            Err(_) => false,
        };
    }
    true
}

fn is_time_in_range(compare_to: OffsetDateTime, params: &FileParams) -> bool {
    if let Some(start) = params.start.clone() {
        return match OffsetDateTime::parse(&start, &Rfc3339) {
            Ok(start) => compare_to >= start,
            Err(_) => false,
        };
    }

    if let Some(end) = params.end.clone() {
        return match OffsetDateTime::parse(&end, &Rfc3339) {
            Ok(end) => compare_to <= end,
            Err(_) => false,
        };
    }
    true
}
