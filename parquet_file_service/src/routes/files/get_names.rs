use crate::AppState;
use anyhow::{anyhow, Error};
use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
    Json,
};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use slog::error;
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
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
    let file_names = grab_file_names(&state.data_dir, params)
        .await
        .map_err(|e| {
            error!(state.logger, "error getting filenames: {}", e);
            e
        })?;
    let files = Files { file_names };
    Ok(Json(files))
}

async fn grab_file_names(data_dir: &str, params: FileParams) -> Result<Vec<String>, Error> {
    let mut files_names = vec![];
    if let Ok(mut entries) = fs::read_dir(data_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(filename) = entry.file_name().to_str() {
                let file_pieces: Vec<String> = filename.split('_').map(|f| f.to_owned()).collect();
                let file_generated_at = OffsetDateTime::parse(
                    &drop_suffix(file_pieces.last().unwrap(), ".parquet"),
                    &Rfc3339,
                )
                .map_err(|_| {
                    anyhow!("error stored filename does not have a valid rfc3339 datetime in name")
                })?;
                let mut valid_time_range = true;
                let file_data_type = file_pieces.first().unwrap();
                if let Some(start) = params.start.clone() {
                    let start_time = OffsetDateTime::parse(&start, &Rfc3339).map_err(|_| {
                        anyhow!("start param value is not a value Rfc3339 datetime")
                    })?;
                    if file_generated_at < start_time {
                        valid_time_range = false;
                    }
                }

                if let Some(end) = params.end.clone() {
                    let end_time = OffsetDateTime::parse(&end, &Rfc3339)
                        .map_err(|_| anyhow!("end param value is not a value Rfc3339 datetime"))?;
                    if file_generated_at > end_time {
                        valid_time_range = false;
                    }
                }

                if let Some(observations) = params.observations {
                    if observations && file_data_type.eq("observations") {
                        if valid_time_range {
                            files_names.push(filename.to_owned())
                        }
                    }
                }

                if let Some(forecasts) = params.forecasts {
                    if forecasts && file_data_type.eq("forecasts") {
                        if valid_time_range {
                            files_names.push(filename.to_owned())
                        }
                    }
                }

                if params.forecasts.is_none() && params.observations.is_none() {
                    if valid_time_range {
                        files_names.push(filename.to_owned())
                    }
                }
            }
        }
    }
    Ok(files_names)
}

fn drop_suffix(input: &str, suffix: &str) -> String {
    if let Some(stripped) = input.strip_suffix(suffix) {
        stripped.to_string()
    } else {
        input.to_string()
    }
}
