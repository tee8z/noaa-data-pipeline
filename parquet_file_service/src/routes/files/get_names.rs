use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Serialize;
use slog::{error, Logger};
use tokio::fs;

use crate::AppState;

#[derive(Serialize)]
struct Files {
    file_names: Vec<String>,
}

pub async fn files(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let files = Files {
        file_names: grab_file_names(&state.logger, &state.data_dir).await,
    };
    Json(files)
}

async fn grab_file_names(logger: &Logger, data_dir: &str) -> Vec<String> {
    // Read the contents of the directory
    let mut files_names = vec![];
    if let Ok(mut entries) = fs::read_dir(data_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(filename) = entry.file_name().to_str() {
                files_names.push(filename.to_string());
            }
        }

        if let Err(err) = entries.next_entry().await {
            error!(logger, "Error getting entries: {}", err);
        }
    }
    files_names
}
