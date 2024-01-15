use axum::{
    response::IntoResponse,
    Json
};
use serde::Serialize;
use tokio::fs;

#[derive(Serialize)]
struct Files {
    file_names: Vec<String>,
}

pub async fn files() -> impl IntoResponse {
    let files = Files {
        file_names: grab_file_names().await,
    };
    Json(files)
}

async fn grab_file_names() -> Vec<String> {
    // Read the contents of the directory
    let mut files_names = vec![];
    //TODO: make this configuerable, pull from context
    let UPLOADS_DIRECTORY = "test";
    if let Ok(mut entries) = fs::read_dir(UPLOADS_DIRECTORY).await {
        while let Some(entry) = entries.next_entry().await.expect("error getting entries") {
            if let Some(filename) = entry.file_name().to_str() {
                files_names.push(filename.to_string());
            }
        }
    }
    files_names
}