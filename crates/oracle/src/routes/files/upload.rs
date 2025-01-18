use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
};
use log::{error, info};
use std::sync::Arc;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::AppState;

#[utoipa::path(
    post,
    path = "file/{file_name}",
    params(
         ("file_name" = String, Path, description = "Name of file to upload"),
    ),
    responses(
        (status = OK, description = "Successfully uploaded weather data file"),
        (status = BAD_REQUEST, description = "Invalid file"),
        (status = INTERNAL_SERVER_ERROR, description = "Failed to save file")
    ))]
pub async fn upload(
    State(state): State<Arc<AppState>>,
    Path(file_name): Path<String>,
    mut multipart: Multipart,
) -> Result<(), (StatusCode, String)> {
    if !path_is_valid(&file_name) {
        return Err((StatusCode::BAD_REQUEST, "Invalid file".to_owned()));
    }
    while let Some(field) = multipart.next_field().await.unwrap() {
        let data = field.bytes().await.map_err(|err| {
            error!("error getting file's bytes: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get file's bytes: {}", err),
            )
        })?;

        info!(
            "length of `{}` is {} mb",
            file_name,
            bytes_to_mb(data.len())
        );
        let current_folder = state.file_access.current_folder();
        let path = std::path::Path::new(&current_folder).join(&file_name);
        // Create a new file and write the data to it
        let mut file = File::create(&path).await.map_err(|err| {
            error!("error creating file: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create file: {}", err),
            )
        })?;
        file.write_all(&data).await.map_err(|err| {
            error!("error creating file: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to write to file: {}", err),
            )
        })?;
    }

    Ok(())
}

fn bytes_to_mb(bytes: usize) -> f64 {
    bytes as f64 / 1_048_576.0
}

// to prevent directory traversal attacks we ensure the path consists of exactly one normal component
fn path_is_valid(path: &str) -> bool {
    let path = std::path::Path::new(path);

    let mut components = path.components().peekable();

    if let Some(first) = components.peek() {
        if !matches!(first, std::path::Component::Normal(_)) {
            return false;
        }
    }

    components.count() == 1 && is_parquet_file(path)
}

fn is_parquet_file(path: &std::path::Path) -> bool {
    if let Some(extenstion) = path.extension() {
        extenstion == "parquet"
    } else {
        false
    }
}
