use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, Request, StatusCode},
};
use hyper::{
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    HeaderMap,
};
use log::error;
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::{drop_suffix, AppState};

#[utoipa::path(
    get,
    path = "file/{filename}",
    params(
         ("filename" = String, Path, description = "Name of file to download"),
    ),
    responses(
        (status = OK, description = "Successfully retrieved file", body = Body),
        (status = BAD_REQUEST, description = "Invalid file name"),
        (status = INTERNAL_SERVER_ERROR, description = "Failed to retrieve file by name")
    ))]
pub async fn download(
    State(state): State<Arc<AppState>>,
    Path(filename): Path<String>,
    _request: Request<Body>,
) -> Result<(HeaderMap, Body), (StatusCode, String)> {
    let file_pieces: Vec<String> = filename.split('_').map(|f| f.to_owned()).collect();
    let created_time = drop_suffix(file_pieces.last().unwrap(), ".parquet");
    let file_generated_at = OffsetDateTime::parse(&created_time, &Rfc3339).map_err(|e| {
        error!(
            "error stored filename does not have a valid rfc3339 datetime in name: {}",
            e
        );
        (
            StatusCode::BAD_REQUEST,
            format!(
                "Badly formatted filename, not a valid rfc3339 datetime: {}",
                e
            ),
        )
    })?;
    // split filename for the date, add that to the path
    let file_path = state
        .file_access
        .build_file_path(&filename, file_generated_at);

    let file = File::open(file_path).await.map_err(|err| {
        error!("error opening file: {}", err);
        (StatusCode::NOT_FOUND, format!("File not found: {}", err))
    })?;

    // convert the `AsyncRead` into a `Stream`
    let stream = ReaderStream::new(file);
    // convert the `Stream` into an `axum::body::HttpBody`
    let body = Body::from_stream(stream);
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/parquet").unwrap(),
    );
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename)).unwrap(),
    );

    Ok((headers, body))
}
