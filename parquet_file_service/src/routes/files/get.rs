use std::sync::Arc;

use axum::{
    body::StreamBody,
    extract::{Path, State},
    http::{HeaderValue, Request, StatusCode},
};
use hyper::{
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    Body, HeaderMap,
};
use slog::error;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::AppState;

pub async fn download(
    State(state): State<Arc<AppState>>,
    Path(filename): Path<String>,
    _request: Request<Body>,
) -> Result<(HeaderMap, StreamBody<ReaderStream<File>>), (StatusCode, String)> {
    let file_path = format!("{}/{}", state.data_dir, filename);

    let file = File::open(file_path).await.map_err(|err| {
        error!(state.logger, "error opening file: {}", err);
        (StatusCode::NOT_FOUND, format!("File not found: {}", err))
    })?;

    // convert the `AsyncRead` into a `Stream`
    let stream = ReaderStream::new(file);
    // convert the `Stream` into an `axum::body::HttpBody`
    let body = StreamBody::new(stream);
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
