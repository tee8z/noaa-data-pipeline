use axum::{
    body::StreamBody,
    extract::Path,
    http::{HeaderValue, Request, StatusCode},
};
use hyper::{
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    Body, HeaderMap
};
use tokio::{
    fs::File,
};
use tokio_util::io::ReaderStream;


pub async fn download(
    Path(filename): Path<String>,
    _request: Request<Body>,
) -> Result<(HeaderMap, StreamBody<ReaderStream<File>>), (StatusCode, String)> {
    //TODO: make this configuerable, pull from context
    let UPLOADS_DIRECTORY = "test";
    let file_path = format!("{}/{}", UPLOADS_DIRECTORY, filename);

    let file = File::open(file_path)
        .await
        .map_err(|err| (StatusCode::NOT_FOUND, format!("File not found: {}", err)))
        .unwrap();

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