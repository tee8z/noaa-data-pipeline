use axum::{
    body::StreamBody,
    extract::Path,
    http::{HeaderValue, Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    BoxError, Json, Router,
};
use futures::{Stream, TryStreamExt};
use hyper::{
    body::Bytes,
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    Body, HeaderMap, Method,
};
use serde::Serialize;
use std::{io, net::SocketAddr, str::FromStr};
use tokio::{
    fs::{self, File},
    io::BufWriter,
};
use tokio_util::io::{ReaderStream, StreamReader};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = app();
    let listener = SocketAddr::from_str(&"0.0.0.0:9100").unwrap();
    axum::Server::bind(&listener).serve(app.into_make_service()).await?;
    Ok(())
}
pub fn app() -> Router {
    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([Method::GET, Method::POST])
        // allow requests from any origin
        .allow_origin(Any);

    let serve_dir = ServeDir::new("assets").not_found_service(ServeFile::new("assets/index.html"));

    Router::new()
        .route("/files", get(files)) //TODO: add filtering based on observation vs forecast and time ranges
        .route("/file/:file_name", get(download))
        .route("/file/:file_name", post(save_request_body))
        .nest_service("/assets", serve_dir.clone())
        .fallback_service(serve_dir)
        .layer(cors)
}

const UPLOADS_DIRECTORY: &str = "weather_data";

//TODO: param for file name (should be download path)
async fn download(
    Path(filename): Path<String>,
    _request: Request<Body>,
) -> Result<(HeaderMap, StreamBody<ReaderStream<File>>), (StatusCode, String)> {
    let file_path = format!("{}/{}", UPLOADS_DIRECTORY, filename);

    let file = File::open(file_path)
        .await
        .map_err(|err| return (StatusCode::NOT_FOUND, format!("File not found: {}", err)))
        .unwrap();

    // convert the `AsyncRead` into a `Stream`
    let stream = ReaderStream::new(file);
    // convert the `Stream` into an `axum::body::HttpBody`
    let body = StreamBody::new(stream);
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&"application/parquet").unwrap(),
    );
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename)).unwrap(),
    );

    Ok((headers, body))
}

#[derive(Serialize)]
struct Files {
    file_names: Vec<String>,
}

async fn files() -> impl IntoResponse {
    let files = Files {
        file_names: grab_file_names().await,
    };
    Json(files)
}

async fn grab_file_names() -> Vec<String> {
    // Read the contents of the directory
    let mut files_names = vec![];
    if let Ok(mut entries) = fs::read_dir(UPLOADS_DIRECTORY).await {
        while let Some(entry) = entries.next_entry().await.expect("error getting entries") {
            if let Some(filename) = entry.file_name().to_str() {
                files_names.push(filename.to_string());
            }
        }
    }
    files_names
}

// POST'ing to `/file/foo.txt` will create a file called `foo.txt`.
async fn save_request_body(
    Path(file_name): Path<String>,
    request: Request<Body>,
) -> Result<(), (StatusCode, String)> {
    stream_to_file(&file_name, request.into_body()).await
}

// Save a `Stream` to a file
async fn stream_to_file<S, E>(path: &str, stream: S) -> Result<(), (StatusCode, String)>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<BoxError>,
{
    if !path_is_valid(path) {
        return Err((StatusCode::BAD_REQUEST, "Invalid file".to_owned()));
    }

    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
        let body_reader = StreamReader::new(body_with_io_error);
        futures::pin_mut!(body_reader);

        // Create the file. `File` implements `AsyncWrite`.
        let path = std::path::Path::new(UPLOADS_DIRECTORY).join(path);
        let mut file = BufWriter::new(File::create(path).await?);

        // Copy the body into the file.
        tokio::io::copy(&mut body_reader, &mut file).await?;

        Ok::<_, io::Error>(())
    }
    .await
    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

// to prevent directory traversal attacks we ensure the path consists of exactly one normal
// component
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
