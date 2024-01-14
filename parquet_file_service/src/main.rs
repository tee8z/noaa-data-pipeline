use axum::{
    body::StreamBody,
    extract::{DefaultBodyLimit, Multipart, Path},
    http::{HeaderValue, Request, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use hyper::{
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    Body, HeaderMap, Method,
};
use serde::Serialize;
use std::{net::SocketAddr, str::FromStr};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tokio_util::io::ReaderStream;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = app();
    let listener = SocketAddr::from_str("0.0.0.0:9100").unwrap();
    axum::Server::bind(&listener)
        .serve(app.into_make_service())
        .await?;
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
        .route("/file/:file_name", post(upload))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024)) // max is in bytes
        .nest_service("/assets", serve_dir.clone())
        .fallback_service(serve_dir)
        .layer(cors)
}

const UPLOADS_DIRECTORY: &str = "weather_data";

async fn download(
    Path(filename): Path<String>,
    _request: Request<Body>,
) -> Result<(HeaderMap, StreamBody<ReaderStream<File>>), (StatusCode, String)> {
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

async fn upload(
    Path(file_name): Path<String>,
    mut multipart: Multipart,
) -> Result<(), (StatusCode, String)> {
    if !path_is_valid(&file_name) {
        return Err((StatusCode::BAD_REQUEST, "Invalid file".to_owned()));
    }
    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        println!("Length of `{}` is {} bytes", name, data.len());
        let path = std::path::Path::new(UPLOADS_DIRECTORY).join(&file_name);
        // Create a new file and write the data to it
        let mut file = File::create(&path).await.expect("Failed to create file");
        file.write_all(&data)
            .await
            .expect("Failed to write to file");
    }
    Ok(())
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
