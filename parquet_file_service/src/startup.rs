use axum::{
    extract::{DefaultBodyLimit},
    routing::{get, post},
    Router,
};
use hyper::Method;
use crate::{files, download, upload, index_handler};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

pub fn app(server_address: String, ui: String) -> Router {
    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([Method::GET, Method::POST])
        // allow requests from any origin
        .allow_origin(Any);

    // The ui folder needs to be generated and have this relative path from where the binary is being run
    let serve_dir = ServeDir::new("ui").not_found_service(ServeFile::new(ui.clone()));

    Router::new()
        .route("/files", get(files)) //TODO: add filtering based on observation vs forecast and time ranges
        .route("/file/:file_name", get(download))
        .route("/file/:file_name", post(upload))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024)) // max is in bytes
        .route("/", get(move || index_handler(server_address.clone(), ui.clone())))
        .nest_service("/ui", serve_dir.clone())
        .fallback_service(serve_dir)
        .layer(cors)
}