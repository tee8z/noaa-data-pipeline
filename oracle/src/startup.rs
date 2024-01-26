use std::sync::Arc;

use crate::{create_enum_event, download, files, get_event, get_events, get_pubkey, health_check, index_handler, sign_event, upload, OracleService};
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use hyper::Method;
use slog::Logger;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

#[derive(Clone)]
pub struct AppState {
    pub logger: Logger,
    pub data_dir: String,
    pub ui_dir: String,
    pub remote_url: String,
    pub oracle_service: OracleService,
}

pub fn app(
    logger: Logger,
    remote_url: String,
    ui_dir: String,
    data_dir: String,
    oracle_service: OracleService,
) -> Router {
    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([Method::GET, Method::POST])
        // TODO (@tee8z): add limits to what browser pages can hit this service via config value
        .allow_origin(Any);

    // The ui folder needs to be generated and have this relative path from where the binary is being run
    let serve_dir = ServeDir::new("ui").not_found_service(ServeFile::new(ui_dir.clone()));
    let app_state = AppState {
        logger,
        data_dir,
        ui_dir,
        remote_url,
        oracle_service,
    };

    let raw_data_routes = Router::new()
        .route("/", get(files))
        .route("/:file_name", get(download))
        .route("/:file_name", post(upload)); //NOTE: make this a private route with a proxy (like nginx) so only the daemon can upload to it

    let oracle_routes = Router::new()
        .route("/", get(get_pubkey))
        .route("/events", get(get_events))
        .route("/events/:event_id", get(get_event))
        .route("/enum", post(create_enum_event))
        .route("/sign", post(sign_event));

    Router::new()
        .route("/health_check", get(health_check))
        .nest("/files", raw_data_routes)
        .nest("/oracle", oracle_routes)
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024)) // max is in bytes
        .route("/", get(index_handler))
        .with_state(Arc::new(app_state))
        .nest_service("/ui", serve_dir.clone())
        .fallback_service(serve_dir)
        .layer(cors)
}
