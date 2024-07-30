use crate::{
    create_event, download, files, forecasts, get_event, get_npub, get_pubkey, get_stations,
    index_handler, list_events, observations,
    oracle::{self, Oracle},
    routes, sign_event, upload, EventData, FileAccess, WeatherData,
};
use anyhow::anyhow;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use hyper::Method;
use std::sync::Arc;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

#[derive(Clone)]
pub struct AppState {
    pub ui_dir: String,
    pub remote_url: String,
    pub file_access: Arc<FileAccess>,
    pub weather_db: Arc<WeatherData>,
    pub oracle: Arc<Oracle>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        routes::events::oracle_routes::get_npub,
        routes::events::oracle_routes::get_pubkey,
        routes::events::oracle_routes::list_events,
        routes::events::oracle_routes::create_event,
        routes::events::oracle_routes::get_event,
        routes::events::oracle_routes::sign_event,
        routes::stations::weather_routes::forecasts,
        routes::stations::weather_routes::observations,
        routes::stations::weather_routes::get_stations,
        routes::files::download::download,
        routes::files::get_names::files,
        routes::files::upload::upload,
    ),
    components(
        schemas(
                routes::files::get_names::Files,
                oracle::OracleError,
                oracle::CreateEvent,
                oracle::SignEvent,
                oracle::OracleAttestation,
                oracle::OracleAnnouncement,
            )
    ),
    tags(
        (name = "noaa data oracle api", description = "a RESTful api that acts as an oracle for NOAA forecast and observation data")
    )
)]
struct ApiDoc;

pub fn app(
    remote_url: String,
    ui_dir: String,
    data_dir: String,
    event_dir: String,
    private_key_file_path: String,
) -> Result<Router, anyhow::Error> {
    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([Method::GET, Method::POST])
        // allow requests from any origin
        .allow_origin(Any);

    // The ui folder needs to be generated and have this relative path from where the binary is being run
    let serve_dir = ServeDir::new("ui").not_found_service(ServeFile::new(ui_dir.clone()));
    let file_access = Arc::new(FileAccess::new(data_dir));
    let weather_db = Arc::new(
        WeatherData::new(file_access.clone())
            .map_err(|e| anyhow!("error setting up weather data: {}", e))?,
    );

    let event_db = Arc::new(
        EventData::new(&event_dir).map_err(|e| anyhow!("error setting up event data: {}", e))?,
    );
    let oracle = Arc::new(Oracle::new(
        event_db,
        weather_db.clone(),
        &private_key_file_path,
    )?);

    let app_state = AppState {
        ui_dir,
        remote_url,
        weather_db,
        file_access,
        oracle,
    };

    let api_docs = ApiDoc::openapi();
    Ok(Router::new()
        .route("/files", get(files))
        .route("/file/:file_name", get(download))
        .route("/file/:file_name", post(upload))
        .route("/stations", get(get_stations))
        .route("/stations/forecasts", get(forecasts))
        .route("/stations/observations", get(observations))
        .route("/oracle/npub", get(get_npub))
        .route("/oracle/pubkey", get(get_pubkey))
        .route("/oracle/events", get(list_events))
        .route("/oracle/events", post(create_event))
        .route("/oracle/events/:event_id", get(get_event))
        .route("/oracle/events/:event_id/sign", post(sign_event))
        .layer(DefaultBodyLimit::max(30 * 1024 * 1024)) // max is in bytes
        .route("/", get(index_handler))
        .with_state(Arc::new(app_state))
        .merge(Scalar::with_url("/docs", api_docs))
        .nest_service("/ui", serve_dir.clone())
        .fallback_service(serve_dir)
        .layer(cors))
}
