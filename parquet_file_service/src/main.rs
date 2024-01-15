use axum::Server;
use parquet_file_service::{app, create_folder, get_config_info, setup_logger};
use slog::info;
use std::{net::SocketAddr, str::FromStr};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = get_config_info();
    let logger = setup_logger(&cli);
    let weather_data = cli.weather_dir.unwrap_or(String::from("./weather_data"));
    create_folder(&logger, &weather_data.clone());
    let address = SocketAddr::from_str(&format!(
        "{}:{}",
        cli.domain.unwrap_or(String::from("127.0.0.1")),
        cli.port.unwrap_or(String::from("9100"))
    ))
    .unwrap();

    info!(logger, "listening on http://{}", address);

    let app = app(
        logger,
        cli.ui_dir.unwrap_or(String::from("./ui")),
        address.to_string(),
        weather_data,
    );
    Server::bind(&address)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
