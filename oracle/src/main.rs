use axum::Server;
use oracle::{app, create_folder, get_config_info, setup_logger, DbManager, OracleService};
use slog::info;
use std::{net::SocketAddr, str::FromStr};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli: oracle::Cli = get_config_info();
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
    let db = DbManager::new(&cli.db_file.unwrap_or(String::from("./oracle_data.db")))?;
    let oracle = OracleService::new(db, cli.private_key_file);

    let app = app(
        logger,
        cli.remote_url
            .unwrap_or(String::from("http://127.0.0.1:9100")),
        cli.ui_dir.unwrap_or(String::from("./ui")),
        weather_data,
        oracle,
    );
    Server::bind(&address)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
