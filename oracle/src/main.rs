use axum::serve;
use oracle::{app, create_folder, get_config_info, setup_logger};
use slog::info;
use std::{net::SocketAddr, str::FromStr};
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli: oracle::Cli = get_config_info();
    let logger = setup_logger(&cli);
    let weather_data = cli.weather_dir.unwrap_or(String::from("./weather_data"));
    create_folder(&logger, &weather_data.clone());
    let socket_addr = SocketAddr::from_str(&format!(
        "{}:{}",
        cli.domain.unwrap_or(String::from("127.0.0.1")),
        cli.port.unwrap_or(String::from("9100"))
    ))
    .unwrap();

    let std_listener = std::net::TcpListener::bind(socket_addr)?;
    std_listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(std_listener)?;

    info!(logger, "listening on http://{}", socket_addr);

    let app = app(
        logger,
        cli.remote_url
            .unwrap_or(String::from("http://127.0.0.1:9100")),
        cli.ui_dir.unwrap_or(String::from("./ui")),
        weather_data,
    );
    serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
