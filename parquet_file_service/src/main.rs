use axum::Server;
use parquet_file_service::{create_folder, app};
use slog::Logger;
use std::net::SocketAddr;

//TODO: make config
const UPLOADS_DIRECTORY: &str = "weather_data";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    create_folder(&format!("./{}", UPLOADS_DIRECTORY));
    let address = SocketAddr::from(([127, 0, 0, 1], 9100));
    println!("listening on http://{}", address);
    //TODO: make ui configuerable
    let app = app(address.to_string(), String::from("./ui"));
    Server::bind(&address)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}