use clap::Parser;
use daemon::{
    get_coordinates, get_forecasts, get_observations, save_forecasts, save_observations,
    send_parquet_files, setup_logger, Cli,
};
use slog::debug;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let logger = setup_logger(&cli);

    //TODO: put this in a loop that runs once an hour (data updates every 45 minutes after the hour, not sure what timezone though)
    let city_weather_coordinates = get_coordinates();
    debug!(logger, "coordinates: {}", city_weather_coordinates);
    let forecasts = get_forecasts(&logger, &city_weather_coordinates).await?;
    debug!(logger, "forecasts: {:?}", forecasts);
    let observations = get_observations(&logger, &city_weather_coordinates).await?;
    debug!(logger, "observations: {:?}", observations);
    let current_utc_time: String = OffsetDateTime::now_utc().format(&Rfc3339)?;
    let root_path = "./data";
    let forecast_parquet = save_forecasts(
        forecasts,
        root_path,
        format!("{}_{}", "forecasts", current_utc_time),
    );
    let observation_parquet = save_observations(
        observations,
        root_path,
        format!("{}_{}", "observations", current_utc_time),
    );
    send_parquet_files(&cli, observation_parquet, forecast_parquet).await?;
    // end of loop
    Ok(())
}
