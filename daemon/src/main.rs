use clap::Parser;
use daemon::{
    create_folder, get_coordinates, get_forecasts, get_observations, save_forecasts,
    save_observations, send_parquet_files, setup_logger, Cli,
};
use slog::{debug, error, Logger};
use std::time::Duration;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let logger = setup_logger(&cli);

    // Run once to start
    process_data(cli.clone(), logger.clone()).await?;

    // Run every hour after
    //process_weather_data_hourly(cli, logger).await;
    Ok(())
}

async fn process_weather_data_hourly(cli: Cli, logger: Logger) {
    let sleep_between_checks = 3600;
    let mut check_channel_interval = interval(Duration::from_secs(sleep_between_checks));
    loop {
        tokio::select! {
            _ = check_channel_interval.tick() => {
                let mut retry_count = 0;
                while retry_count < 3 {
                    match process_data(cli.clone(), logger.clone()).await {
                        Ok(_) => {
                            // Break the loop if the processing is successful
                            break;
                        }
                        Err(err) => {
                            // Log the error or take appropriate action
                            error!(&logger, "Error processing data (trying again): {}", err);
                            // Increment the retry count
                            retry_count += 1;
                        }
                    }
                }
                if retry_count > 0 {
                    error!(&logger, "Tried processing three times, giving up until next hour: {}", OffsetDateTime::now_utc());
                }
            }
        }
    }
}

async fn process_data(cli: Cli, logger: Logger) -> Result<(), anyhow::Error> {
    let city_weather_coordinates = get_coordinates(&logger).await?;
    debug!(logger, "coordinates: {}", city_weather_coordinates);
    let forecasts = get_forecasts(&logger, &city_weather_coordinates).await?;
    debug!(logger, "forecasts: {:?}", forecasts);
    let observations = get_observations(&logger, &city_weather_coordinates).await?;
    debug!(logger, "observations: {:?}", observations);
    let current_utc_time: String = OffsetDateTime::now_utc().format(&Rfc3339)?;
    let root_path = "./data";
    create_folder(root_path, &logger);
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
    //send_parquet_files(&cli, observation_parquet, forecast_parquet).await?;
    Ok(())
}
