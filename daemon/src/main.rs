use clap::Parser;
use daemon::{
    create_folder, get_coordinates, get_forecasts, get_observations, save_forecasts,
    save_observations, send_parquet_files, setup_logger, Cli, RateLimiter,
};
use slog::{debug, error, Logger};
use std::{sync::Arc, time::Duration};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::Mutex;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let logger = setup_logger(&cli);

    // Max send 2 requests per 20 second to noaa
    let rate_limiter = Arc::new(Mutex::new(RateLimiter::new(3, 20.0)));

    // Run once to start
    process_data(cli.clone(), logger.clone(), Arc::clone(&rate_limiter)).await?;

    // Run every hour after
    process_weather_data_hourly(cli, logger, Arc::clone(&rate_limiter)).await;
    Ok(())
}

async fn process_weather_data_hourly(
    cli: Cli,
    logger: Logger,
    rate_limit: Arc<Mutex<RateLimiter>>,
) {
    let sleep_between_checks = 3600;
    let mut check_channel_interval = interval(Duration::from_secs(sleep_between_checks));
    loop {
        tokio::select! {
            _ = check_channel_interval.tick() => {
                let mut retry_count = 0;
                while retry_count < 3 {
                    match process_data(cli.clone(), logger.clone(), rate_limit.clone()).await {
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

async fn process_data(
    cli: Cli,
    logger: Logger,
    rate_limiter: Arc<Mutex<RateLimiter>>,
) -> Result<(), anyhow::Error> {
    let rate_limiter_coordinates = Arc::clone(&rate_limiter);
    let city_weather_coordinates = get_coordinates(&logger, rate_limiter_coordinates).await?;
    debug!(logger, "coordinates: {}", city_weather_coordinates);
    let rate_limiter_forecast = Arc::clone(&rate_limiter);
    let forecasts =
        get_forecasts(&logger, &city_weather_coordinates, rate_limiter_forecast).await?;
    debug!(logger, "forecasts: {:?}", forecasts);
    let rate_limiter_observation = Arc::clone(&rate_limiter);

    let observations =
        get_observations(&logger, &city_weather_coordinates, rate_limiter_observation).await?;
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
    send_parquet_files(&cli, observation_parquet, forecast_parquet).await?;
    Ok(())
}
