use daemon::{
    create_folder, get_config_info, get_coordinates, get_forecasts, get_observations,
    save_forecasts, save_observations, send_parquet_files, setup_logger, Cli, RateLimiter,
};
use slog::{debug, error, info, Logger};
use std::{sync::Arc, time::Duration};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::signal::ctrl_c;
use tokio::sync::Mutex;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = get_config_info();
    let logger = setup_logger(&cli);

    // Max send 3 requests per 15 second to noaa
    let rate_limiter = Arc::new(Mutex::new(RateLimiter::new(
        cli.token_capacity.unwrap_or(3),
        cli.refill_rate.unwrap_or(15.0_f64),
    )));

    // Run once every hour
    process_weather_data_hourly(cli, logger, Arc::clone(&rate_limiter)).await;
    Ok(())
}

async fn process_weather_data_hourly(
    cli: Cli,
    logger: Logger,
    rate_limit: Arc<Mutex<RateLimiter>>,
) {
    // defaults to once an hour
    let sleep_between_checks = cli.sleep_interval.unwrap_or(3600);
    info!(
        logger,
        "wait time between data pulls: {} seconds", sleep_between_checks
    );
    let mut check_channel_interval = interval(Duration::from_secs(sleep_between_checks));
    loop {
        tokio::select! {
            _ = check_channel_interval.tick() => {
                    match process_data(cli.clone(), logger.clone(), rate_limit.clone()).await {
                        Ok(_) => info!(logger, "finished processing data, waiting an hour to run again"),
                        Err(err) => error!(&logger, "error processing data: {}", err)
                    }
            }
            _ = ctrl_c() => {
                info!(logger, "shutting down");
                break;
            },
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
    let root_path = cli.data_dir.clone().unwrap_or(String::from("./data"));

    create_folder(&root_path, &logger);
    let forecast_parquet = save_forecasts(
        forecasts,
        &root_path,
        format!("{}_{}", "forecasts", current_utc_time),
    );
    let observation_parquet = save_observations(
        observations,
        &root_path,
        format!("{}_{}", "observations", current_utc_time),
    );
    send_parquet_files(&cli, logger, observation_parquet, forecast_parquet).await?;
    Ok(())
}
