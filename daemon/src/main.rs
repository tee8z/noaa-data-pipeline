use daemon::{
    create_folder, get_config_info, get_coordinates, save_forecasts, save_observations,
    send_parquet_files, setup_logger, subfolder_exists, Cli, ForecastService, ObservationService,
    RateLimiter, XmlFetcher,
};
use slog::{debug, error, info, Logger};
use std::{sync::Arc, time::Duration};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
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
        }
    }
}

async fn process_data(
    cli: Cli,
    logger: Logger,
    rate_limiter: Arc<Mutex<RateLimiter>>,
) -> Result<(), anyhow::Error> {
    let logger_cpy = &logger.clone();
    let fetcher = Arc::new(XmlFetcher::new(
        logger.clone(),
        cli.user_agent
            .clone()
            .unwrap_or(String::from("noaa-data-pipeline/1.0")),
        rate_limiter,
    ));

    let city_weather_coordinates = get_coordinates(fetcher.clone()).await?;

    debug!(logger_cpy, "coordinates: {}", city_weather_coordinates);

    let forecast_service = ForecastService::new(logger.clone(), fetcher.clone());
    let forecasts = forecast_service
        .get_forecasts(&city_weather_coordinates)
        .await?;
    debug!(logger_cpy, "forecasts: {:?}", forecasts);

    let observation_service = ObservationService::new(logger, fetcher);
    let observations = observation_service
        .get_observations(&city_weather_coordinates)
        .await?;

    debug!(logger_cpy, "observations: {:?}", observations);
    let current_utc_time: String = OffsetDateTime::now_utc().format(&Rfc3339)?;
    let root_path = cli.data_dir.clone().unwrap_or(String::from("./data"));
    create_folder(&root_path, logger_cpy);
    let current_date = OffsetDateTime::now_utc().date();
    let subfolder = format!("{}/{}", root_path, current_date);
    if !subfolder_exists(&subfolder) {
        create_folder(&subfolder, logger_cpy)
    }
    let forecast_parquet = save_forecasts(
        forecasts,
        &subfolder,
        format!("{}_{}", "forecasts", current_utc_time),
    );
    let observation_parquet = save_observations(
        observations,
        &subfolder,
        format!("{}_{}", "observations", current_utc_time),
    );
    send_parquet_files(&cli, logger_cpy, observation_parquet, forecast_parquet).await?;
    Ok(())
}
