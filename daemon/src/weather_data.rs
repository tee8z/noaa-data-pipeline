use crate::models::{
    noaa::forecast::Root as ForecastRoot,
    noaa::observation::Root as ObservationRoot,
    station::{Station, StationRoot},
    zone::Root as ZoneRoot,
};
use crate::{
    parquet_handler::{save_forecasts, save_observations, save_stations},
    Mapping,
};
use anyhow::{anyhow, Error};
use futures::future::join_all;
use reqwest::{self, Client};
use reqwest_middleware::ClientBuilder;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use serde::de::DeserializeOwned;
use slog::{debug, error, info, Logger};
use std::vec;
use std::{collections::HashMap, time::Duration};
use time::OffsetDateTime;
use tokio::time::sleep;

//TODO: return path to parquet file
pub async fn load_data(logger: Logger) -> Result<(String, String), Error> {
    info!(
        logger.clone(),
        "started to download stations and zone information"
    );
    let stations = get_stations(logger.clone()).await;
    let current_utc_time: OffsetDateTime = OffsetDateTime::now_utc();
    save_stations(stations.clone(), format!("{}_{}", "stations", current_utc_time));
    info!(
        logger.clone(),
        "completed downloading station and zones information: {}",
        stations.len()
    );
    info!(
        logger.clone(),
        "started to download forecast offices information"
    );
    let mapping = match get_forecast_offices(logger.clone(), stations.clone()).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting mapping zones: {}", e)),
    }?;
    info!(
        logger.clone(),
        "completed downloading forecast offices information: {}",
        mapping.len()
    );
    info!(logger.clone(), "started to download observations");
    let mapping_with_observations =
        match get_observation(logger.clone(), stations, mapping.clone()).await {
            Ok(f) => Ok(f),
            Err(e) => Err(anyhow!("error getting observation data for station: {}", e)),
        }?;
    info!(
        logger.clone(),
        "completed downloading observations {}",
        mapping.len()
    );

    let values: Vec<_> = mapping_with_observations
        .clone()
        .values()
        .map(|item| Mapping {
            zone_id: item.zone_id.clone(),
            forecast_office_id: item.forecast_office_id.clone(),
            observation_station_id: item.observation_station_id.clone(),
            observation_latitude: item.observation_latitude,
            observation_longitude: item.observation_longitude,
            forecast_values: item.forecast_values.clone(),
            raw_coordinates: item.raw_coordinates.clone(),
            observation_values: item.observation_values.clone(),
        })
        .collect();

    info!(logger.clone(), "starting to download forecasts");
    // get forecast for coordinates
    let all_station_data =
        match get_forecast(logger.clone(), mapping_with_observations, values.iter()).await {
            Ok(f) => Ok(f),
            Err(e) => Err(anyhow!("error getting forecast data for station: {}", e)),
        }?;
    info!(
        logger.clone(),
        "completed downloading forecasts {}",
        all_station_data.len()
    );
    info!(
        logger.clone(),
        "started saving parquet files of observation and forecasts data"
    );

    let parquet_files = match save_results(all_station_data).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!(
            "error saving parquet files for weather data: {}",
            e
        )),
    }?;
    info!(
        logger.clone(),
        "completed saving parquet files of observation and forecasts data"
    );

    Ok(parquet_files)
}

async fn get_stations(logger: Logger) -> Result<Vec<Station>, Error> {
    let station_file = read_station_file("stations");
    if station_file.is_none() {
        let stations = match fetch_station_data(logger.clone(), 500).await {
            Ok(f) => Ok(f),
            Err(e) => Err(anyhow!("error getting station zones: {}", e)),
        }?;
    }

    Ok(stations)
}

async fn fetch_url<T>(logger: &Logger, url: &str) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(Client::builder().user_agent("dataFetcher/1.0").build()?)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    debug!(logger.clone(), "requesting: {}", url);
    let response = client.get(url).send().await.map_err(|e| {
        debug!(logger.clone(), "error sending request: {}", e);
        anyhow!("error sending request: {}", e)
    })?;
    let body = response.json::<T>().await?;
    Ok(body)
}

async fn fetch_station_data(logger: Logger, page_size: usize) -> Result<Vec<Station>, Error> {
    let url = format!("https://api.weather.gov/stations?limit={page_size}");

    let mut next_url = Some(url.to_string());
    let mut all_stations = vec![];
    while let Some(url) = next_url {
        let logger = &logger.clone();
        let response = fetch_url::<StationRoot>(&logger, &url).await?;
        let stations: Vec<Station> = response
            .features
            .iter()
            .filter_map(|feature| {
                if feature.properties.forecast.clone().is_none() {
                    return None;
                }
                let station_id = feature.properties.station_identifier.to_string();
                let forecast_url = feature.properties.forecast.clone().unwrap();
                let zone: Vec<&str> = forecast_url.split("/").collect();
                let zone_id = zone.last().unwrap().to_string();
                let coords = feature.geometry.coordinates.clone();
                let lat = *coords.first().unwrap();
                let long = *coords.last().unwrap();
                Some(Station {
                    station_id,
                    zone_id,
                    station_name: feature.properties.name.to_string(),
                    latitude: lat,
                    longitude: long,
                })
            })
            .collect();
        all_stations.extend(stations);
        next_url = response.pagination.next;
    }

    Ok(all_stations)
}

async fn get_forecast_offices(
    logger: Logger,
    stations: Vec<Station>,
) -> Result<HashMap<String, Mapping>, Error> {
    let mut zone_urls = vec![];
    let page_size = 50;
    let mut station_iter = stations.iter().peekable();
    while let Some(_next) = station_iter.peek() {
        let mut next_collection = Vec::new();
        for _ in 0..page_size {
            if let Some(station) = station_iter.next() {
                next_collection.push(station);
            } else {
                break; // No more items to take
            }
        }
        let zone_ids = next_collection
            .iter()
            .map(|station| station.zone_id.clone())
            .collect();
        let zone_params = join_url_param(zone_ids);
        let url =
            format!("ttps://api.weather.gov/zones?id={zone_params}&type=land&limit={page_size}");
        zone_urls.push(url);
    }
    //TODO: make this configurable
    let chunk_size = 20;
    let mut all_results = HashMap::new();
    let url_chunks: Vec<Vec<String>> = zone_urls
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    for (index, chunks) in url_chunks.iter().enumerate() {
        info!(logger.clone(), "starting to process chunk {}", index);
        let tasks = chunks.iter().map(|url| {
            let logger = logger.clone();
            let url = url.clone();
            tokio::spawn(async move { fetch_url::<ZoneRoot>(&logger, &url).await })
        });
        let results = join_all(tasks)
            .await
            .into_iter()
            .flat_map(|result| {
                let station_zone_office = result
                    .unwrap()
                    .unwrap()
                    .features
                    .iter()
                    .map(|feature| {
                        //TODO: see how often there actually are collections on these
                        let observation_station: Vec<&str> = feature
                            .properties
                            .observation_stations
                            .first()
                            .unwrap()
                            .split("/")
                            .collect();
                        let office: Vec<&str> = feature
                            .properties
                            .forecast_offices
                            .first()
                            .unwrap()
                            .split("/")
                            .collect();
                        let mapping = Mapping {
                            observation_station_id: observation_station.last().unwrap().to_string(),
                            forecast_office_id: office.last().unwrap().to_string(),
                            zone_id: feature.properties.id2.to_string(),
                            ..Default::default()
                        };
                        (mapping.observation_station_id.clone(), mapping)
                    })
                    .collect::<HashMap<String, Mapping>>();
                station_zone_office
            })
            .collect::<HashMap<String, Mapping>>();
        all_results.extend(results);
        info!(logger.clone(), "completed chunk {} and pausing", index);
        //TODO: make this configurable
        let pause_duration = Duration::from_secs(1);
        sleep(pause_duration).await;
    }

    Ok(all_results)
}

fn join_url_param(params: Vec<String>) -> String {
    params.join("%2")
}

async fn get_observation(
    logger: Logger,
    stations: Vec<Station>,
    mut mapping: HashMap<String, Mapping>,
) -> Result<HashMap<String, Mapping>, Error> {
    let station_total = stations.len();
    let step_size = station_total / 10; // 10% progress step size
    let urls: Vec<_> = stations
        .iter()
        .map(|station| {
            format!(
                "https://api.weather.gov/stations/{}/observations/latest?require_qc=false",
                station.station_id
            )
        })
        .collect();

    //TODO: make this configurable
    let chunk_size = 20;
    let url_chunks: Vec<Vec<String>> = urls
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    for (index, chunks) in url_chunks.iter().enumerate() {
        info!(logger.clone(), "starting to process chunk {}", index);
        let tasks = chunks.iter().map(|url| {
            let logger = logger.clone();
            let url = url.clone();
            let task: tokio::task::JoinHandle<Result<ObservationRoot, Error>> =
                tokio::spawn(async move { fetch_url::<ObservationRoot>(&logger, &url).await });
            task
        });

        join_all(tasks).await.into_iter().for_each(|re| {
            let root_res = match re {
                Ok(res) => match res {
                    Ok(res) => Some(res),
                    Err(e) => {
                        error!(logger.clone(), "error getting item: {}", e);
                        None
                    }
                },
                Err(e) => {
                    error!(logger.clone(), "error getting item: {}", e);
                    None
                }
            };
            if root_res.is_none() {
                return;
            }
            let root = root_res.unwrap();
            let station_url = root.properties.station.split("/").last();
            if station_url.is_none() {
                return;
            }
            let find_mapping = mapping.get_mut(station_url.unwrap());
            if find_mapping.is_none() {
                debug!(
                    logger.clone(),
                    "station not found: {}",
                    station_url.unwrap()
                );
                return;
            }
            let mapping: &mut Mapping = find_mapping.unwrap();
            if root.geometry.is_none() {
                debug!(
                    logger.clone(),
                    "station geometry not found: {}",
                    station_url.unwrap()
                );
                return;
            }
            let geo = root.geometry.unwrap();
            mapping.raw_coordinates = geo.coordinates.clone();

            let lat = geo.coordinates.first().unwrap();
            let long = geo.coordinates.last().unwrap();
            mapping.observation_latitude = lat.abs() as u64;
            mapping.observation_longitude = long.abs() as u64;

            mapping.observation_values = root.properties;
        });
        if index % step_size == 0 {
            let progress = (index as f64 / station_total as f64) * 100.0;
            info!(&logger, "Progress: {:.1}%", progress);
        }
        info!(logger.clone(), "completed chunk {} and pausing", index);
        //TODO: make this configurable
        let pause_duration = Duration::from_secs(1);
        sleep(pause_duration).await;
    }

    Ok(mapping)
}

//TODO: fix function to add forecast data to mapping
async fn get_forecast(
    logger: Logger,
    mut mapping: HashMap<String, Mapping>,
    vals: impl Iterator<Item = &Mapping>,
) -> Result<HashMap<String, Mapping>, Error> {
    let station_total = mapping.len();
    let step_size = station_total / 10; // 10% progress step size
    let urls: Vec<_> = vals
        .enumerate()
        .map(|(index, mapping)| {
            let map_clone = mapping.clone();
            let url = format!(
                "https://api.weather.gov/gridpoints/{}/{},{}/forecast?units=us",
                map_clone.forecast_office_id,
                map_clone.observation_latitude,
                map_clone.observation_longitude
            );
            let logger = logger.clone();
            let task = tokio::spawn(async move {
                if index % step_size == 0 {
                    let progress = (index as f64 / station_total as f64) * 100.0;
                    info!(&logger, "Progress: {:.1}%", progress);
                }
                let result = fetch_url::<ForecastRoot>(&logger, &url).await;
                (map_clone.observation_station_id.clone(), result)
            });
            task
        })
        .collect();

    join_all(urls).await.into_iter().for_each(|re| {
        let (observation_id, repsonse) = re.unwrap();
        let root = repsonse.unwrap();
        let mut mapping = mapping.get_mut(&observation_id).unwrap().clone();
        mapping.forecast_values = root.properties;
    });

    Ok(mapping)
}

async fn save_results(mapping: HashMap<String, Mapping>) -> Result<(String, String), Error> {
    let values: Vec<&Mapping> = mapping.values().collect();
    let current_utc_time: OffsetDateTime = OffsetDateTime::now_utc();
    let observations_parquet = save_observations(
        values.clone(),
        format!("{}_{}", "observations", current_utc_time),
    );
    let forecast_parquet = save_forecasts(values, format!("{}_{}", "forecasts", current_utc_time));
    Ok((observations_parquet, forecast_parquet))
}
