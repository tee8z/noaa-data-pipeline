use crate::{
    models::{
        noaa::forecast::Root as ForecastRoot,
        noaa::observation::Root as ObservationRoot,
        station::{Station, StationRoot},
        zone::Root as ZoneRoot,
    },
    read_station_file, FileError,
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
use std::{collections::HashMap, time::Duration};
use std::vec;
use time::OffsetDateTime;
use tokio::time::sleep;

//TODO: return path to parquet file
pub async fn load_data(logger: Logger) -> Result<(String, String), Error> {
    info!(
        logger.clone(),
        "started to download stations and zone information"
    );
    let root_data = "./data";
    let stations = get_stations(logger.clone(), root_data).await?;
    let current_utc_time: OffsetDateTime = OffsetDateTime::now_utc();
    save_stations(
        stations.clone(),
        format!("{}/stations_{}", root_data, current_utc_time),
    );
    info!(
        logger.clone(),
        "completed downloading station and zones information: {}",
        stations.len()
    );
    info!(
        logger.clone(),
        "started to download forecast offices information"
    );
    let forecast_data = match get_forecast_offices(logger.clone(), stations.clone()).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting mapping zones: {}", e)),
    }?;
    info!(
        logger.clone(),
        "completed downloading forecast offices information: {}",
        forecast_data.mappings.len()
    );
    info!(logger.clone(), "started to download observations");
    let mapping_with_observations = match get_observation(
        logger.clone(),
        forecast_data.stations,
        forecast_data.mappings.clone(),
    )
    .await
    {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting observation data for station: {}", e)),
    }?;
    info!(
        logger.clone(),
        "completed downloading observations {}",
        forecast_data.mappings.len()
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

    let parquet_files = match save_results("./data", all_station_data).await {
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

async fn get_stations(logger: Logger, root_data: &str) -> Result<Vec<Station>, Error> {
    // TODO: make root_path configurable
    match read_station_file(logger.clone(), root_data, String::from("stations")) {
        Ok(stations) => Ok(stations),
        Err(FileError::NotFound) => match fetch_station_data(logger.clone(), 500, 5).await {
            Ok(f) => Ok(f),
            Err(e) => Err(anyhow!("error getting station zones: {}", e)),
        },
    }
}

async fn fetch_url<T>(logger: &Logger, url: &str) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(Client::builder().user_agent("fetching_data/1.0").build()?)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    debug!(logger.clone(), "requesting: {}", url);
    let response = client.get(url).send().await.map_err(|e| {
        error!(logger.clone(), "error sending request: {}", e);
        anyhow!("error sending request: {}", e)
    })?;
    match response.json::<T>().await {
        Ok(body) => Ok(body),
        Err(e) => {
            error!(logger.clone(), "error sending request: {}", e);
            Err(anyhow!("error parsing body of request: {}", e))
        }
    }
}

async fn fetch_station_data(
    logger: Logger,
    page_size: usize,
    max_calls: usize,
) -> Result<Vec<Station>, Error> {
    let url = format!("https://api.weather.gov/stations?limit={page_size}");

    let mut next_url = Some(url.to_string());
    let mut all_stations = vec![];
    let mut call_count = 0;
    while let Some(url) = next_url {
        if max_calls < call_count {
            break;
        }
        call_count += 1;
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

pub struct ForecastOfficeData {
    pub stations: Vec<Station>,
    pub mappings: HashMap<String, Mapping>,
}

async fn get_forecast_offices(
    logger: Logger,
    stations: Vec<Station>,
) -> Result<ForecastOfficeData, Error> {
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
            format!("https://api.weather.gov/zones?id={zone_params}&type=land&limit={page_size}");
        zone_urls.push(url);
    }
    //TODO: make this configurable
    let chunk_size = 20;
    let mut all_results = HashMap::new();
    let url_chunks: Vec<Vec<String>> = zone_urls
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    let mut stations_found: Vec<Station> = vec![];
    for (index, chunks) in url_chunks.iter().enumerate() {
        info!(logger.clone(), "starting to process chunk {}", index);
        let tasks = chunks.iter().map(|url| {
            let logger = logger.clone();
            let url = url.clone();
            tokio::spawn(async move { fetch_url::<ZoneRoot>(&logger, &url).await })
        });
        let tasks_results = join_all(tasks).await;
        let results = tasks_results
            .into_iter()
            .filter_map(|result| {
                let res = result.unwrap();
                let features = res.unwrap().features;

                if features.is_none() {
                    return None;
                }
                let station_zone_office = features
                    .unwrap()
                    .iter()
                    .filter_map(|feature| {
                        //TODO: see how often there actually are more than 1 item in this collections
                        let first_station = feature.properties.observation_stations.first();
                        if first_station.is_none() {
                            return None;
                        }
                        let observation_station: Vec<&str> =
                            first_station.unwrap().split("/").collect();
                        let office: Vec<&str> = first_station.unwrap().split("/").collect();
                        let mapping = Mapping {
                            observation_station_id: observation_station.last().unwrap().to_string(),
                            forecast_office_id: office.last().unwrap().to_string(),
                            zone_id: feature.properties.id2.to_string(),
                            ..Default::default()
                        };
                        match stations
                            .iter()
                            .find(|station| station.station_id == mapping.observation_station_id)
                        {
                            Some(station) => stations_found.push(station.to_owned()),
                            None => (),
                        }
                        Some((mapping.observation_station_id.clone(), mapping))
                    })
                    .collect::<HashMap<String, Mapping>>();
                Some(station_zone_office)
            })
            .flat_map(|map| map)
            .collect::<HashMap<String, Mapping>>();
        all_results.extend(results.clone());
        info!(
            logger.clone(),
            "completed chunk {} found {} and pausing",
            index,
            results.len()
        );
        //TODO: make this configurable
        let pause_duration = Duration::from_secs(1);
        sleep(pause_duration).await;
    }
    Ok(ForecastOfficeData {
        stations: stations_found,
        mappings: all_results,
    })
}

fn join_url_param(params: Vec<String>) -> String {
    params.join("%2C")
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

        let tasks_results = join_all(tasks).await;
        tasks_results.into_iter().for_each(|re| {
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
            if root.clone().properties.is_none() {
                return;
            }
            let properties = root.clone().properties.unwrap();
            let station_url = properties.station.split("/").last();
            if station_url.is_none() {
                return;
            }
            let find_mapping = mapping.get_mut(station_url.unwrap().trim());
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

            mapping.observation_values = root.properties.unwrap();
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

async fn save_results(
    root_path: &str,
    mapping: HashMap<String, Mapping>,
) -> Result<(String, String), Error> {
    let values: Vec<&Mapping> = mapping.values().collect();
    let current_utc_time: OffsetDateTime = OffsetDateTime::now_utc();
    let observations_parquet = save_observations(
        values.clone(),
        root_path,
        format!("{}_{}", "observations", current_utc_time),
    );
    let forecast_parquet = save_forecasts(
        values,
        root_path,
        format!("{}_{}", "forecasts", current_utc_time),
    );
    Ok((observations_parquet, forecast_parquet))
}
