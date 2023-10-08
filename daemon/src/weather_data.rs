use crate::models::{
    noaa::forecast::Root as ForecastRoot,
    noaa::observation::Root as ObservationRoot,
    station::{Root, Station},
    zone::Root as ZoneRoot,
};
use crate::{
    parquet_handler::{save_forecasts, save_observations},
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
use std::iter::Peekable;
use std::{collections::HashMap, time::Duration};
use std::{fs::File, io::Write, vec};
use time::OffsetDateTime;
use tokio::time::sleep;
use xml::reader::XmlEvent;
use xml::EventReader;

//TODO: return path to parquet file
pub async fn load_data(logger: Logger) -> Result<(String, String), Error> {
    let path = r#"./stations.xml"#;
    info!(logger.clone(), "started downloading stations file");
    match download_file(path).await {
        Ok(_) => (),
        Err(e) => error!(logger.clone(), "error downloading station file: {}", e),
    }
    let stations = parse_xml_data(logger.clone(), path).await;
    let stations2 = stations.clone();
    let cleaned: Vec<Station> = stations
        .iter()
        .filter(|station| station.station_name.to_lowercase().contains("airport"))
        .map(|station| Station {
            station_id: station.station_id.to_string(),
            state: station.state.to_string(),
            station_name: station.station_name.to_string(),
            latitude: station.latitude,
            longitude: station.longitude,
        })
        .collect();
    info!(logger.clone(), "completed stations file download");
    info!(
        logger.clone(),
        "started to download station zones information"
    );
    let zones = match get_stations_zones(logger.clone(), cleaned).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting station zones: {}", e)),
    }?;
    info!(
        logger.clone(),
        "completed downloading station zones information: {}",
        zones.len()
    );
    info!(
        logger.clone(),
        "started to download forecast offices information"
    );
    let mapping = match get_forecast_offices(logger.clone(), zones).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting mapping zones: {}", e)),
    }?;
    info!(
        logger.clone(),
        "completed downloading forecast offices information: {}",
        mapping.len()
    );
    info!(logger.clone(), "started to download observations");
    let cleaned2 = stations2
        .iter()
        .filter(|station| station.station_name.to_lowercase().contains("airport"))
        .collect::<Vec<&Station>>();
    let mapping_with_observations =
        match get_observation(logger.clone(), cleaned2, mapping.clone()).await {
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

async fn download_file(path: &str) -> Result<(), anyhow::Error> {
    let url = "https://w1.weather.gov/xml/current_obs/index.xml";
    let client = Client::builder().user_agent("dataFetcher/1.0").build()?;
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("status: {}", response.status()));
    }
    let mut output_file = File::create(path)?;
    output_file
        .write_all(&response.bytes().await.unwrap())
        .unwrap();
    output_file.flush()?;
    Ok(())
}

async fn parse_xml_data<'a>(logger: Logger, file_path: &str) -> Vec<Station> {
    let file = File::open(file_path).unwrap();
    // Deserialize the XML into a Vec<Station> where each Station corresponds to a <station> element
    let mut stations = vec![];
    let parser = EventReader::new(file);
    let mut current_element = String::from("");
    let mut current_station: Option<Station> = None;
    for event in parser {
        match event {
            Ok(XmlEvent::StartElement { name, .. }) => match name.local_name.as_str() {
                "station" => {
                    current_station = Some(Station {
                        ..Default::default()
                    })
                }
                "station_id" | "longitude" | "latitude" | "station_name" | "state" => {
                    current_element = name.local_name.to_string();
                }
                &_ => {
                    current_element = String::from("");
                }
            },
            Ok(XmlEvent::Characters(val)) => {
                if let Some(ref mut station) = current_station.as_mut() {
                    match current_element.as_str() {
                        "station_id" => station.station_id = val,
                        "longitude" => station.longitude = val.parse::<f64>().unwrap(),
                        "latitude" => station.latitude = val.parse::<f64>().unwrap(),
                        "station_name" => station.station_name = val,
                        "state" => station.state = val,
                        &_ => {}
                    }
                }
            }
            Ok(XmlEvent::EndElement { name }) => match name.local_name.as_str() {
                "station" => {
                    if let Some(station) = current_station.as_ref() {
                        stations.push(Station {
                            station_id: station.station_id.to_string(),
                            state: station.state.to_string(),
                            station_name: station.station_name.to_string(),
                            latitude: station.latitude,
                            longitude: station.longitude,
                        })
                    }
                }
                _ => (),
            },
            Err(e) => {
                error!(logger, "error parsing station xml file: {}", e);
                break;
            }
            _ => {}
        }
    }
    stations
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

async fn get_stations_zones(
    logger: Logger,
    stations: Vec<Station>,
) -> Result<Vec<(String, String)>, Error> {
    let page_size = 50;
    let mut station_urls = vec![];
    let mut stations_iter = stations.iter().peekable();
    while let Some(_next) = stations_iter.peek() {
        let mut next_collection = Vec::new();
        for _ in 0..page_size {
            if let Some(station) = stations_iter.next() {
                next_collection.push(station.station_id.clone());
            } else {
                break; // No more items to take
            }
        }
        let stations_param = join_url_param(next_collection);
        let url = format!("https://api.weather.gov/stations?id={stations_param}&limit={page_size}");
        let logger = logger.clone();
        let task = tokio::spawn(async move { fetch_url::<Vec<Root>>(&logger, &url).await });
        station_urls.push(task);
    }

    let results = join_all(station_urls)
        .await
        .into_iter()
        .filter_map(|result| {
            let root_res = match result {
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
                return None;
            }

            let station_to_zone: Vec<(String, String)> = root_res
                .unwrap()
                .iter()
                .flat_map(|root| {
                    root.features.iter().map(|feature| {
                        let zone: Vec<&str> = feature.properties.forecast.split("/").collect();
                        (
                            feature.properties.station_identifier.to_string(),
                            zone.last().unwrap().to_string(),
                        )
                    })
                })
                .collect();
            Some(station_to_zone)
        })
        .flat_map(|item| item)
        .collect();

    Ok(results)
}

async fn get_forecast_offices(
    logger: Logger,
    zones: Vec<(String, String)>,
) -> Result<HashMap<String, Mapping>, Error> {
    let mut zone_urls = vec![];
    let page_size = 50;
    let mut zone_iter = zones.iter().peekable();
    while let Some(_next) = zone_iter.peek() {
        let mut next_collection = Vec::new();
        for _ in 0..page_size {
            if let Some(station) = zone_iter.next() {
                next_collection.push(station.1.clone());
            } else {
                break; // No more items to take
            }
        }
        let zone_params = join_url_param(next_collection);
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
    aiport_stations: Vec<&Station>,
    mut mapping: HashMap<String, Mapping>,
) -> Result<HashMap<String, Mapping>, Error> {
    let station_total = aiport_stations.len();
    let step_size = station_total / 10; // 10% progress step size
    let urls: Vec<_> = aiport_stations
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
