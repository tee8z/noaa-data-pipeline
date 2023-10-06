use anyhow::{anyhow, Error};
use futures::future::join_all;
use reqwest::{self, Client};
use serde::de::DeserializeOwned;
use slog::{error, Logger};
use std::collections::HashMap;
use std::iter::Peekable;
use std::{fs::File, io::Write, vec};
use tokio::task::JoinHandle;
use xml::reader::XmlEvent;
use xml::EventReader;

use crate::models::{
    observation::Root as ObservationRoot,
    station::{Root, Station},
    zone::Root as ZoneRoot,
};
use crate::Mapping;

//TODO: return path to parquet file
pub async fn load_data(logger: Logger) -> Result<(), Error> {
    let path = r#"./stations.xml"#;

    match download_file(path).await {
        Ok(_) => (),
        Err(e) => error!(logger.clone(), "error downloading station file: {}", e),
    }
    let stations = parse_xml_data(logger.clone(), path).await;
    let stations2 = stations.clone();
    let cleaned = stations
        .iter()
        .filter(|station| station.station_name.to_lowercase().contains("airport"))
        .peekable();
    let zones = match get_stations_zones(cleaned).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting station zones: {}", e)),
    }?;
    let mapping = match get_forecast_offices(zones).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting mapping zones: {}", e)),
    }?;
    //get coordinates and observation
    let cleaned2 = stations2
        .iter()
        .filter(|station| station.station_name.to_lowercase().contains("airport"))
        .collect::<Vec<&Station>>();
    let mapping_with_observations = match get_observation(cleaned2, mapping.clone()).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting observation data for station: {}", e)),
    }?;
    // get forecast for coordinates
    let all_station_data = match get_forecast(mapping_with_observations).await {
        Ok(f) => Ok(f),
        Err(e) => Err(anyhow!("error getting forecast data for station: {}", e)),
    }?;

    let parquet_file = save_results(all_station_data).await;
    Ok(())
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

async fn fetch_url<T>(url: &str) -> Result<T, reqwest::Error>
where
    T: DeserializeOwned,
{
    let client = Client::builder().user_agent("dataFetcher/1.0").build()?;
    let response = client.get(url).send().await?;
    let body = response.json::<T>().await?;
    Ok(body)
}

async fn get_stations_zones(
    mut aiport_stations: Peekable<impl Iterator<Item = &Station>>,
) -> Result<Peekable<impl Iterator<Item = (String, String)>>, Error> {
    let page_size = 50;
    let mut station_urls = vec![];
    while let Some(_next) = aiport_stations.peek() {
        let mut next_collection = Vec::new();
        for _ in 0..page_size {
            if let Some(station) = aiport_stations.next() {
                next_collection.push(station.station_id.clone());
            } else {
                break; // No more items to take
            }
        }
        let stations_param = join_url_param(next_collection);
        let url = format!("https://api.weather.gov/stations?id={stations_param}&limit={page_size}");
        let cloned_url = url.clone();
        let task = tokio::spawn(async move { fetch_url::<Root>(&cloned_url).await });
        station_urls.push(task);
    }

    let results = join_all(station_urls)
        .await
        .into_iter()
        .flat_map(|result| {
            let station_to_zone: Vec<(String, String)> = result
                .unwrap()
                .unwrap()
                .features
                .iter()
                .map(|feature| {
                    let zone: Vec<&str> = feature.properties.forecast.split("/").collect();
                    (
                        feature.properties.station_identifier.to_string(),
                        zone.last().unwrap().to_string(),
                    )
                })
                .collect();
            station_to_zone
        })
        .peekable();

    Ok(results)
}

async fn get_forecast_offices(
    mut zones: Peekable<impl Iterator<Item = (String, String)>>,
) -> Result<HashMap<String, Mapping>, Error> {
    let mut zone_urls = vec![];
    let page_size = 50;
    while let Some(_next) = zones.peek() {
        let mut next_collection = Vec::new();
        for _ in 0..page_size {
            if let Some(station) = zones.next() {
                next_collection.push(station.1.clone());
            } else {
                break; // No more items to take
            }
        }
        let zone_params = join_url_param(next_collection);
        let url =
            format!("ttps://api.weather.gov/zones?id={zone_params}&type=land&limit={page_size}");
        let cloned_url = url.clone();
        let task = tokio::spawn(async move { fetch_url::<ZoneRoot>(&cloned_url).await });
        zone_urls.push(task);
    }

    let results = join_all(zone_urls)
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

    Ok(results)
}

fn join_url_param(params: Vec<String>) -> String {
    params.join("%2")
}

async fn get_observation(
    aiport_stations: Vec<&Station>,
    mut mapping: HashMap<String, Mapping>,
) -> Result<HashMap<String, Mapping>, Error> {
    let urls: Vec<_> = aiport_stations
        .iter()
        .map(|station| {
            let url = format!(
                "https://api.weather.gov/stations/{}/observations/latest?require_qc=false",
                station.station_id
            );
            let cloned_url = url.clone();
            let task = tokio::spawn(async move { fetch_url::<ObservationRoot>(&cloned_url).await });
            task
        })
        .collect();

    join_all(urls).await.into_iter().for_each(|re| {
        let root = re.unwrap().unwrap();
        let mut mapping = mapping.get_mut(&root.id).unwrap().clone();
        let lat = root.geometry.coordinates.first().unwrap();
        let long = root.geometry.coordinates.last().unwrap();
        mapping.observation_latitude = lat.abs() as u64;
        mapping.observation_longitude = long.abs() as u64;
        mapping.observation_values = root.properties;
    });

    Ok(mapping)
}

async fn get_forecast(
    mut mapping: HashMap<String, Mapping>,
) -> Result<HashMap<String, Mapping>, Error> {
    let url = "https://w1.weather.gov/xml/current_obs/index.xml";
    let client = Client::builder().user_agent("dataFetcher/1.0").build()?;
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("status: {}", response.status()));
    }
    Ok(mapping)
}


async fn save_results(mapping: HashMap<String, Mapping>) -> Result<String, Error> {
    Ok("path".to_string())
}