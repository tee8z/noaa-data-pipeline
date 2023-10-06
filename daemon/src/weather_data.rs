use anyhow::{anyhow, Error};
use reqwest::{self, Client};
use slog::{error, Logger};
use std::{fs::File, io::Write, vec};
use xml::reader::XmlEvent;
use xml::EventReader;

//TODO: return path to parquet file
pub async fn load_data(logger: Logger) -> Result<(), Error> {
    let path = r#"./stations.xml"#;

    match download_file(path).await {
        Ok(_) => (),
        Err(e) => error!(logger.clone(), "error downloading station file: {}", e),
    }
    let stations = parse_xml_data(logger.clone(), path).await;
    let cleaned: Vec<&Station> = stations
        .iter()
        .filter(|station| station.station_name.to_lowercase().contains("airport"))
        .collect();
    let _forecast = match get_forecast(cleaned).await {
        Ok(f) => f,
        Err(e) => error!(logger, "error getting forecaset data: {}", e),
    };
    // let observation = get_observations(cleaned).await;
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

#[derive(Default)]
struct Station {
    pub station_id: String,
    pub state: String,
    pub station_name: String,
    pub latitude: f64,
    pub longitude: f64,
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

async fn get_forecast(_aiport_stations: Vec<&Station>) -> Result<(), Error> {
    let url = "https://w1.weather.gov/xml/current_obs/index.xml";
    let client = Client::builder().user_agent("dataFetcher/1.0").build()?;
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("status: {}", response.status()));
    }
    Ok(())
}
