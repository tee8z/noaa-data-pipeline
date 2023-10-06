use anyhow::{anyhow, Error};
use parquet;
use parquet::data_type::AsBytes;
use reqwest::{self, Client};
use std::{fs::File, io::Write, vec};
use tokio;
use xml::reader::XmlEvent;
use xml::EventReader;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let path = r#"./stations.xml"#;

    download_file(path).await?;
    let stations = parse_xml_data(path).await;
    let cleaned: Vec<&Station> = stations
        .iter()
        .filter(|station| station.station_name.to_lowercase().contains("airport"))
        .collect();
    let forecast = get_forecast(cleaned).await;
    let observation = get_observations(cleaned).await;
    write_parquet_files(stations, forecast, observation);
    send_parquet_files();
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

async fn parse_xml_data<'a>(file_path: &str) -> Vec<Station> {
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
                    println!("current_element {}", current_element)
                }
                &_ => {
                    current_element = String::from("");
                }
            },
            Ok(XmlEvent::Characters(val)) => {
                println!("val {}", val);

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
                eprintln!("Error: {e}");
                break;
            }
            _ => {}
        }
    }
    stations
}


async fn get_forecast(aiport_stations: Vec<&Station>) -> Result<(), Error> {
    let url = "https://w1.weather.gov/xml/current_obs/index.xml";
    let client = Client::builder().user_agent("dataFetcher/1.0").build()?;
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("status: {}", response.status()));
    }
    Ok(())
}
/*
fn write_parquet_file(stations: Vec<Station>) {
    let writer = ParquetWriter::new("weather_stations.parquet").unwrap();

    let schema = writer
        .schema_builder()
        .column("station_id", Encoding::Plain)
        .column("state", Encoding::Plain)
        .column("station_name", Encoding::Plain)
        .column("latitude", Encoding::Plain)
        .column("longitude", Encoding::Plain)
        .build()
        .unwrap();

    writer.write_schema(&schema).unwrap();

    for station in stations {
        writer.write_row(&station).unwrap();
    }

    writer.flush().unwrap();

    writer.close().unwrap();
}*/

