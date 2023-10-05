use anyhow::anyhow;
use futures_util::{io::BufReader, StreamExt, TryStreamExt};
use parquet;
use quick_xml::{events::Event, Reader};
use reqwest::{self, Error, Response};
use std::{future::Future, io::Bytes, vec};
use tokio;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let url = "https://w1.weather.gov/xml/current_obs/index.xml";
    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("status: {}", response.status()));
    }

    let stations = parse_xml_data(response);

    // write_parquet_file(stations);
    Ok(())
}

async fn parse_xml_data(response: Response) -> Vec<Station> {
    let stream = response.bytes_stream();
    let mut reader = BufReader::new(stream);
    let mut xml_reader = Reader::from_reader(&reader);
    // Loop over the Reader and read each event.
    loop {
        let event = xml_reader.read_event().unwrap();

        // Process the event as needed.
        match event {
            Event::Start(element) => {
                // The start of a new element.
                // ...
            }
            Event::Text(text) => {
                // The text content of an element.
                // ...
            }
            Event::End(element) => {
                // The end of an element.
                // ...
            }
            Event::Eof => {
                // The end of the stream.
                break;
            }
            _ => {
                // Other types of events, such as comments and processing instructions.
                // ...
            }
        }
    }
    /* let document = parser.parse(xml_data.as_bytes()).unwrap();

        let stations = document
            .root()
            .children()
            .filter(|node| node.tag() == "station")
            .map(|node| {
                Station {
                    station_id: node.attr("id").unwrap().to_string(),
                    state: node.attr("state").unwrap().to_string(),
                    station_name: node.attr("name").unwrap().to_string(),
                    latitude: node.attr("latitude").unwrap().parse::<f64>().unwrap(),
                    longitude: node.attr("longitude").unwrap().parse::<f64>().unwrap(),
                }
            })
            .collect();
    */
    vec![]
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

struct Station {
    station_id: String,
    state: String,
    station_name: String,
    latitude: f64,
    longitude: f64,
}
