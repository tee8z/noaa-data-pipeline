use anyhow::{anyhow, Error};
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
};
use reqwest::Client;
use slog::Logger;
use std::{env, fs::File, io::Read, sync::Arc};

use crate::{create_observation_schema, models::parquet::Forecast, station, Mapping, Observation, Station, create_station_schema};

pub fn save_stations(stations: Vec<Station>, file_name: String) -> String {
    let full_name = format!("{}.parquet", file_name);

    let file = File::create(full_name.clone()).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_station_schema()), Arc::new(props))
            .unwrap();

    let mut row_group = writer.next_row_group().unwrap();
    stations
        .as_slice()
        .write_to_row_group(&mut row_group)
        .unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    full_name
}

pub fn read_station_file(logger: Logger, partial_name: String) -> String {
    
}

pub fn save_observations(mappings: Vec<&Mapping>, file_name: String) -> String {
    let full_name = format!("{}.parquet", file_name);

    let file = File::create(full_name.clone()).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_observation_schema()), Arc::new(props))
            .unwrap();

    let observations: Vec<Observation> = mappings.iter().map(|data| (*data).into()).collect();

    let mut row_group = writer.next_row_group().unwrap();
    observations
        .as_slice()
        .write_to_row_group(&mut row_group)
        .unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    full_name
}

pub fn save_forecasts(mappings: Vec<&Mapping>, file_name: String) -> String {
    let full_name = format!("{}.parquet", file_name);
    let file = File::create(full_name.clone()).unwrap();

    let forecasts: Vec<Forecast> = mappings
        .iter()
        .flat_map(|data| <&station::Mapping as Into<Vec<Forecast>>>::into(*data))
        .collect();

    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_observation_schema()), Arc::new(props))
            .unwrap();

    let mut row_group = writer.next_row_group().unwrap();
    forecasts
        .as_slice()
        .write_to_row_group(&mut row_group)
        .unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    full_name
}
pub async fn send_parquet_files(file_locations: (String, String)) -> Result<(), Error> {
    let observation_path = get_full_path(file_locations.0.clone());
    let forecast_path = get_full_path(file_locations.1.clone());
    //TODO: make url configurable
    let url_observ = format!("http://localhost:9100/{}", file_locations.0);
    let url_forcast = format!("http://localhost:9100/{}", file_locations.1);

    send_file_to_endpoint(&observation_path, &url_observ).await?;
    send_file_to_endpoint(&forecast_path, &url_forcast).await?;
    Ok(())
}

async fn send_file_to_endpoint(file_path: &str, endpoint_url: &str) -> Result<(), anyhow::Error> {
    // Create a reqwest client.
    let client = Client::new();

    // Open the file for reading.
    let mut file =
        File::open(file_path).map_err(|e| anyhow!("error opening file to upload: {}", e))?;

    // Create a buffer to read the file data into.
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .map_err(|e| anyhow!("error reading file to buffer: {}", e))?;

    // Create a request builder for a POST request to the endpoint.
    let request = client.post(endpoint_url).body(buffer);

    // Send the request and handle the response.
    let response = request
        .send()
        .await
        .map_err(|e| anyhow!("error sending file to api: {}", e))?;

    // Check the response status.
    if response.status().is_success() {
        println!("File successfully uploaded.");
    } else {
        println!(
            "Failed to upload the file. Status code: {:?}",
            response.status()
        );
    }

    Ok(())
}

fn get_full_path(relative_path: String) -> String {
    let mut current_dir = env::current_dir().expect("Failed to get current directory");

    // Append the relative path to the current working directory
    current_dir.push(relative_path);

    // Convert the `PathBuf` to a `String` if needed
    current_dir.to_string_lossy().to_string()
}
