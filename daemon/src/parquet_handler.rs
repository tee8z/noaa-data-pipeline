use anyhow::{anyhow, Error};
use parquet::{
    file::{
        properties::WriterProperties, reader::FileReader, serialized_reader::SerializedFileReader,
        writer::SerializedFileWriter,
    },
    record::RecordWriter,
};
use reqwest::Client;
use slog::Logger;
use std::error::Error as ErrorStd;
use std::{
    env, fmt,
    fs::{self, File},
    io::Read,
    sync::Arc,
};

use crate::{
    create_observation_schema, create_station_schema, models::parquet::Forecast, station, Mapping,
    Observation, Station,
};

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

pub fn read_station_file(
    _logger: Logger,
    root_path: &str,
    partial_name: String,
) -> Result<Vec<Station>, FileError> {
    let parquet_file_path = get_by_partial_name(root_path, &partial_name)?;
    let file = std::fs::File::open(parquet_file_path).expect("Failed to open file");

    let reader = SerializedFileReader::new(file).unwrap();
    let mut rows = reader.get_row_iter(None).unwrap();
    let mut stations: Vec<Station> = Vec::new();
    while let Some(record) = rows.next() {
        //TODO: gracefully handle error
        let row = record.expect("Failed to read record");

        let station = Station {
            ..Default::default()
        };
        //TODO: gracefully handle error
        let updated_station = station
            .from_parquet_row(&row)
            .expect("Failed to convert Parquet record to struct");

        stations.push(updated_station);
    }
    Ok(stations)
}

#[derive(Debug)]
pub enum FileError {
    NotFound,
}

impl Default for FileError {
    fn default() -> Self {
        FileError::NotFound
    }
}

impl ErrorStd for FileError {
    fn description(&self) -> &str {
        match self {
            FileError::NotFound => "file path not found",
        }
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error: {}", self.to_string())
    }
}

fn get_by_partial_name(root_path: &str, search_string: &str) -> Result<String, FileError> {
    let mut matching_files = Vec::new();

    if let Ok(entries) = fs::read_dir(root_path) {
        for entry in entries {
            if let Ok(entry) = entry {
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();

                if file_name_str.contains(search_string) {
                    let file_path = entry.path();
                    matching_files.push(file_path);
                }
            }
        }
    }

    matching_files.sort_by(|a, b| {
        let timestamp_a = extract_utc_timestamp(a);
        let timestamp_b = extract_utc_timestamp(b);
        timestamp_b.cmp(&timestamp_a) // Sort in descending order (newest first)
    });

    match matching_files.first() {
        Some(path) => Ok(path.to_string_lossy().to_string().to_owned()),
        None => Err(FileError::NotFound),
    }
}

fn extract_utc_timestamp(file_path: &std::path::PathBuf) -> i64 {
    if let Some(file_name) = file_path.file_name() {
        if let Some(file_name_str) = file_name.to_str() {
            if let Some(timestamp_str) = file_name_str.rsplitn(2, '_').nth(0) {
                if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                    return timestamp;
                }
            }
        }
    }
    0 // Default timestamp in case of parsing errors
}

pub fn save_observations(mappings: Vec<&Mapping>, root_path: &str, file_name: String) -> String {
    let full_name = format!("{}/{}.parquet", root_path, file_name);

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

pub fn save_forecasts(mappings: Vec<&Mapping>, root_path: &str, file_name: String) -> String {
    let full_name = format!("{}/{}.parquet", root_path, file_name);
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
