use anyhow::{anyhow, Error};
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
};
use reqwest::Client;
use std::{fs::File, io::Read, sync::Arc};

use crate::{
    create_forecast_schema, create_observation_schema, get_full_path, Forecast, Observation,
};

pub fn save_observations(
    observations: Vec<Observation>,
    root_path: &str,
    file_name: String,
) -> String {
    let full_name = format!("{}/{}.parquet", root_path, file_name);

    let file = File::create(full_name.clone()).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_observation_schema()), Arc::new(props))
            .unwrap();

    let mut row_group = writer.next_row_group().unwrap();
    observations
        .as_slice()
        .write_to_row_group(&mut row_group)
        .unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    full_name
}

pub fn save_forecasts(forecast: Vec<Forecast>, root_path: &str, file_name: String) -> String {
    let full_name = format!("{}/{}.parquet", root_path, file_name);
    let file = File::create(full_name.clone()).unwrap();

    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_forecast_schema()), Arc::new(props))
            .unwrap();

    let mut row_group = writer.next_row_group().unwrap();
    forecast
        .as_slice()
        .write_to_row_group(&mut row_group)
        .unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();
    full_name
}

pub async fn send_parquet_files(
    observation_file: String,
    forecast_file: String,
) -> Result<(), Error> {
    let observation_path = get_full_path(observation_file.clone());
    let forecast_path = get_full_path(forecast_file.clone());

    //TODO: make url configurable
    let url_observ = format!("http://localhost:9100/{}", observation_file);
    let url_forcast = format!("http://localhost:9100/{}", forecast_file);

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
