use std::{fs::File, sync::Arc};

use anyhow::{anyhow, Error};
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
};
use reqwest::{multipart, Body, Client};
use tokio::fs::File as TokioFile;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::{
    create_forecast_schema, create_observation_schema, get_full_path, Cli, Forecast, Observation,
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
    cli: &Cli,
    observation_relative_file_path: String,
    forecast_relative_file_path_file: String,
) -> Result<(), Error> {
    let base_url = cli.base_url.clone().unwrap_or(String::from("http://localhost:9100"));
    let observation_filename = observation_relative_file_path.split('/').last().unwrap();
    let forecast_filename = forecast_relative_file_path_file.split('/').last().unwrap();

    let observation_full_path = get_full_path(observation_relative_file_path.clone());
    let forecast_full_path = get_full_path(forecast_relative_file_path_file.clone());

    let url_observ = format!("{}/file/{}", base_url, observation_filename);
    let url_forcast = format!("{}/file/{}", base_url, forecast_filename);

    send_file_to_endpoint(&observation_full_path, observation_filename, &url_observ).await?;
    send_file_to_endpoint(&forecast_full_path, forecast_filename, &url_forcast).await?;
    Ok(())
}

async fn send_file_to_endpoint(
    file_path: &str,
    file_name: &str,
    endpoint_url: &str,
) -> Result<(), anyhow::Error> {
    // Create a reqwest client.
    let client = Client::new();

    // Open the file for reading.
    let file = TokioFile::open(file_path)
        .await
        .map_err(|e| anyhow!("error opening file to upload: {}", e))?;

    // read file body stream
    let stream = FramedRead::new(file, BytesCodec::new());
    let file_body = Body::wrap_stream(stream);

    //make form part of file
    let parquet_file = multipart::Part::stream(file_body)
        .file_name(file_name.to_owned())
        .mime_str("application/parquet")?;

    let form = multipart::Form::new().part("file", parquet_file);

    // Create a request builder for a POST request to the endpoint.
    println!("endpoint: {}", endpoint_url);
    let response = client
        .post(endpoint_url)
        .multipart(form)
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
