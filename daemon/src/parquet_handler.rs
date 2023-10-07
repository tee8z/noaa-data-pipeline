use anyhow::Error;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    record::RecordWriter,
};
use std::{fs::File, sync::Arc};

use crate::{create_observation_schema, models::parquet::Forecast, station, Mapping, Observation};

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


// TODO: set up sending the two new parquet files generated to the api server
pub fn send_parquet_files(_file_locations: (String, String)) -> Result<(), Error> {
    Ok(())
}