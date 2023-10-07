use std::{fs::File, sync::Arc};

use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};

use crate::{create_observation_schema, Mapping, Observation};

fn save_observations(mappings: Vec<Mapping>) {
    let file = File::create("my_structs.parquet")?;
    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_observation_schema()), Arc::new(props))?;

    for item in mappings {
        let observation: Observation = item.into();
        writer.write(&observation.to_parquet_record())?;
    }

    writer.close()?;
}

fn save_forecasts(mappings: Vec<Mapping>) {
    let file = File::create("my_structs.parquet")?;
    let props = WriterProperties::builder().build();
    let mut writer =
        SerializedFileWriter::new(file, Arc::new(create_observation_schema()), Arc::new(props))?;

    for item in mappings {
        let forecast: Forecast = item.into();
        writer.write(&forecast.to_parquet_record())?;
    }

    writer.close()?;
}
