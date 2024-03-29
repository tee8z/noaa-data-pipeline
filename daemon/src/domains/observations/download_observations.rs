use anyhow::{anyhow, Error};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    schema::types::Type,
};
use parquet_derive::ParquetRecordWriter;
use slog::{debug, error, Logger};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Read,
    sync::Arc,
};
use time::{format_description::well_known::Rfc2822, macros::format_description, OffsetDateTime};
use zip::ZipArchive;

use crate::{CityWeather, CurrentObservation, Units, XmlFetcher};

#[derive(Clone)]
pub struct CurrentWeather {
    pub station_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub generated_at: OffsetDateTime,
    pub temperature_value: Option<f64>,
    pub temperature_unit_code: String,
    pub relative_humidity: Option<i64>,
    pub relative_humidity_unit_code: String,
    pub wind_direction: Option<i64>,
    pub wind_direction_unit_code: String,
    pub wind_speed: Option<i64>,
    pub wind_speed_unit_code: String,
    pub dewpoint_value: Option<f64>,
    pub dewpoint_unit_code: String,
}

impl TryFrom<CurrentObservation> for CurrentWeather {
    type Error = anyhow::Error;
    fn try_from(val: CurrentObservation) -> Result<Self, Self::Error> {
        Ok(CurrentWeather {
            station_id: val.station_id,
            latitude: val.latitude.parse::<f64>()?,
            longitude: val.longitude.parse::<f64>()?,
            generated_at: OffsetDateTime::parse(
                &val.observation_time_rfc822
                    .unwrap_or(OffsetDateTime::now_utc().to_string()),
                &Rfc2822,
            )
            .map_err(|e| anyhow!("error parsing observation_time time: {}", e))?,
            temperature_value: val
                .temp_f
                .unwrap_or(String::from(""))
                .parse::<f64>()
                .map(Some)
                .unwrap_or(None),
            temperature_unit_code: Units::Fahrenheit.to_string(),
            relative_humidity: val
                .relative_humidity
                .unwrap_or(String::from(""))
                .parse::<i64>()
                .map(Some)
                .unwrap_or(None),
            relative_humidity_unit_code: Units::Percent.to_string(),
            wind_direction: val
                .wind_degrees
                .unwrap_or(String::from(""))
                .parse::<i64>()
                .map(Some)
                .unwrap_or(None),
            wind_direction_unit_code: Units::DegreesTrue.to_string(),
            wind_speed: val
                .wind_kt
                .unwrap_or(String::from(""))
                .parse::<i64>()
                .map(Some)
                .unwrap_or(None),
            wind_speed_unit_code: Units::Knots.to_string(),
            dewpoint_value: val
                .dewpoint_f
                .unwrap_or(String::from(""))
                .parse::<f64>()
                .map(Some)
                .unwrap_or(None),
            dewpoint_unit_code: Units::Fahrenheit.to_string(),
        })
    }
}

#[derive(Debug, ParquetRecordWriter)]
pub struct Observation {
    pub station_id: String,
    pub station_name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub generated_at: String,
    pub temperature_value: Option<f64>,
    pub temperature_unit_code: String,
    pub relative_humidity: Option<i64>,
    pub relative_humidity_unit_code: String,
    pub wind_direction: Option<i64>,
    pub wind_direction_unit_code: String,
    pub wind_speed: Option<i64>,
    pub wind_speed_unit_code: String,
    pub dewpoint_value: Option<f64>,
    pub dewpoint_unit_code: String,
}

impl TryFrom<CurrentWeather> for Observation {
    type Error = anyhow::Error;
    fn try_from(val: CurrentWeather) -> Result<Self, Self::Error> {
        let rfc_3339_time_description =
            format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
        let parquet = Observation {
            station_id: val.station_id,
            station_name: String::from(""),
            latitude: val.latitude,
            longitude: val.longitude,
            generated_at: val
                .generated_at
                .format(rfc_3339_time_description)
                .map_err(|e| anyhow!("error formatting generated_at time: {}", e))?,
            temperature_value: val.temperature_value,
            temperature_unit_code: val.temperature_unit_code,
            wind_speed: val.wind_speed,
            wind_speed_unit_code: val.wind_speed_unit_code,
            wind_direction: val.wind_direction,
            wind_direction_unit_code: val.wind_direction_unit_code,
            relative_humidity: val.relative_humidity,
            relative_humidity_unit_code: val.relative_humidity_unit_code,
            dewpoint_value: val.dewpoint_value,
            dewpoint_unit_code: val.dewpoint_unit_code,
        };
        Ok(parquet)
    }
}

pub fn create_observation_schema() -> Type {
    let station_id = Type::primitive_type_builder("station_id", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(Some(LogicalType::String))
        .build()
        .unwrap();

    let station_name = Type::primitive_type_builder("station_name", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(Some(LogicalType::String))
        .build()
        .unwrap();

    let latitude = Type::primitive_type_builder("latitude", PhysicalType::DOUBLE)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let longitude = Type::primitive_type_builder("longitude", PhysicalType::DOUBLE)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let generated_at = Type::primitive_type_builder("generated_at", PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(LogicalType::String))
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let temperature_value = Type::primitive_type_builder("temperature_value", PhysicalType::DOUBLE)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let temperature_unit_code =
        Type::primitive_type_builder("temperature_unit_code", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();

    let relative_humidity = Type::primitive_type_builder("relative_humidity", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let relative_humidity_unit_code =
        Type::primitive_type_builder("relative_humidity_unit_code", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();

    let wind_direction = Type::primitive_type_builder("wind_direction", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_direction_unit_code =
        Type::primitive_type_builder("wind_direction_unit_code", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();

    let wind_speed = Type::primitive_type_builder("wind_speed", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_unit_code =
        Type::primitive_type_builder("wind_speed_unit_code", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();

    let dewpoint_value = Type::primitive_type_builder("dewpoint_value", PhysicalType::DOUBLE)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let dewpoint_unit_code =
        Type::primitive_type_builder("dewpoint_unit_code", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .unwrap();

    let schema = Type::group_type_builder("observation")
        .with_fields(vec![
            Arc::new(station_id),
            Arc::new(station_name),
            Arc::new(latitude),
            Arc::new(longitude),
            Arc::new(generated_at),
            Arc::new(temperature_value),
            Arc::new(temperature_unit_code),
            Arc::new(relative_humidity),
            Arc::new(relative_humidity_unit_code),
            Arc::new(wind_direction),
            Arc::new(wind_direction_unit_code),
            Arc::new(wind_speed),
            Arc::new(wind_speed_unit_code),
            Arc::new(dewpoint_value),
            Arc::new(dewpoint_unit_code),
        ])
        .build()
        .unwrap();

    schema
}

pub struct ObservationService {
    pub logger: Logger,
    pub fetcher: Arc<XmlFetcher>,
}
impl ObservationService {
    pub fn new(logger: Logger, fetcher: Arc<XmlFetcher>) -> Self {
        ObservationService { logger, fetcher }
    }
    pub async fn get_observations(
        &self,
        city_weather: &CityWeather,
    ) -> Result<Vec<Observation>, Error> {
        let url = "https://w1.weather.gov/xml/current_obs/all_xml.zip";
        let zip_file = self.fetcher.fetch_xml_zip(url).await?;
        let find_file_indexies =
            find_file_indexes_for_stations(zip_file.try_clone()?, city_weather.get_station_ids())?;
        let current_weather = self.parse_weather_data(zip_file, find_file_indexies)?;
        let mut observations = vec![];
        for value in current_weather.values() {
            let current = value.clone();
            let mut observation: Observation = current.try_into()?;
            let city = city_weather.city_data.get(&observation.station_id).unwrap();
            observation.station_name = city.station_name.clone();
            observations.push(observation)
        }
        Ok(observations)
    }

    fn parse_weather_data(
        &self,
        zip_file: File,
        file_indexies: Vec<usize>,
    ) -> Result<HashMap<String, CurrentWeather>, Error> {
        let mut archive = ZipArchive::new(zip_file)?;
        let mut current_weather: HashMap<String, CurrentWeather> = HashMap::new();
        for file_index in file_indexies {
            let mut entry = archive.by_index(file_index)?;
            let mut content = String::new();
            entry.read_to_string(&mut content)?;
            let converted_xml: CurrentObservation = match serde_xml_rs::from_str(&content) {
                Ok(val) => val,
                Err(e) => {
                    error!(self.logger, "error converting the raw content: {}", e);
                    CurrentObservation::default()
                }
            };
            debug!(self.logger.clone(), "converted xml: {:?}", converted_xml);
            if converted_xml != CurrentObservation::default() {
                current_weather.insert(converted_xml.station_id.clone(), converted_xml.try_into()?);
            }
        }
        Ok(current_weather)
    }
}
fn find_file_indexes_for_stations(
    zip_file: File,
    station_ids: HashSet<String>,
) -> Result<Vec<usize>, Error> {
    let mut matching_entries = Vec::new();
    let mut archive = ZipArchive::new(zip_file)?;
    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        let entry_name = entry.name().split('.').next().unwrap();
        if station_ids.contains(entry_name) {
            matching_entries.push(i);
        }
    }

    Ok(matching_entries)
}
