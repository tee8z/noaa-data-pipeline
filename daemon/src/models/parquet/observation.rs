use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::types::Type,
};
use parquet_derive::ParquetRecordWriter;
use std::sync::Arc;

use crate::Mapping;

pub fn create_observation_schema() -> Type {
    let zone_id = Type::primitive_type_builder("zone_id", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let forecast_office_id =
        Type::primitive_type_builder("forecast_office_id", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let observation_station_id =
        Type::primitive_type_builder("observation_station_id", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let observation_latitude =
        Type::primitive_type_builder("observation_latitude", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let observation_longitude =
        Type::primitive_type_builder("observation_longitude", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let observation_timestamp =
        Type::primitive_type_builder("observation_timestamp", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let elevation_unit_code =
        Type::primitive_type_builder("elevation_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let elevation_value = Type::primitive_type_builder("elevation_value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let temperature_unit_code =
        Type::primitive_type_builder("temperature_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let temperature_value = Type::primitive_type_builder("temperature_value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let temperature_quality_control =
        Type::primitive_type_builder("temperature_quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let dewpoint_unit_code =
        Type::primitive_type_builder("dewpoint_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let dewpoint_value = Type::primitive_type_builder("dewpoint_value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let dewpoint_quality_control =
        Type::primitive_type_builder("dewpoint_quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_direction_unit_code =
        Type::primitive_type_builder("wind_direction_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_direction_value =
        Type::primitive_type_builder("wind_direction_value", PhysicalType::FLOAT)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_direction_quality_control =
        Type::primitive_type_builder("wind_direction_quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_speed_unit_code =
        Type::primitive_type_builder("wind_speed_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_speed_value = Type::primitive_type_builder("wind_speed_value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_quality_control =
        Type::primitive_type_builder("wind_speed_quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_gust_unit_code =
        Type::primitive_type_builder("wind_gust_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_gust_value = Type::primitive_type_builder("wind_gust_value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_gust_quality_control =
        Type::primitive_type_builder("wind_gust_quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let barometric_pressure_unit_code =
        Type::primitive_type_builder("barometric_pressure_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let barometric_pressure_value =
        Type::primitive_type_builder("barometric_pressure_value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let barometric_pressure_quality_control = Type::primitive_type_builder(
        "barometric_pressure_quality_control",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let sea_level_pressure_unit_code =
        Type::primitive_type_builder("sea_level_pressure_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let sea_level_pressure_value =
        Type::primitive_type_builder("sea_level_pressure_value", PhysicalType::FLOAT)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let sea_level_pressure_quality_control = Type::primitive_type_builder(
        "sea_level_pressure_quality_control",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let visibility_unit_code =
        Type::primitive_type_builder("visibility_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let visibility_value = Type::primitive_type_builder("visibility_value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let visibility_quality_control =
        Type::primitive_type_builder("visibility_quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let precip_last_hour_unit_code = Type::primitive_type_builder(
        "precipitation_last_hour_unit_code",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let precip_last_hour_value =
        Type::primitive_type_builder("precipitation_last_hour_value", PhysicalType::FLOAT)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let precip_last_hour_quality_control = Type::primitive_type_builder(
        "precipitation_last_hour_quality_control",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let relative_humidity_unit_code =
        Type::primitive_type_builder("relative_humidity_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity_value =
        Type::primitive_type_builder("relative_humidity_value", PhysicalType::FLOAT)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity_quality_control = Type::primitive_type_builder(
        "relative_humidity_quality_control",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let schema = Type::group_type_builder("observation")
        .with_fields(vec![
            Arc::new(zone_id),
            Arc::new(forecast_office_id),
            Arc::new(observation_station_id),
            Arc::new(observation_latitude),
            Arc::new(observation_longitude),
            Arc::new(observation_timestamp),
            Arc::new(elevation_unit_code),
            Arc::new(elevation_value),
            Arc::new(temperature_unit_code),
            Arc::new(temperature_value),
            Arc::new(temperature_quality_control),
            Arc::new(dewpoint_unit_code),
            Arc::new(dewpoint_value),
            Arc::new(dewpoint_quality_control),
            Arc::new(wind_direction_unit_code),
            Arc::new(wind_direction_value),
            Arc::new(wind_direction_quality_control),
            Arc::new(wind_speed_unit_code),
            Arc::new(wind_speed_value),
            Arc::new(wind_speed_quality_control),
            Arc::new(wind_gust_unit_code),
            Arc::new(wind_gust_value),
            Arc::new(wind_gust_quality_control),
            Arc::new(barometric_pressure_unit_code),
            Arc::new(barometric_pressure_value),
            Arc::new(barometric_pressure_quality_control),
            Arc::new(sea_level_pressure_unit_code),
            Arc::new(sea_level_pressure_value),
            Arc::new(sea_level_pressure_quality_control),
            Arc::new(visibility_unit_code),
            Arc::new(visibility_value),
            Arc::new(visibility_quality_control),
            Arc::new(precip_last_hour_unit_code),
            Arc::new(precip_last_hour_value),
            Arc::new(precip_last_hour_quality_control),
            Arc::new(relative_humidity_unit_code),
            Arc::new(relative_humidity_value),
            Arc::new(relative_humidity_quality_control),
        ])
        .build()
        .unwrap();

    schema
}

#[derive(Debug, ParquetRecordWriter)]
pub struct Observation {
    pub zone_id: String,
    pub forecast_office_id: String,
    pub observation_station_id: String,
    pub observation_latitude: i64,
    pub observation_longitude: i64,
    pub observation_timestamp: String,
    pub elevation_unit_code: Option<String>,
    pub elevation_value: Option<i64>,
    pub temperature_unit_code: Option<String>,
    pub temperature_value: Option<i64>,
    pub temperature_quality_control: Option<String>,
    pub dewpoint_unit_code: Option<String>,
    pub dewpoint_value: Option<i64>,
    pub dewpoint_quality_control: Option<String>,
    pub wind_direction_unit_code: Option<String>,
    pub wind_direction_value: Option<f64>,
    pub wind_direction_quality_control: Option<String>,
    pub wind_speed_unit_code: Option<String>,
    pub wind_speed_value: Option<f64>,
    pub wind_speed_quality_control: Option<String>,
    pub wind_gust_unit_code: Option<String>,
    pub wind_gust_value: Option<f64>,
    pub wind_gust_quality_control: Option<String>,
    pub barometric_pressure_unit_code: Option<String>,
    pub barometric_pressure_value: Option<i64>,
    pub barometric_pressure_quality_control: Option<String>,
    pub sea_level_pressure_unit_code: Option<String>,
    pub sea_level_pressure_value: Option<f64>,
    pub sea_level_pressure_quality_control: Option<String>,
    pub visibility_unit_code: Option<String>,
    pub visibility_value: Option<i64>,
    pub visibility_quality_control: Option<String>,
    pub precipitation_last_hour_unit_code: Option<String>,
    pub precipitation_last_hour_value: Option<f64>,
    pub precipitation_last_hour_quality_control: Option<String>,
    pub relative_humidity_unit_code: Option<String>,
    pub relative_humidity_value: Option<f64>,
    pub relative_humidity_quality_control: Option<String>,
}

impl From<&Mapping> for Observation {
    fn from(value: &Mapping) -> Self {
        Self {
            zone_id: value.zone_id.to_string(),
            forecast_office_id: value.forecast_office_id.to_string(),
            observation_station_id: value.observation_station_id.to_string(),
            observation_latitude: value.observation_latitude as i64,
            observation_longitude: value.observation_longitude as i64,
            observation_timestamp: value.observation_values.timestamp.to_string(),
            elevation_unit_code: Some(value.observation_values.elevation.unit_code.to_string()),
            elevation_value: value.observation_values.elevation.value,
            temperature_unit_code: Some(value.observation_values.temperature.unit_code.to_string()),
            temperature_value: value.observation_values.temperature.value,
            temperature_quality_control: Some(
                value
                    .observation_values
                    .temperature
                    .quality_control
                    .to_string(),
            ),
            dewpoint_unit_code: Some(value.observation_values.dewpoint.unit_code.to_string()),
            dewpoint_value: value.observation_values.dewpoint.value,
            dewpoint_quality_control: Some(
                value
                    .observation_values
                    .dewpoint
                    .quality_control
                    .to_string(),
            ),
            wind_direction_unit_code: Some(
                value
                    .observation_values
                    .wind_direction
                    .unit_code
                    .to_string(),
            ),
            wind_direction_value: value.observation_values.wind_direction.value,
            wind_direction_quality_control: Some(
                value
                    .observation_values
                    .wind_direction
                    .quality_control
                    .to_string(),
            ),
            wind_speed_unit_code: Some(value.observation_values.wind_speed.unit_code.to_string()),
            wind_speed_value: value.observation_values.wind_speed.value,
            wind_speed_quality_control: Some(
                value
                    .observation_values
                    .wind_speed
                    .quality_control
                    .to_string(),
            ),
            wind_gust_unit_code: Some(value.observation_values.wind_gust.unit_code.to_string()),
            wind_gust_value: value.observation_values.wind_gust.value,
            wind_gust_quality_control: Some(
                value
                    .observation_values
                    .wind_gust
                    .quality_control
                    .to_string(),
            ),
            barometric_pressure_unit_code: Some(
                value
                    .observation_values
                    .barometric_pressure
                    .unit_code
                    .to_string(),
            ),
            barometric_pressure_value: value.observation_values.barometric_pressure.value,
            barometric_pressure_quality_control: Some(
                value
                    .observation_values
                    .barometric_pressure
                    .quality_control
                    .to_string(),
            ),
            sea_level_pressure_unit_code: Some(
                value
                    .observation_values
                    .sea_level_pressure
                    .unit_code
                    .to_string(),
            ),
            sea_level_pressure_value: value.observation_values.sea_level_pressure.value,
            sea_level_pressure_quality_control: Some(
                value
                    .observation_values
                    .sea_level_pressure
                    .quality_control
                    .to_string(),
            ),
            visibility_unit_code: Some(value.observation_values.visibility.unit_code.to_string()),
            visibility_value: value.observation_values.visibility.value,
            visibility_quality_control: Some(
                value
                    .observation_values
                    .visibility
                    .quality_control
                    .to_string(),
            ),
            precipitation_last_hour_unit_code: Some(
                value
                    .observation_values
                    .precipitation_last_hour
                    .unit_code
                    .to_string(),
            ),
            precipitation_last_hour_value: value.observation_values.precipitation_last_hour.value,
            precipitation_last_hour_quality_control: Some(
                value
                    .observation_values
                    .precipitation_last_hour
                    .quality_control
                    .to_string(),
            ),
            relative_humidity_unit_code: Some(
                value
                    .observation_values
                    .relative_humidity
                    .unit_code
                    .to_string(),
            ),
            relative_humidity_value: value.observation_values.relative_humidity.value,
            relative_humidity_quality_control: Some(
                value
                    .observation_values
                    .relative_humidity
                    .quality_control
                    .to_string(),
            ),
        }
    }
}
