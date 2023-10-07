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
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let observation_longitude =
        Type::primitive_type_builder("observation_longitude", PhysicalType::INT64)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let observation_timestamp =
        Type::primitive_type_builder("observation_timestamp", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let elev_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let elev_val = Type::primitive_type_builder("value", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let elevation = Type::group_type_builder("elevation")
        .with_fields(vec![Arc::new(elev_unit_code), Arc::new(elev_val)])
        .build()
        .unwrap();

    let temp_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let temp_val = Type::primitive_type_builder("value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let temp_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let temperature = Type::group_type_builder("temperature")
        .with_fields(vec![
            Arc::new(temp_unit_code),
            Arc::new(temp_val),
            Arc::new(temp_quality_control),
        ])
        .build()
        .unwrap();

    let dew_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let dew_val = Type::primitive_type_builder("value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let dew_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let dewpoint = Type::group_type_builder("dewpoint")
        .with_fields(vec![
            Arc::new(dew_unit_code),
            Arc::new(dew_val),
            Arc::new(dew_quality_control),
        ])
        .build()
        .unwrap();

    let wind_direction_unit_code =
        Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_direction_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_direction_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_direction = Type::group_type_builder("wind_direction")
        .with_fields(vec![
            Arc::new(wind_direction_unit_code),
            Arc::new(wind_direction_val),
            Arc::new(wind_direction_quality_control),
        ])
        .build()
        .unwrap();

    let wind_speed_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_speed = Type::group_type_builder("wind_speed")
        .with_fields(vec![
            Arc::new(wind_speed_unit_code),
            Arc::new(wind_speed_val),
            Arc::new(wind_speed_quality_control),
        ])
        .build()
        .unwrap();

    let wind_gust_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_gust_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_gust_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_gust = Type::group_type_builder("wind_gust")
        .with_fields(vec![
            Arc::new(wind_gust_unit_code),
            Arc::new(wind_gust_val),
            Arc::new(wind_gust_quality_control),
        ])
        .build()
        .unwrap();

    let barometric_pressure_unit_code =
        Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let barometric_pressure_val = Type::primitive_type_builder("value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let barometric_pressure_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let barometric_pressure = Type::group_type_builder("barometric_pressure")
        .with_fields(vec![
            Arc::new(barometric_pressure_unit_code),
            Arc::new(barometric_pressure_val),
            Arc::new(barometric_pressure_quality_control),
        ])
        .build()
        .unwrap();

    let sea_level_pressure_unit_code =
        Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let sea_level_pressure_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let sea_level_pressure_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let sea_level_pressure = Type::group_type_builder("sea_level_pressure")
        .with_fields(vec![
            Arc::new(sea_level_pressure_unit_code),
            Arc::new(sea_level_pressure_val),
            Arc::new(sea_level_pressure_quality_control),
        ])
        .build()
        .unwrap();

    let visibility_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let visibility_val = Type::primitive_type_builder("value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let visibility_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let visibility = Type::group_type_builder("visibility")
        .with_fields(vec![
            Arc::new(visibility_unit_code),
            Arc::new(visibility_val),
            Arc::new(visibility_quality_control),
        ])
        .build()
        .unwrap();

    let precip_last_hour_unit_code =
        Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let precip_last_hour_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let precip_last_hour_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let precip_last_hour = Type::group_type_builder("precipitation_last_hour")
        .with_fields(vec![
            Arc::new(precip_last_hour_unit_code),
            Arc::new(precip_last_hour_val),
            Arc::new(precip_last_hour_quality_control),
        ])
        .build()
        .unwrap();

    let relative_humidity_unit_code =
        Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let relative_humidity_quality_control =
        Type::primitive_type_builder("quality_control", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity = Type::group_type_builder("precipitation_last_hour")
        .with_fields(vec![
            Arc::new(relative_humidity_unit_code),
            Arc::new(relative_humidity_val),
            Arc::new(relative_humidity_quality_control),
        ])
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
            Arc::new(elevation),
            Arc::new(temperature),
            Arc::new(dewpoint),
            Arc::new(wind_direction),
            Arc::new(wind_speed),
            Arc::new(wind_gust),
            Arc::new(barometric_pressure),
            Arc::new(sea_level_pressure),
            Arc::new(visibility),
            Arc::new(precip_last_hour),
            Arc::new(relative_humidity),
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
    pub elevation: Option<Elevation>,
    pub temperature: Option<Temperature>,
    pub dewpoint: Option<Dewpoint>,
    pub wind_direction: Option<WindDirection>,
    pub wind_speed: Option<WindSpeed>,
    pub wind_gust: Option<WindGust>,
    pub barometric_pressure: Option<BarometricPressure>,
    pub sea_level_pressure: Option<SeaLevelPressure>,
    pub visibility: Option<Visibility>,
    pub precipitation_last_hour: Option<PrecipitationLastHour>,
    pub relative_humidity: Option<RelativeHumidity>,
}

impl From<Mapping> for Observation {
    fn from(value: Mapping) -> Self {
        Self {
            zone_id: value.zone_id,
            forecast_office_id: value.forecast_office_id,
            observation_station_id: value.observation_station_id,
            observation_latitude: value.observation_latitude as i64,
            observation_longitude: value.observation_longitude as i64,
            observation_timestamp: value.observation_values.timestamp,
            elevation: Some(Elevation {
                unit_code: value.observation_values.elevation.unit_code,
                value: value.observation_values.elevation.value,
            }),
            temperature: Some(Temperature {
                unit_code: value.observation_values.temperature.unit_code,
                value: value.observation_values.temperature.value,
                quality_control: value.observation_values.temperature.quality_control,
            }),
            dewpoint: Some(Dewpoint {
                unit_code: value.observation_values.dewpoint.unit_code,
                value: value.observation_values.dewpoint.value,
                quality_control: value.observation_values.dewpoint.quality_control,
            }),
            wind_direction: Some(WindDirection {
                unit_code: value.observation_values.wind_direction.unit_code,
                value: value.observation_values.wind_direction.value,
                quality_control: value.observation_values.wind_direction.quality_control,
            }),
            wind_speed: Some(WindSpeed {
                unit_code: value.observation_values.wind_speed.unit_code,
                value: value.observation_values.wind_speed.value,
                quality_control: value.observation_values.wind_speed.quality_control,
            }),
            wind_gust: Some(WindGust {
                unit_code: value.observation_values.wind_gust.unit_code,
                value: value.observation_values.wind_gust.value,
                quality_control: value.observation_values.wind_gust.quality_control,
            }),
            barometric_pressure: Some(BarometricPressure {
                unit_code: value.observation_values.barometric_pressure.unit_code,
                value: value.observation_values.barometric_pressure.value,
                quality_control: value.observation_values.barometric_pressure.quality_control,
            }),
            sea_level_pressure: Some(SeaLevelPressure {
                unit_code: value.observation_values.sea_level_pressure.unit_code,
                value: value.observation_values.sea_level_pressure.value,
                quality_control: value.observation_values.sea_level_pressure.quality_control,
            }),
            visibility: Some(Visibility {
                unit_code: value.observation_values.visibility.unit_code,
                value: value.observation_values.visibility.value,
                quality_control: value.observation_values.visibility.quality_control,
            }),
            precipitation_last_hour: Some(PrecipitationLastHour {
                unit_code: value.observation_values.precipitation_last_hour.unit_code,
                value: value.observation_values.precipitation_last_hour.value,
                quality_control: value
                    .observation_values
                    .precipitation_last_hour
                    .quality_control,
            }),
            relative_humidity: Some(RelativeHumidity {
                unit_code: value.observation_values.relative_humidity.unit_code,
                value: value.observation_values.relative_humidity.value,
                quality_control: value.observation_values.relative_humidity.quality_control,
            }),
        }
    }
}

#[derive(Debug, ParquetRecordWriter)]
pub struct Elevation {
    pub unit_code: String,
    pub value: Option<i64>,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct Temperature {
    pub unit_code: String,
    pub value: Option<i64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct Dewpoint {
    pub unit_code: String,
    pub value: Option<i64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct WindDirection {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct WindSpeed {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct WindGust {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct BarometricPressure {
    pub unit_code: String,
    pub value: Option<i64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct SeaLevelPressure {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct Visibility {
    pub unit_code: String,
    pub value: Option<i64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct PrecipitationLastHour {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct RelativeHumidity {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}
