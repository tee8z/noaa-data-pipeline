use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::types::Type,
};
use parquet_derive::ParquetRecordWriter;
use std::sync::Arc;

use crate::Mapping;

pub fn create_forecast_schema() -> Type {
    let zone_id = Type::primitive_type_builder("zone_id", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let forecast_office_id =
        Type::primitive_type_builder("forecast_office_id", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let observation_station_id =
        Type::primitive_type_builder("observation_station_id", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let observation_latitude =
        Type::primitive_type_builder("observation_latitude", PhysicalType::INT64)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let observation_longitude =
        Type::primitive_type_builder("observation_longitude", PhysicalType::INT64)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
    let update = Type::primitive_type_builder("updated", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let generated_at = Type::primitive_type_builder("generated_at", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let day = Type::primitive_type_builder("day", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let start_time = Type::primitive_type_builder("start_time", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let end_time = Type::primitive_type_builder("end_time", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_val = Type::primitive_type_builder("value", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed = Type::group_type_builder("wind_speed")
        .with_fields(vec![
            Arc::new(wind_speed_unit_code),
            Arc::new(wind_speed_val),
        ])
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

    let temperature = Type::group_type_builder("temperature")
        .with_fields(vec![Arc::new(temp_unit_code), Arc::new(temp_val)])
        .build()
        .unwrap();

    let probability_of_precipitation_unit_code =
        Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let probability_of_precipitation_val =
        Type::primitive_type_builder("value", PhysicalType::FLOAT)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let probability_of_precipitation = Type::group_type_builder("probability_of_precipitation")
        .with_fields(vec![
            Arc::new(probability_of_precipitation_unit_code),
            Arc::new(probability_of_precipitation_val),
        ])
        .build()
        .unwrap();

    let dew_unit_code = Type::primitive_type_builder("unit_code", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let dew_val = Type::primitive_type_builder("value", PhysicalType::FLOAT)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let dewpoint = Type::group_type_builder("dewpoint")
        .with_fields(vec![Arc::new(dew_unit_code), Arc::new(dew_val)])
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

    let relative_humidity = Type::group_type_builder("precipitation_last_hour")
        .with_fields(vec![
            Arc::new(relative_humidity_unit_code),
            Arc::new(relative_humidity_val),
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

    let wind_direction = Type::group_type_builder("wind_direction")
        .with_fields(vec![
            Arc::new(wind_direction_unit_code),
            Arc::new(wind_direction_val),
        ])
        .build()
        .unwrap();
    let forecast_14_day = Type::group_type_builder("forecast_14_day")
        .with_repetition(Repetition::REPEATED)
        .with_fields(vec![
            Arc::new(day),
            Arc::new(start_time),
            Arc::new(end_time),
            Arc::new(wind_speed),
            Arc::new(temperature),
            Arc::new(probability_of_precipitation),
            Arc::new(dewpoint),
            Arc::new(relative_humidity),
            Arc::new(wind_direction),
        ])
        .build()
        .unwrap();

    let schema = Type::group_type_builder("forecast")
        .with_fields(vec![
            Arc::new(zone_id),
            Arc::new(forecast_office_id),
            Arc::new(observation_station_id),
            Arc::new(observation_latitude),
            Arc::new(observation_longitude),
            Arc::new(update),
            Arc::new(generated_at),
            Arc::new(forecast_14_day),
        ])
        .build()
        .unwrap();
    schema
}

#[derive(ParquetRecordWriter)]
pub struct Forecast14Day {
    pub day: i64,
    pub start_time: String,
    pub end_time: String,
    pub wind_speed: Option<WindSpeed>,
    pub temperature: Option<Temperature>,
    pub probability_of_precipitation: Option<ProbabilityOfPrecipitation>,
    pub dewpoint: Option<Dewpoint>,
    pub relative_humidity: Option<RelativeHumidity>,
    pub wind_direction: Option<WindDirection>,
}

#[derive(ParquetRecordWriter)]
pub struct Forecast {
    pub zone_id: String,
    pub forecast_office_id: String,
    pub observation_station_id: String,
    pub observation_latitude: i64,
    pub observation_longitude: i64,
    pub updated: Option<String>,
    pub generated_at: Option<String>,
    pub forecast_14_day: Vec<Forecast14Day>,
}

impl From<Mapping> for Forecast {
    fn from(value: Mapping) -> Self {
        let forecast = value
            .forecast_values
            .periods
            .iter()
            .map(|val| {
                let wind_speed = if let Some(speed) = val.wind_speed.clone() {
                    let items: Vec<&str> = speed.split(" ").collect();
                    let val = items[0].to_string().parse::<i64>().unwrap();
                    let unit = items[1].to_string();
                    Some(WindSpeed {
                        value: Some(val),
                        unit_code: unit,
                    })
                } else {
                    None
                };
                let temperature = Temperature {
                    value: Some(val.temperature),
                    unit_code: val.temperature_unit.clone(),
                };
                let wind_direction = if let Some(direction) = val.wind_direction.clone() {
                    Some(WindDirection {
                        unit_code: String::from("wmoUnit:degree_(angle)"),
                        value: wind_direction_to_angle(&direction),
                    })
                } else {
                    None
                };
                Forecast14Day {
                    day: val.number,
                    start_time: val.start_time.clone(),
                    end_time: val.end_time.clone(),
                    wind_speed: wind_speed,
                    temperature: Some(temperature),
                    probability_of_precipitation: Some(ProbabilityOfPrecipitation {
                        unit_code: val.probability_of_precipitation.unit_code.clone(),
                        value: val.probability_of_precipitation.value.clone(),
                    }),
                    dewpoint: Some(Dewpoint {
                        unit_code: val.dewpoint.unit_code.clone(),
                        value: val.dewpoint.value,
                    }),
                    relative_humidity: Some(RelativeHumidity {
                        unit_code: val.relative_humidity.unit_code.clone(),
                        value: val.relative_humidity.value,
                    }),
                    wind_direction: wind_direction,
                }
            })
            .collect();
        Self {
            zone_id: value.zone_id,
            forecast_office_id: value.forecast_office_id,
            observation_station_id: value.observation_station_id,
            observation_latitude: value.observation_latitude as i64,
            observation_longitude: value.observation_longitude as i64,
            updated: Some(value.forecast_values.updated),
            generated_at: Some(value.forecast_values.generated_at),
            forecast_14_day: forecast,
        }
    }
}

#[derive(ParquetRecordWriter)]
pub struct WindSpeed {
    pub unit_code: String,
    pub value: Option<i64>,
}

#[derive(ParquetRecordWriter)]
pub struct ProbabilityOfPrecipitation {
    pub unit_code: String,
    pub value: Option<i64>,
}

#[derive(ParquetRecordWriter)]
pub struct Temperature {
    pub unit_code: String,
    pub value: Option<i64>,
}

#[derive(ParquetRecordWriter)]
pub struct Dewpoint {
    pub unit_code: String,
    pub value: Option<f64>,
}

#[derive(ParquetRecordWriter)]
pub struct RelativeHumidity {
    pub unit_code: String,
    pub value: Option<i64>,
}

#[derive(ParquetRecordWriter)]
pub struct WindDirection {
    pub unit_code: String,
    pub value: Option<f64>,
}

fn wind_direction_to_angle(direction: &str) -> Option<f64> {
    match direction.to_uppercase().as_str() {
        "N" => Some(0.0),
        "NNE" => Some(22.5),
        "NE" => Some(45.0),
        "ENE" => Some(67.5),
        "E" => Some(90.0),
        "ESE" => Some(112.5),
        "SE" => Some(135.0),
        "SSE" => Some(157.5),
        "S" => Some(180.0),
        "SSW" => Some(202.5),
        "SW" => Some(225.0),
        "WSW" => Some(247.5),
        "W" => Some(270.0),
        "WNW" => Some(292.5),
        "NW" => Some(315.0),
        "NNW" => Some(337.5),
        _ => None, // Invalid wind direction
    }
}
