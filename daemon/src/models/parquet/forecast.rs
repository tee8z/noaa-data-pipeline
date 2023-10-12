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
        Type::primitive_type_builder("observation_latitude", PhysicalType::DOUBLE)
            .with_converted_type(ConvertedType::INT_64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let observation_longitude =
        Type::primitive_type_builder("observation_longitude", PhysicalType::DOUBLE)
            .with_converted_type(ConvertedType::INT_64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let updated = Type::primitive_type_builder("updated", PhysicalType::BYTE_ARRAY)
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

    let wind_speed_unit_code =
        Type::primitive_type_builder("wind_speed_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_speed_value = Type::primitive_type_builder("wind_speed_value", PhysicalType::INT64)
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

    let probability_of_precipitation_unit_code = Type::primitive_type_builder(
        "probability_of_precipitation_unit_code",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let probability_of_precipitation_value =
        Type::primitive_type_builder("probability_of_precipitation_value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let dewpoint_unit_code =
        Type::primitive_type_builder("dewpoint_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let dewpoint_value = Type::primitive_type_builder("dewpoint_value", PhysicalType::DOUBLE)
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
        Type::primitive_type_builder("relative_humidity_value", PhysicalType::INT64)
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
        Type::primitive_type_builder("wind_direction_value", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let schema = Type::group_type_builder("forecast")
        .with_fields(vec![
            Arc::new(zone_id),
            Arc::new(forecast_office_id),
            Arc::new(observation_station_id),
            Arc::new(observation_latitude),
            Arc::new(observation_longitude),
            Arc::new(updated),
            Arc::new(generated_at),
            Arc::new(day),
            Arc::new(start_time),
            Arc::new(end_time),
            Arc::new(wind_speed_unit_code),
            Arc::new(wind_speed_value),
            Arc::new(temperature_unit_code),
            Arc::new(temperature_value),
            Arc::new(probability_of_precipitation_unit_code),
            Arc::new(probability_of_precipitation_value),
            Arc::new(dewpoint_unit_code),
            Arc::new(dewpoint_value),
            Arc::new(relative_humidity_unit_code),
            Arc::new(relative_humidity_value),
            Arc::new(wind_direction_unit_code),
            Arc::new(wind_direction_value),
        ])
        .build()
        .unwrap();

    schema
}

#[derive(ParquetRecordWriter)]
pub struct Forecast {
    pub zone_id: String,
    pub forecast_office_id: String,
    pub observation_station_id: String,
    pub observation_latitude: f64,
    pub observation_longitude: f64,
    pub updated: Option<String>,
    pub generated_at: Option<String>,
    pub day: i64,
    pub start_time: String,
    pub end_time: String,
    pub wind_speed_unit_code: Option<String>,
    pub wind_speed_value: Option<i64>,
    pub temperature_unit_code: Option<String>,
    pub temperature_value: Option<i64>,
    pub probability_of_precipitation_unit_code: Option<String>,
    pub probability_of_precipitation_value: Option<i64>,
    pub dewpoint_unit_code: Option<String>,
    pub dewpoint_value: Option<f64>,
    pub relative_humidity_unit_code: Option<String>,
    pub relative_humidity_value: Option<i64>,
    pub wind_direction_unit_code: Option<String>,
    pub wind_direction_value: Option<f64>,
}

impl From<&Mapping> for Vec<Forecast> {
    fn from(value: &Mapping) -> Self {
        let forecasts = value
            .forecast_values
            .periods
            .iter()
            .map(|val| {
                let wind_speed_unit_code: Option<String>;
                let wind_speed_value: Option<i64>;
                if let Some(speed) = val.wind_speed.clone() {
                    let items: Vec<&str> = speed.split(" ").collect();
                    wind_speed_unit_code = Some(items[1].to_string());
                    wind_speed_value = Some(items[0].to_string().parse::<i64>().unwrap());
                } else {
                    wind_speed_unit_code = None;
                    wind_speed_value = None;
                }

                let wind_direction_unit_code: Option<String>;
                let wind_direction_value: Option<f64>;
                if let Some(direction) = val.wind_direction.clone() {
                    wind_direction_unit_code = Some(String::from("wmoUnit:degree_(angle)"));
                    wind_direction_value = wind_direction_to_angle(&direction);
                } else {
                    wind_direction_unit_code = None;
                    wind_direction_value = None;
                }
                let raw_coordinates = value.raw_coordinates.clone();
                let lat = *raw_coordinates.first().unwrap();
                let long = *raw_coordinates.last().unwrap();
                Forecast {
                    zone_id: value.zone_id.clone(),
                    forecast_office_id: value.forecast_office_id.clone(),
                    observation_station_id: value.observation_station_id.clone(),
                    observation_latitude: lat,
                    observation_longitude: long,
                    updated: Some(value.forecast_values.updated.clone()),
                    generated_at: Some(value.forecast_values.generated_at.clone()),
                    day: val.number,
                    start_time: val.start_time.clone(),
                    end_time: val.end_time.clone(),
                    wind_speed_unit_code,
                    wind_speed_value,
                    temperature_unit_code: Some(val.temperature_unit.clone()),
                    temperature_value: Some(val.temperature),
                    probability_of_precipitation_unit_code: Some(
                        val.probability_of_precipitation.unit_code.clone(),
                    ),
                    probability_of_precipitation_value: val
                        .probability_of_precipitation
                        .value
                        .clone(),
                    dewpoint_unit_code: Some(val.dewpoint.unit_code.clone()),
                    dewpoint_value: val.dewpoint.value,
                    relative_humidity_unit_code: Some(val.relative_humidity.unit_code.clone()),
                    relative_humidity_value: val.relative_humidity.value,
                    wind_direction_unit_code,
                    wind_direction_value,
                }
            })
            .collect();

        forecasts
    }
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
