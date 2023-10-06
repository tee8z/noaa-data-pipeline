use std::sync::Arc;

use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::types::Type,
};

fn create_mapping_schema() -> Type {
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
        Type::primitive_type_builder("observation_latitude", PhysicalType::INT64)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let forecast_values = Type::group_type_builder("forecast_values");

    let observation_values = Type::group_type_builder("observation_values");

    let schema = Type::group_type_builder("mapping")
        .with_fields(vec![
            Arc::new(zone_id),
            Arc::new(forecast_office_id),
            Arc::new(observation_station_id),
            Arc::new(observation_latitude),
            Arc::new(observation_longitude),
        ])
        .build()
        .unwrap();
    schema
}
