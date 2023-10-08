use std::sync::Arc;

use parquet::{
    basic::{Repetition, Type as PhysicalType},
    schema::types::Type,
};

pub fn create_station_schema() -> Type {
    let station_id = Type::primitive_type_builder("station_id", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let zone_id = Type::primitive_type_builder("zone_id", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let station_name = Type::primitive_type_builder("station_name", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let latitude = Type::primitive_type_builder("latitude", PhysicalType::FLOAT)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let longitude = Type::primitive_type_builder("longitude", PhysicalType::FLOAT)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let schema = Type::group_type_builder("station")
        .with_fields(vec![
            Arc::new(station_id),
            Arc::new(zone_id),
            Arc::new(station_name),
            Arc::new(latitude),
            Arc::new(longitude),
        ])
        .build()
        .unwrap();

    schema
}
