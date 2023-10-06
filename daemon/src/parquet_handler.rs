/*
fn write_parquet_file(stations: Vec<Station>) {
    let writer = ParquetWriter::new("weather_stations.parquet").unwrap();

    let schema = writer
        .schema_builder()
        .column("station_id", Encoding::Plain)
        .column("state", Encoding::Plain)
        .column("station_name", Encoding::Plain)
        .column("latitude", Encoding::Plain)
        .column("longitude", Encoding::Plain)
        .build()
        .unwrap();

    writer.write_schema(&schema).unwrap();

    for station in stations {
        writer.write_row(&station).unwrap();
    }

    writer.flush().unwrap();

    writer.close().unwrap();
}*/

