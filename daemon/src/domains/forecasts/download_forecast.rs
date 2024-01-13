use crate::{fetch_xml, CityWeather, Dwml, Point};
use anyhow::{anyhow, Error};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::types::Type,
};
use parquet_derive::ParquetRecordWriter;
use slog::{debug, Logger};
use std::sync::Arc;
use std::{collections::HashMap, fmt, ops::Add};
use time::{
    format_description::well_known::Rfc3339, macros::format_description, Duration, OffsetDateTime,
};
/*
More Options defined  here:
TODO: pull list down from the website and request everything
https://graphical.weather.gov/xml/docs/elementInputNames.php

Maximum Temperature 	maxt
Minimum Temperature 	mint
Wind Speed 	wspd
Wind Direction 	wdir
12 Hour Probability of Precipitation 	pop12
Liquid Precipitation Amount 	qpf
Maximum Relative Humidity 	maxrh
Minimum Relative Humidity 	minrh
*/
#[derive(Debug, Clone)]
pub struct WeatherForecast {
    pub station_id: String,
    pub latitude: String,
    pub longitude: String,
    pub generated_at: OffsetDateTime,
    pub begin_time: OffsetDateTime,
    pub end_time: OffsetDateTime,
    pub max_temp: f64,
    pub min_temp: f64,
    pub temperature_unit_code: String,
    pub wind_speed: f64,
    pub wind_speed_unit_code: String,
    pub wind_direction: i64,
    pub wind_direction_unit_code: String,
    pub relative_humidity_max: f64,
    pub relative_humidity_min: f64,
    pub relative_humidity_unit_code: String,
    pub liquid_precipitation_amt: f64,
    pub liquid_precipitation_unit_code: String,
    pub twelve_hour_probability_of_precipitation: f64,
    pub twelve_hour_probability_of_precipitation_unit_code: String,
}

#[derive(ParquetRecordWriter)]
pub struct Forecast {
    pub station_id: String,
    pub latitude: String,
    pub longitude: String,
    pub generated_at: String,
    pub begin_time: String,
    pub end_time: String,
    pub max_temp: f64,
    pub min_temp: f64,
    pub temperature_unit_code: String,
    pub wind_speed: f64,
    pub wind_speed_unit_code: String,
    pub wind_direction: i64,
    pub wind_direction_unit_code: String,
    pub relative_humidity_max: f64,
    pub relative_humidity_min: f64,
    pub relative_humidity_unit_code: String,
    pub liquid_precipitation_amt: f64,
    pub liquid_precipitation_unit_code: String,
    pub twelve_hour_probability_of_precipitation: f64,
    pub twelve_hour_probability_of_precipitation_unit_code: String,
}

impl TryFrom<WeatherForecast> for Forecast {
    type Error = anyhow::Error;
    fn try_from(val: WeatherForecast) -> Result<Self, Self::Error> {
        let rfc_3339_time_description =
            format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
        let parquet = Forecast {
            station_id: val.station_id,
            latitude: val.latitude,
            longitude: val.longitude,
            generated_at: val
                .generated_at
                .format(rfc_3339_time_description)
                .map_err(|e| anyhow!("error formatting generated_at time: {}", e))?,
            begin_time: val
                .begin_time
                .format(rfc_3339_time_description)
                .map_err(|e| anyhow!("error formatting begin time: {}", e))?,
            end_time: val
                .begin_time
                .format(rfc_3339_time_description)
                .map_err(|e| anyhow!("error formatting end time: {}", e))?,
            max_temp: val.max_temp,
            min_temp: val.min_temp,
            temperature_unit_code: val.temperature_unit_code,
            wind_speed: val.wind_speed,
            wind_speed_unit_code: val.wind_speed_unit_code,
            wind_direction: val.wind_direction,
            wind_direction_unit_code: val.wind_direction_unit_code,
            relative_humidity_max: val.relative_humidity_max,
            relative_humidity_min: val.relative_humidity_min,
            relative_humidity_unit_code: val.relative_humidity_unit_code,
            liquid_precipitation_amt: val.liquid_precipitation_amt,
            liquid_precipitation_unit_code: val.liquid_precipitation_unit_code,
            twelve_hour_probability_of_precipitation: val.twelve_hour_probability_of_precipitation,
            twelve_hour_probability_of_precipitation_unit_code: val
                .twelve_hour_probability_of_precipitation_unit_code,
        };
        Ok(parquet)
    }
}

pub fn create_forecast_schema() -> Type {
    let station_id = Type::primitive_type_builder("station_id", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let latitude = Type::primitive_type_builder("latitude", PhysicalType::DOUBLE)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let longitude = Type::primitive_type_builder("longitude", PhysicalType::DOUBLE)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let generated_at = Type::primitive_type_builder("generated_at", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let begin_time = Type::primitive_type_builder("begin_time", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let end_time = Type::primitive_type_builder("end_time", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let max_temp = Type::primitive_type_builder("max_temp", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let min_temp = Type::primitive_type_builder("max_temp", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let temperature_unit_code =
        Type::primitive_type_builder("temperature_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_speed_value = Type::primitive_type_builder("wind_speed", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_unit_code =
        Type::primitive_type_builder("wind_speed_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let wind_direction_value = Type::primitive_type_builder("wind_direction", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_direction_unit_code =
        Type::primitive_type_builder("wind_direction_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity_max =
        Type::primitive_type_builder("relative_humidity_max", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity_min =
        Type::primitive_type_builder("relative_humidity_min", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let relative_humidity_unit_code =
        Type::primitive_type_builder("relative_humidity_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let liquid_precipitation_amt =
        Type::primitive_type_builder("liquid_precipitation_amt", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let liquid_precipitation_unit_code =
        Type::primitive_type_builder("liquid_precipitation_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();

    let twelve_hour_probability_of_precipitation = Type::primitive_type_builder(
        "twelve_hour_probability_of_precipitation",
        PhysicalType::INT64,
    )
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let twelve_hour_probability_of_precipitation_unit_code = Type::primitive_type_builder(
        "twelve_hour_probability_of_precipitation_unit_code",
        PhysicalType::BYTE_ARRAY,
    )
    .with_converted_type(ConvertedType::UTF8)
    .with_repetition(Repetition::OPTIONAL)
    .build()
    .unwrap();

    let schema = Type::group_type_builder("forecast")
        .with_fields(vec![
            Arc::new(station_id),
            Arc::new(latitude),
            Arc::new(longitude),
            Arc::new(generated_at),
            Arc::new(begin_time),
            Arc::new(end_time),
            Arc::new(max_temp),
            Arc::new(min_temp),
            Arc::new(temperature_unit_code),
            Arc::new(wind_speed_value),
            Arc::new(wind_speed_unit_code),
            Arc::new(wind_direction_value),
            Arc::new(wind_direction_unit_code),
            Arc::new(relative_humidity_max),
            Arc::new(relative_humidity_min),
            Arc::new(relative_humidity_unit_code),
            Arc::new(liquid_precipitation_amt),
            Arc::new(liquid_precipitation_unit_code),
            Arc::new(twelve_hour_probability_of_precipitation),
            Arc::new(twelve_hour_probability_of_precipitation_unit_code),
        ])
        .build()
        .unwrap();

    schema
}

pub struct TimeRange {
    pub key: String,
    pub start_time: OffsetDateTime,
    pub end_time: Option<OffsetDateTime>,
}

impl TryFrom<Dwml> for HashMap<String, Vec<WeatherForecast>> {
    type Error = anyhow::Error;
    fn try_from(raw_data: Dwml) -> Result<Self, Self::Error> {
        let mut time_layouts: HashMap<String, Vec<TimeRange>> = HashMap::new();
        for time_layout in raw_data.data.time_layout {
            let time_range: Vec<TimeRange> = time_layout.to_time_ranges()?;
            match time_range.first() {
                None => (),
                Some(first_item) => {
                    time_layouts.insert(first_item.key.clone(), time_range);
                }
            }
        }

        let mut points: HashMap<String, Point> = HashMap::new();
        raw_data.data.location.iter().for_each(|location| {
            points.insert(location.location_key.clone(), location.point.clone());
        });

        let mut weather: HashMap<String, Vec<WeatherForecast>> = HashMap::new();
        raw_data.data.parameters.iter().for_each(|parameter_point| {
            //let point = parameter_point.applicable_location;
            //TODO: THIS IS THE TRICKY MAPPING PART!!
        });

        Ok(weather)
    }
}

impl fmt::Display for WeatherForecast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Station ID: {}, Max Temp: {}, Min Temp: {}",
            self.station_id, self.max_temp, self.min_temp
        )
    }
}

pub async fn get_forecasts(
    logger: &Logger,
    city_weather: &CityWeather,
) -> Result<Vec<Forecast>, Error> {
    let mut forecast_data: HashMap<String, Vec<WeatherForecast>> = HashMap::new();

    //TODO: call 200 stations at a time, max allowed
    let url = get_url(city_weather);
    debug!(logger.clone(), "url: {}", url);
    let raw_xml = fetch_xml(logger, &url).await?;
    debug!(logger.clone(), "raw xml: {}", raw_xml);
    let converted_xml: Dwml = serde_xml_rs::from_str(&raw_xml)?;
    debug!(logger.clone(), "converted xml: {:?}", converted_xml);
    let current_forecast_data: HashMap<String, Vec<WeatherForecast>> = converted_xml.try_into()?;
    //TODO: add each 200 parsed data into the HashMap (should be keyed on station_id and then a list of each weather reading per day)
    forecast_data.extend(current_forecast_data);


    let mut forecasts = vec![];
    for week_forecast in forecast_data.values() {
        for daily_forecast in week_forecast {
            let current = daily_forecast.clone();
            let forecast: Forecast = current.try_into()?;
            forecasts.push(forecast)
        }
    };

    Ok(forecasts)
}

fn get_url(city_weather: &CityWeather) -> String {
    let current_time = OffsetDateTime::now_utc();
    let format_description = Rfc3339;
    let now = current_time.format(&format_description).unwrap();
    // Define the duration of one week (7 days)
    let one_week_duration = Duration::weeks(1);
    let one_week_from_now = current_time.add(one_week_duration);
    let one_week = one_week_from_now.format(&format_description).unwrap();
    format!("https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?listLatLon={}&product=time-series&begin={}&end={}&Unit=e&maxt=maxt&mint=mint&wspd=wspd&wdir=wdir&pop12=pop12&qpf=qpf&maxrh=maxrh&minrh=minrh", city_weather.get_coordinates(),now,one_week)
}
