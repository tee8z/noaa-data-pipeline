use crate::Type::{
    Liquid, Maximum, MaximumRelative, Minimum, MinimumRelative,
    ProbabilityOfPrecipitationWithin12Hours, Sustained, Wind,
};
use crate::{fetch_xml, CityWeather, DataReading, Dwml, Location, Units};
use anyhow::{anyhow, Error};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::types::Type,
};
use parquet_derive::ParquetRecordWriter;
use slog::{debug, Logger};
use std::sync::Arc;
use std::{collections::HashMap, ops::Add};
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};
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
    pub max_temp: Option<i64>,
    pub min_temp: Option<i64>,
    pub temperature_unit_code: String,
    pub wind_speed: Option<i64>,
    pub wind_speed_unit_code: String,
    pub wind_direction: Option<i64>,
    pub wind_direction_unit_code: String,
    pub relative_humidity_max: Option<i64>,
    pub relative_humidity_min: Option<i64>,
    pub relative_humidity_unit_code: String,
    pub liquid_precipitation_amt: Option<f64>,
    pub liquid_precipitation_unit_code: String,
    pub twelve_hour_probability_of_precipitation: Option<i64>,
    pub twelve_hour_probability_of_precipitation_unit_code: String,
}

#[derive(ParquetRecordWriter, Debug)]
pub struct Forecast {
    pub station_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub generated_at: String,
    pub begin_time: String,
    pub end_time: String,
    pub max_temp: Option<i64>,
    pub min_temp: Option<i64>,
    pub temperature_unit_code: String,
    pub wind_speed: Option<i64>,
    pub wind_speed_unit_code: String,
    pub wind_direction: Option<i64>,
    pub wind_direction_unit_code: String,
    pub relative_humidity_max: Option<i64>,
    pub relative_humidity_min: Option<i64>,
    pub relative_humidity_unit_code: String,
    pub liquid_precipitation_amt: Option<f64>,
    pub liquid_precipitation_unit_code: String,
    pub twelve_hour_probability_of_precipitation: Option<i64>,
    pub twelve_hour_probability_of_precipitation_unit_code: String,
}

impl TryFrom<WeatherForecast> for Forecast {
    type Error = anyhow::Error;
    fn try_from(val: WeatherForecast) -> Result<Self, Self::Error> {
        let parquet = Forecast {
            station_id: val.station_id,
            latitude: val.latitude.parse::<f64>()?,
            longitude: val.longitude.parse::<f64>()?,
            generated_at: val
                .generated_at
                .format(&Rfc3339)
                .map_err(|e| anyhow!("error formatting generated_at time: {}", e))?,
            begin_time: val
                .begin_time
                .format(&Rfc3339)
                .map_err(|e| anyhow!("error formatting begin time: {}", e))?,
            end_time: val
                .end_time
                .format(&Rfc3339)
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
        .with_repetition(Repetition::REQUIRED)
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
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let begin_time = Type::primitive_type_builder("begin_time", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let end_time = Type::primitive_type_builder("end_time", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_repetition(Repetition::REQUIRED)
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
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let wind_speed_value = Type::primitive_type_builder("wind_speed", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_speed_unit_code =
        Type::primitive_type_builder("wind_speed_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();

    let wind_direction_value = Type::primitive_type_builder("wind_direction", PhysicalType::INT64)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();

    let wind_direction_unit_code =
        Type::primitive_type_builder("wind_direction_unit_code", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
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
            .with_repetition(Repetition::REQUIRED)
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
            .with_repetition(Repetition::REQUIRED)
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
    .with_repetition(Repetition::REQUIRED)
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

#[derive(Debug, Clone)]
pub struct TimeDelta {
    pub first_start: OffsetDateTime,
    pub last_end: OffsetDateTime,
    pub delta_between_readings: Duration,
    pub delta_between_start_and_end: Option<Duration>,
    pub key: String,
    pub time_ranges: Vec<TimeRange>,
}

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub key: String,
    pub start_time: OffsetDateTime,
    pub end_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub first_time: OffsetDateTime,
    pub last_time: OffsetDateTime,
    pub time_interval: Duration,
}

impl TryFrom<Dwml> for HashMap<String, Vec<WeatherForecast>> {
    type Error = anyhow::Error;
    fn try_from(raw_data: Dwml) -> Result<Self, Self::Error> {
        println!("trying to convert into flatted weather forecast format");

        let mut time_layouts: HashMap<String, Vec<TimeRange>> = HashMap::new();
        for time_layout in raw_data.data.time_layout {
            let time_range: Vec<TimeRange> = time_layout.to_time_ranges()?;
            time_layouts.insert(time_range.first().unwrap().key.clone(), time_range);
        }

        // The `location-key` is the key for each hashmap entry
        let mut weather: HashMap<String, Vec<WeatherForecast>> = HashMap::new();
        let generated_at = OffsetDateTime::parse(&raw_data.head.product.creation_date, &Rfc3339)?;

        raw_data.data.location.iter().for_each(|location| {
            let weather_forecast =
                get_forecasts_ranges(location, generated_at, time_layouts.clone());
            println!("inserting into weather_data: {:?}", weather_forecast);
            weather.insert(location.location_key.clone(), weather_forecast);
        });

        for parameter_point in raw_data.data.parameters {
            let location_key = parameter_point.applicable_location.clone();
            let weather_data = weather.get_mut(&location_key).unwrap();
            println!("found weather_data: {:?}", weather_data);
            for temp in parameter_point.temperature.clone() {
                // We want this to panic, we should never have a time layout that doesn't exist in the map
                let temp_times = time_layouts.get(&temp.time_layout).unwrap();
                add_data(weather_data, temp_times, &temp)?;
            }

            for humidity in parameter_point.humidity.clone() {
                let humidity_times = time_layouts.get(&humidity.time_layout).unwrap();
                add_data(weather_data, humidity_times, &humidity)?;
            }

            let precipitation_times = time_layouts
                .get(&parameter_point.precipitation.time_layout)
                .unwrap();
            add_data(
                weather_data,
                precipitation_times,
                &parameter_point.precipitation,
            )?;

            let probability_of_precipitation_times = time_layouts
                .get(&parameter_point.probability_of_precipitation.time_layout)
                .unwrap();
            add_data(
                weather_data,
                probability_of_precipitation_times,
                &parameter_point.probability_of_precipitation,
            )?;

            let wind_direction_times = time_layouts
                .get(&parameter_point.wind_direction.time_layout)
                .unwrap();
            add_data(
                weather_data,
                wind_direction_times,
                &parameter_point.wind_direction,
            )?;

            let wind_speed_times = time_layouts
                .get(&parameter_point.wind_speed.time_layout)
                .unwrap();
            add_data(weather_data, wind_speed_times, &parameter_point.wind_speed)?;

            println!("updated weather_data: {:?}", weather_data);
        }
        Ok(weather)
    }
}

// weather_data is always in 3 hour intervals, time_ranges can be in 3,6,12,24 ranges
fn add_data(
    weather_data: &mut [WeatherForecast],
    time_ranges: &[TimeRange],
    data: &DataReading,
) -> Result<(), Error> {
    for current_data in weather_data.iter_mut() {
        let mut time_iter = time_ranges.iter();
        let mut current_time = time_iter.next().unwrap();
        let mut time_interval_index: Option<usize> = None;
        // This is an important time comparison, if there are more None's than expected this may be the source
        while current_time.start_time <= current_data.begin_time {
            time_interval_index = if let Some(mut interval) = time_interval_index {
                interval += 1;
                Some(interval)
            } else {
                Some(0)
            };
            if let Some(next_time) = time_iter.next() {
                current_time = next_time
            } else {
                break;
            }
        }

        match data.reading_type {
            Liquid => {
                if let Some(index) = time_interval_index {
                    current_data.liquid_precipitation_amt =
                        Some((data.value.get(index)).unwrap().parse::<f64>()?);
                }
                current_data.liquid_precipitation_unit_code = data.units.to_string();
            }
            Maximum => {
                if let Some(index) = time_interval_index {
                    current_data.max_temp = Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.temperature_unit_code = data.units.to_string();
            }
            MaximumRelative => {
                if let Some(index) = time_interval_index {
                    current_data.relative_humidity_max =
                        Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.relative_humidity_unit_code = data.units.to_string();
            }
            Minimum => {
                if let Some(index) = time_interval_index {
                    current_data.min_temp = Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.temperature_unit_code = data.units.to_string();
            }
            MinimumRelative => {
                if let Some(index) = time_interval_index {
                    current_data.relative_humidity_min =
                        Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.relative_humidity_unit_code = data.units.to_string();
            }
            Sustained => {
                if let Some(index) = time_interval_index {
                    current_data.wind_speed =
                        Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.wind_speed_unit_code = data.units.to_string();
            }
            ProbabilityOfPrecipitationWithin12Hours => {
                if let Some(index) = time_interval_index {
                    current_data.twelve_hour_probability_of_precipitation =
                        Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.twelve_hour_probability_of_precipitation_unit_code =
                    data.units.to_string();
            }
            Wind => {
                if let Some(index) = time_interval_index {
                    current_data.wind_direction =
                        Some((data.value.get(index)).unwrap().parse::<i64>()?);
                }
                current_data.wind_direction_unit_code = data.units.to_string();
            }
        }
    }
    Ok(())
}

pub async fn get_forecasts(
    logger: &Logger,
    city_weather: &CityWeather,
) -> Result<Vec<Forecast>, Error> {
    let mut forecast_data: HashMap<String, Vec<WeatherForecast>> = HashMap::new();

    //TODO: call 200 stations at a time, max allowed
    let url = get_url(city_weather);
    let raw_xml = fetch_xml(logger, &url).await?;
    debug!(logger.clone(), "raw xml: {}", raw_xml);

    let converted_xml: Dwml = serde_xml_rs::from_str(&raw_xml)?;
    let weather_with_stations = add_station_ids(city_weather, converted_xml);
    debug!(logger.clone(), "converted xml: {:?}", weather_with_stations);

    let current_forecast_data: HashMap<String, Vec<WeatherForecast>> =
        weather_with_stations.try_into()?;

    debug!(
        logger.clone(),
        "current_forecast_data: {:?}", current_forecast_data
    );

    //TODO: add each 200 parsed data into the HashMap (should be keyed on station_id and then a list of each weather reading per day)
    forecast_data.extend(current_forecast_data);

    let mut forecasts = vec![];
    for all_forecasts in forecast_data.values() {
        for weather_forecats in all_forecasts {
            let current = weather_forecats.clone();
            debug!(logger.clone(), "current weather forecast: {:?}", current);
            let forecast: Forecast = current.try_into()?;
            debug!(logger.clone(), "parquet format forecast: {:?}", forecast);
            forecasts.push(forecast)
        }
    }

    Ok(forecasts)
}

fn get_forecasts_ranges(
    location: &Location,
    generated_at: OffsetDateTime,
    time_layouts: HashMap<String, Vec<TimeRange>>,
) -> Vec<WeatherForecast> {
    let mut first_start_time_only: Vec<OffsetDateTime> = time_layouts
        .iter()
        .filter(|(_, time_ranges)| time_ranges.first().unwrap().end_time.is_none())
        .map(|(_, time_ranges)| time_ranges.first().unwrap().start_time)
        .collect();

    first_start_time_only.sort_by(|a, b| a.cmp(&b));
    let first_time = first_start_time_only.first().unwrap().clone();

    let mut last_start_time_only: Vec<OffsetDateTime> = time_layouts
        .iter()
        .filter(|(_, time_ranges)| time_ranges.first().unwrap().end_time.is_none())
        .map(|(_, time_ranges)| time_ranges.last().unwrap().start_time)
        .collect();

    last_start_time_only.sort_by(|a, b| a.cmp(&b));
    let last_time = last_start_time_only.last().unwrap().clone();
    println!("first_time: {:?}", first_time);
    println!("last_time: {:?}", last_time);
    let time_window = TimeWindow {
        first_time,
        last_time,
        time_interval: Duration::hours(3),
    };
    let mut forecasts = Vec::new();
    let mut current_time = time_window.first_time;

    while current_time <= time_window.last_time {
        let end_time = current_time + time_window.time_interval;

        let weather_forecast = WeatherForecast {
            station_id: location.station_id.clone().unwrap_or_default(),
            latitude: location.point.latitude.clone(),
            longitude: location.point.longitude.clone(),
            generated_at,
            begin_time: current_time,
            end_time,
            max_temp: None,
            min_temp: None,
            temperature_unit_code: Units::Fahrenheit.to_string(),
            wind_speed: None,
            wind_speed_unit_code: Units::Knots.to_string(),
            wind_direction: None,
            wind_direction_unit_code: Units::DegreesTrue.to_string(),
            relative_humidity_max: None,
            relative_humidity_min: None,
            relative_humidity_unit_code: Units::Percent.to_string(),
            liquid_precipitation_amt: None,
            liquid_precipitation_unit_code: Units::Inches.to_string(),
            twelve_hour_probability_of_precipitation: None,
            twelve_hour_probability_of_precipitation_unit_code: Units::Percent.to_string(),
        };

        forecasts.push(weather_forecast);

        // Move to the next time interval
        current_time = end_time;
    }

    forecasts
}

fn add_station_ids(city_weather: &CityWeather, mut converted_xml: Dwml) -> Dwml {
    converted_xml.data.location = converted_xml
        .data
        .location
        .iter()
        .map(|location| {
            let latitude = location.point.latitude.clone();
            let longitude = location.point.longitude.clone();

            let station_id = city_weather
                .city_data
                .clone()
                .values()
                // xml files always provide these to 2 decimal places, make sure to match on that percision
                .find(|val| {
                    val.latitude.parse::<f64>().unwrap() == latitude.parse::<f64>().unwrap()
                        && val.longitude.parse::<f64>().unwrap()
                            == longitude.parse::<f64>().unwrap()
                })
                .map(|val| val.station_id.clone());

            Location {
                location_key: location.location_key.clone(),
                point: location.point.clone(),
                station_id,
            }
        })
        .collect();
    converted_xml
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
