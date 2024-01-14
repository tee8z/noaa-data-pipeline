use std::fmt::{Display, self};

use crate::TimeRange;
use anyhow::{anyhow, Error};
use serde::{Deserialize, Serialize};
use time::{macros::format_description, OffsetDateTime};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename = "dwml")]
pub struct Dwml {
    #[serde(rename = "head")]
    pub head: Head,

    #[serde(rename = "data")]
    pub data: Data,

    #[serde(rename = "version")]
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Data {
    #[serde(rename = "location")]
    pub location: Vec<Location>,

    #[serde(rename = "moreWeatherInformation")]
    pub more_weather_information: Vec<MoreWeatherInformation>,

    #[serde(rename = "time-layout")]
    pub time_layout: Vec<TimeLayout>,

    #[serde(rename = "parameters")]
    pub parameters: Vec<Parameter>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Location {
    #[serde(rename = "location-key")]
    pub location_key: String,

    #[serde(rename = "point")]
    pub point: Point,

    // This is added after parsing to add in mapping the data further down the pipeline
    pub station_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Point {
    #[serde(rename = "latitude")]
    pub latitude: String,

    #[serde(rename = "longitude")]
    pub longitude: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MoreWeatherInformation {
    #[serde(rename = "applicable-location")]
    pub applicable_location: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Parameter {
    #[serde(rename = "temperature")]
    //holds max and min
    pub temperature: Vec<DataReading>,

    #[serde(rename = "precipitation")]
    pub precipitation: DataReading,

    #[serde(rename = "wind-speed")]
    pub wind_speed: DataReading,

    #[serde(rename = "direction")]
    pub wind_direction: DataReading,

    #[serde(rename = "probability-of-precipitation")]
    pub probability_of_precipitation: DataReading,

    #[serde(rename = "humidity")]
    // holds max and min
    pub humidity: Vec<DataReading>,

    #[serde(rename = "applicable-location")]
    pub applicable_location: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DataReading {
    #[serde(rename = "name")]
    pub name: Name,

    #[serde(rename = "value")]
    pub value: Vec<String>,

    #[serde(rename = "type")]
    pub reading_type: Type,

    #[serde(rename = "units")]
    pub units: Units,

    #[serde(rename = "time-layout")]
    pub time_layout: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct TimeLayout {
    #[serde(rename = "time-coordinate")]
    pub time_coordinate: String,
    pub summarization: Option<String>,
    #[serde(rename = "$value")]
    pub time: Vec<Time>,
}
impl TimeLayout {
    pub fn to_time_ranges(&self) -> Result<Vec<TimeRange>, Error> {
        let mut result = Vec::new();
        let mut current_key = String::new();
        let mut current_start_time: Option<OffsetDateTime> = None;
        let description = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:2][offset_hour]:[offset_minute]");
        for item in &self.time {
            match item {
                Time::LayoutKey(key) => {
                    if !current_key.is_empty() {
                        // If a new key is encountered, add the current TimeRange to the result
                        if let Some(start_time) = current_start_time {
                            let time_range = TimeRange {
                                key: current_key.clone(),
                                start_time,
                                end_time: None,
                            };
                            result.push(time_range);
                        }
                    }
                    current_key = key.clone();
                    current_start_time = None;
                }
                Time::StartTime(start_time) => {
                    let current_time = OffsetDateTime::parse(start_time, description)
                        .map_err(|e| anyhow!("error parsing time {}", e))?;
                    current_start_time = Some(current_time);
                }
                Time::EndTime(end_time) => {
                    if let Some(start_time) = current_start_time.take() {
                        let current_time = OffsetDateTime::parse(end_time, description)
                            .map_err(|e| anyhow!("error parsing time {}", e))?;
                        let time_range = TimeRange {
                            key: current_key.clone(),
                            start_time,
                            end_time: Some(current_time),
                        };
                        result.push(time_range);
                    }
                }
            }
        }

        // If the last key doesn't have an end time, treat it as an ongoing time range
        if let Some(start_time) = current_start_time {
            let time_range = TimeRange {
                key: current_key,
                start_time,
                end_time: None,
            };
            result.push(time_range);
        }

        Ok(result)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum Time {
    #[serde(rename = "layout-key")]
    LayoutKey(String),
    #[serde(rename = "start-valid-time")]
    StartTime(String),
    #[serde(rename = "end-valid-time")]
    EndTime(String),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Head {
    #[serde(rename = "product")]
    pub product: Product,

    #[serde(rename = "source")]
    pub source: Source,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Product {
    #[serde(rename = "title")]
    pub title: String,

    #[serde(rename = "field")]
    pub field: String,

    #[serde(rename = "category")]
    pub category: String,

    #[serde(rename = "creation-date")]
    pub creation_date: String,

    #[serde(rename = "srsName")]
    pub srs_name: String,

    #[serde(rename = "concise-name")]
    pub concise_name: String,

    #[serde(rename = "operational-mode")]
    pub operational_mode: String,
}

/*
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CreationDate {
    #[serde(rename = "refresh-frequency")]
    pub refresh_frequency: String,
}*/

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Source {
    #[serde(rename = "more-information")]
    pub more_information: String,

    #[serde(rename = "production-center")]
    pub production_center: ProductionCenter,

    #[serde(rename = "disclaimer")]
    pub disclaimer: String,

    #[serde(rename = "credit")]
    pub credit: String,

    #[serde(rename = "credit-logo")]
    pub credit_logo: String,

    #[serde(rename = "feedback")]
    pub feedback: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ProductionCenter {
    #[serde(rename = "sub-center")]
    pub sub_center: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Type {
    #[serde(rename = "liquid")]
    Liquid,

    #[serde(rename = "maximum")]
    Maximum,

    #[serde(rename = "maximum relative")]
    MaximumRelative,

    #[serde(rename = "minimum")]
    Minimum,

    #[serde(rename = "minimum relative")]
    MinimumRelative,

    #[serde(rename = "sustained")]
    Sustained,

    #[serde(rename = "12 hour")]
    ProbabilityOfPrecipitationWithin12Hours,

    #[serde(rename = "wind")]
    Wind,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Name {
    #[serde(rename = "Daily Maximum Relative Humidity")]
    DailyMaximumRelativeHumidity,

    #[serde(rename = "Daily Maximum Temperature")]
    DailyMaximumTemperature,

    #[serde(rename = "Daily Minimum Relative Humidity")]
    DailyMinimumRelativeHumidity,

    #[serde(rename = "Daily Minimum Temperature")]
    DailyMinimumTemperature,

    #[serde(rename = "Liquid Precipitation Amount")]
    LiquidPrecipitationAmount,

    #[serde(rename = "12 Hourly Probability of Precipitation")]
    The12HourlyProbabilityOfPrecipitation,

    #[serde(rename = "Wind Direction")]
    WindDirection,

    #[serde(rename = "Wind Speed")]
    WindSpeed,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Units {
    #[serde(rename = "degrees true")]
    DegreesTrue,

    #[serde(rename = "Fahrenheit")]
    Fahrenheit,

    #[serde(rename = "inches")]
    Inches,

    #[serde(rename = "knots")]
    Knots,

    #[serde(rename = "percent")]
    Percent,
}

impl Display for Units {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Units::DegreesTrue => write!(f, "degrees true"),
            Units::Fahrenheit => write!(f, "fahrenheit"),
            Units::Inches => write!(f, "inches"),
            Units::Knots =>write!(f, "knots"),
            Units::Percent =>write!(f, "percent"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Summarization {
    #[serde(rename = "none")]
    None,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum TimeCoordinate {
    #[serde(rename = "local")]
    Local,
}
