use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename = "dwml")]
pub struct Dwml {
    #[serde(rename = "head")]
    head: Head,

    #[serde(rename = "data")]
    data: Data,

    #[serde(rename = "version")]
    version: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Data {
    #[serde(rename = "location")]
    location: Vec<Location>,

    #[serde(rename = "moreWeatherInformation")]
    more_weather_information: Vec<MoreWeatherInformation>,

    #[serde(rename = "time-layout")]
    time_layout: Vec<TimeLayout>,

    #[serde(rename = "parameters")]
    parameters: Vec<Parameter>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Location {
    #[serde(rename = "location-key")]
    location_key: String,

    #[serde(rename = "point")]
    point: Point,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Point {
    #[serde(rename = "latitude")]
    latitude: String,

    #[serde(rename = "longitude")]
    longitude: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MoreWeatherInformation {
    #[serde(rename = "applicable-location")]
    applicable_location: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Parameter {
    #[serde(rename = "temperature")]
    temperature: Vec<Direction>,

    #[serde(rename = "precipitation")]
    precipitation: Direction,

    #[serde(rename = "wind-speed")]
    wind_speed: Direction,

    #[serde(rename = "direction")]
    direction: Direction,

    #[serde(rename = "probability-of-precipitation")]
    probability_of_precipitation: Direction,

    #[serde(rename = "humidity")]
    humidity: Vec<Direction>,

    #[serde(rename = "applicable-location")]
    applicable_location: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Direction {
    #[serde(rename = "name")]
    name: Name,

    #[serde(rename = "value")]
    value: Vec<String>,

    #[serde(rename = "type")]
    direction_type: Type,

    #[serde(rename = "units")]
    units: Units,

    #[serde(rename = "time-layout")]
    time_layout: String,
}

#[derive(Debug, Deserialize,Serialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct TimeLayout {
    #[serde(rename = "time-coordinate")]
    pub time_coordinate: String,
    //pub summarization: String,
    #[serde(rename = "layout-key")]
    pub layout_key: String
    /*#[serde(rename = "start-valid-time")]
    pub start_valid_time: Vec<String>,
    #[serde(rename = "end-valid-time")]
    pub end_valid_time: Vec<String>,*/
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Head {
    #[serde(rename = "product")]
    product: Product,

    #[serde(rename = "source")]
    source: Source,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Product {
    #[serde(rename = "title")]
    title: String,

    #[serde(rename = "field")]
    field: String,

    #[serde(rename = "category")]
    category: String,

    #[serde(rename = "creation-date")]
    creation_date: CreationDate,

    #[serde(rename = "srsName")]
    srs_name: String,

    #[serde(rename = "concise-name")]
    concise_name: String,

    #[serde(rename = "operational-mode")]
    operational_mode: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct CreationDate {
    #[serde(rename = "refresh-frequency")]
    refresh_frequency: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Source {
    #[serde(rename = "more-information")]
    more_information: String,

    #[serde(rename = "production-center")]
    production_center: ProductionCenter,

    #[serde(rename = "disclaimer")]
    disclaimer: String,

    #[serde(rename = "credit")]
    credit: String,

    #[serde(rename = "credit-logo")]
    credit_logo: String,

    #[serde(rename = "feedback")]
    feedback: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ProductionCenter {
    #[serde(rename = "sub-center")]
    sub_center: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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
    The12Hour,

    #[serde(rename = "wind")]
    Wind,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Summarization {
    #[serde(rename = "none")]
    None,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum TimeCoordinate {
    #[serde(rename = "local")]
    Local,
}
