use serde::{Deserialize, Serialize};
use serde_json::Value;
// forecast payload
// https://api.weather.gov/gridpoints/AKQ/76,37/forecast?units=us
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    #[serde(rename = "@context")]
    pub context: Option<(String, Context)>,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    pub geometry: Option<Geometry>,
    #[serde(rename = "properties")]
    pub properties: ForecastProperties,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(rename = "@version")]
    pub version: String,
    pub wx: String,
    pub geo: String,
    pub unit: String,
    #[serde(rename = "@vocab")]
    pub vocab: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    #[serde(rename = "type")]
    pub type_field: String,
    pub coordinates: Vec<Vec<Vec<f64>>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastProperties {
    pub updated: String,
    pub units: String,
    pub forecast_generator: String,
    pub generated_at: String,
    pub update_time: String,
    pub valid_times: String,
    pub elevation: Elevation,
    pub periods: Vec<Period>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elevation {
    pub unit_code: String,
    pub value: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Period {
    pub number: i64,
    pub name: String,
    pub start_time: String,
    pub end_time: String,
    pub is_daytime: bool,
    pub temperature: i64,
    pub temperature_unit: String,
    pub temperature_trend: Value,
    pub probability_of_precipitation: ProbabilityOfPrecipitation,
    pub dewpoint: Dewpoint,
    pub relative_humidity: RelativeHumidity,
    pub wind_speed: Option<String>,
    pub wind_direction: Option<String>,
    pub icon: Option<String>,
    pub short_forecast: Option<String>,
    pub detailed_forecast: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbabilityOfPrecipitation {
    pub unit_code: String,
    pub value: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dewpoint {
    pub unit_code: String,
    pub value: Option<f64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelativeHumidity {
    pub unit_code: String,
    pub value: Option<i64>,
}
