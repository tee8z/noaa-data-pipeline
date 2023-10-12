use serde::{Deserialize, Serialize};
use serde_json::Value;
// zone information
// https://api.weather.gov/zones?id=VAZ097&type=land&limit=500
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    #[serde(rename = "@context")]
    pub context: Option<Context>,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    pub features: Option<Vec<Feature>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(rename = "@version")]
    pub version: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Feature {
    pub id: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub geometry: Option<Value>,
    pub properties: Properties,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Properties {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_field: String,
    #[serde(rename = "id")]
    pub id2: String,
    #[serde(rename = "type")]
    pub type_field2: String,
    pub name: Option<String>,
    pub effective_date: Option<String>,
    pub expiration_date: Option<String>,
    pub state: Option<String>,
    pub cwa: Option<Vec<String>>,
    pub forecast_offices: Vec<String>,
    pub time_zone: Option<Vec<String>>,
    pub observation_stations: Vec<String>,
    pub radar_station: Option<String>,
}
