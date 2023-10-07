use serde::{Deserialize, Serialize};

use crate::models::{
    noaa::{forecast, observation},
};
// combined results
#[derive(Debug, Default, Clone)]
pub struct Mapping {
    pub zone_id: String,
    pub forecast_office_id: String,
    pub observation_station_id: String,
    pub observation_latitude: u64,
    pub observation_longitude: u64,
    pub forecast_values: forecast::Properties,
    pub observation_values: observation::Properties,
}

// from xml file
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Station {
    pub station_id: String,
    pub state: String,
    pub station_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

// stations detail
// https://api.weather.gov/stations?id=KPVG%2CKCNB&limit=500
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    #[serde(rename = "@context")]
    pub context: (String, Context),
    #[serde(rename = "type")]
    pub type_field: String,
    pub features: Vec<Feature>,
    pub observation_stations: Vec<String>,
    pub pagination: Pagination,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(rename = "@version")]
    pub version: String,
    pub wx: String,
    pub s: String,
    pub geo: String,
    pub unit: String,
    #[serde(rename = "@vocab")]
    pub vocab: String,
    pub geometry: Geometry,
    pub city: String,
    pub state: String,
    pub distance: Distance,
    pub bearing: Bearing,
    pub value: Value,
    pub unit_code: UnitCode,
    pub forecast_office: ForecastOffice,
    pub forecast_grid_data: ForecastGridData,
    pub public_zone: PublicZone,
    pub county: County,
    pub observation_stations: ObservationStations,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Distance {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bearing {
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Value {
    #[serde(rename = "@id")]
    pub id: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnitCode {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastOffice {
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForecastGridData {
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicZone {
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct County {
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationStations {
    #[serde(rename = "@container")]
    pub container: String,
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Feature {
    pub id: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub geometry: Geometry2,
    pub properties: Properties,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Geometry2 {
    #[serde(rename = "type")]
    pub type_field: String,
    pub coordinates: Vec<f64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Properties {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_field: String,
    pub elevation: Elevation,
    pub station_identifier: String,
    pub name: String,
    pub time_zone: String,
    pub forecast: String,
    pub county: String,
    pub fire_weather_zone: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elevation {
    pub unit_code: String,
    pub value: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    pub next: String,
}
