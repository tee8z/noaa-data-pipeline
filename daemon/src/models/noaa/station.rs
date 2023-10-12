use parquet::record::RowAccessor;
use parquet::{errors::ParquetError, record::Row};
use parquet_derive::ParquetRecordWriter;
use serde::{Deserialize, Serialize};

use crate::{ForecastProperties, ObservationProperties};
// combined results
#[derive(Debug, Default, Clone)]
pub struct Mapping {
    pub zone_id: String,
    pub forecast_office_id: String,
    pub observation_station_id: String,
    pub observation_latitude: u64,
    pub observation_longitude: u64,
    pub raw_coordinates: Vec<f64>,
    pub forecast_values: ForecastProperties,
    pub observation_values: ObservationProperties,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, ParquetRecordWriter)]
#[serde(rename_all = "camelCase")]
pub struct Station {
    pub station_id: String,
    pub zone_id: String,
    pub station_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

impl Station {
    pub fn from_parquet_row(self, row: &Row) -> Result<Self, ParquetError> {
        let station_id = row.get_bytes(0)?.as_utf8()?;
        let zone_id = row.get_bytes(1)?.as_utf8()?;
        let station_name = row.get_bytes(2)?.as_utf8()?;
        let latitude = row.get_double(3)?;
        let longitude = row.get_double(4)?;
        Ok(Self {
            station_id: station_id.to_owned(),
            zone_id: zone_id.to_owned(),
            station_name: station_name.to_owned(),
            latitude: latitude,
            longitude: longitude,
        })
    }
}

// stations detail
// https://api.weather.gov/stations?id=KPVG%2CKCNB&limit=500
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StationRoot {
    #[serde(rename = "@context")]
    pub context: Option<(String, Context)>,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    pub features: Vec<Feature>,
    pub observation_stations: Vec<String>,
    pub pagination: Pagination,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(rename = "@version")]
    pub version: String,
    pub wx: Option<String>,
    pub s: Option<String>,
    pub geo: Option<String>,
    pub unit: Option<String>,
    #[serde(rename = "@vocab")]
    pub vocab: String,
    pub geometry: Option<Geometry>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub distance: Option<Distance>,
    pub bearing: Option<Bearing>,
    pub value: Option<Value>,
    pub unit_code: Option<UnitCode>,
    pub forecast_office: Option<ForecastOffice>,
    pub forecast_grid_data: Option<ForecastGridData>,
    pub public_zone: Option<PublicZone>,
    pub county: Option<County>,
    pub observation_stations: Option<ObservationStations>,
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
    pub forecast: Option<String>,
    pub county: Option<String>,
    pub fire_weather_zone: Option<String>,
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
    pub next: Option<String>,
}
