use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct CurrentObservation {

    #[serde(rename = "location")]
    pub location: String,

    #[serde(rename = "station_id")]
    pub station_id: String,

    #[serde(rename = "latitude")]
    pub latitude: String,

    #[serde(rename = "longitude")]
    pub longitude: String,

    #[serde(rename = "observation_time")]
    pub observation_time: String,

    #[serde(rename = "observation_time_rfc822")]
    pub observation_time_rfc822: String,

    #[serde(rename = "temperature_string")]
    pub temperature_string: String,

    #[serde(rename = "temp_f")]
    pub temp_f: String,

    #[serde(rename = "temp_c")]
    pub temp_c: String,

    #[serde(rename = "relative_humidity")]
    pub relative_humidity: String,

    #[serde(rename = "wind_string")]
    pub wind_string: String,

    #[serde(rename = "wind_dir")]
    pub wind_dir: String,

    #[serde(rename = "wind_degrees")]
    pub wind_degrees: String,

    #[serde(rename = "wind_mph")]
    pub wind_mph: String,

    #[serde(rename = "wind_kt")]
    pub wind_kt: String,

    #[serde(rename = "dewpoint_string")]
    pub dewpoint_string: String,

    #[serde(rename = "dewpoint_f")]
    pub dewpoint_f: String,

    #[serde(rename = "dewpoint_c")]
    pub dewpoint_c: String,
}