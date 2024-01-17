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
    pub observation_time: Option<String>,

    #[serde(rename = "observation_time_rfc822")]
    pub observation_time_rfc822: Option<String>,

    #[serde(rename = "temperature_string")]
    pub temperature_string: Option<String>,

    #[serde(rename = "temp_f")]
    pub temp_f: Option<String>,

    #[serde(rename = "temp_c")]
    pub temp_c: Option<String>,

    #[serde(rename = "relative_humidity")]
    pub relative_humidity: Option<String>,

    #[serde(rename = "wind_string")]
    pub wind_string: Option<String>,

    #[serde(rename = "wind_dir")]
    pub wind_dir: Option<String>,

    #[serde(rename = "wind_degrees")]
    pub wind_degrees: Option<String>,

    #[serde(rename = "wind_mph")]
    pub wind_mph: Option<String>,

    #[serde(rename = "wind_kt")]
    pub wind_kt: Option<String>,

    #[serde(rename = "dewpoint_string")]
    pub dewpoint_string: Option<String>,

    #[serde(rename = "dewpoint_f")]
    pub dewpoint_f: Option<String>,

    #[serde(rename = "dewpoint_c")]
    pub dewpoint_c: Option<String>,
}
