use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct CurrentObservation {
    #[serde(rename = "credit")]
    pub credit: String,

    #[serde(rename = "credit_URL")]
    pub credit_url: String,

    #[serde(rename = "image")]
    pub image: Image,

    #[serde(rename = "suggested_pickup")]
    pub suggested_pickup: String,

    #[serde(rename = "suggested_pickup_period")]
    pub suggested_pickup_period: String,

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

    #[serde(rename = "weather")]
    pub weather: String,

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

    #[serde(rename = "two_day_history_url")]
    pub two_day_history_url: String,

    #[serde(rename = "ob_url")]
    pub ob_url: String,

    #[serde(rename = "disclaimer_url")]
    pub disclaimer_url: String,

    #[serde(rename = "copyright_url")]
    pub copyright_url: String,

    #[serde(rename = "privacy_policy_url")]
    pub privacy_policy_url: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Image {
    #[serde(rename = "url")]
    url: String,

    #[serde(rename = "title")]
    title: String,

    #[serde(rename = "link")]
    link: String,
}
