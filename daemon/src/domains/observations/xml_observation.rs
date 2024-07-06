use serde::{Deserialize, Serialize};

use crate::{DataSource, Request};

#[derive(Serialize, Deserialize)]
#[serde(rename = "response")]
pub struct ObservationData {
    #[serde(rename = "request_index")]
    pub request_index: String,

    #[serde(rename = "data_source")]
    pub data_source: DataSource,

    #[serde(rename = "request")]
    pub request: Request,

    #[serde(rename = "errors")]
    pub errors: String,

    #[serde(rename = "warnings")]
    pub warnings: String,

    #[serde(rename = "time_taken_ms")]
    pub time_taken_ms: String,

    #[serde(rename = "data")]
    pub data: CurrentData,

    #[serde(rename = "_xmlns:xsd")]
    pub xmlns_xsd: String,

    #[serde(rename = "_xmlns:xsi")]
    pub xmlns_xsi: String,

    #[serde(rename = "_version")]
    pub version: String,

    #[serde(rename = "_xsi:noNamespaceSchemaLocation")]
    pub xsi_no_namespace_schema_location: String,
}

#[derive(Serialize, Deserialize)]
pub struct CurrentData {
    #[serde(rename = "METAR")]
    pub metar: Vec<Metar>,

    #[serde(rename = "num_results")]
    pub num_results: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Metar {
    #[serde(rename = "raw_text")]
    pub raw_text: String,

    #[serde(rename = "station_id")]
    pub station_id: String,

    #[serde(rename = "observation_time")]
    pub observation_time: String,

    #[serde(rename = "latitude")]
    pub latitude: String,

    #[serde(rename = "longitude")]
    pub longitude: String,

    #[serde(rename = "temp_c")]
    pub temp_c: String,

    #[serde(rename = "dewpoint_c")]
    pub dewpoint_c: String,

    #[serde(rename = "wind_dir_degrees")]
    pub wind_dir_degrees: String,

    #[serde(rename = "wind_speed_kt")]
    pub wind_speed_kt: String,

    #[serde(rename = "visibility_statute_mi")]
    pub visibility_statute_mi: String,

    #[serde(rename = "altim_in_hg")]
    pub altim_in_hg: String,

    #[serde(rename = "quality_control_flags")]
    pub quality_control_flags: QualityControlFlags,

    #[serde(rename = "sky_condition")]
    pub sky_condition: SkyConditionUnion,

    #[serde(rename = "flight_category")]
    pub flight_category: String,

    #[serde(rename = "metar_type")]
    pub metar_type: String,

    #[serde(rename = "elevation_m")]
    pub elevation_m: String,

    #[serde(rename = "wx_string")]
    pub wx_string: Option<String>,

    #[serde(rename = "precip_in")]
    pub precip_in: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QualityControlFlags {
    #[serde(rename = "auto")]
    pub auto: Option<String>,

    #[serde(rename = "auto_station")]
    pub auto_station: Option<String>,

    #[serde(rename = "no_signal")]
    pub no_signal: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SkyConditionElement {
    #[serde(rename = "sky_cover")]
    pub sky_cover: String,

    #[serde(rename = "cloud_base_ft_agl")]
    pub cloud_base_ft_agl: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SkyConditionUnion {
    SkyConditionElement(SkyConditionElement),

    SkyConditionElementArray(Vec<SkyConditionElement>),
}
