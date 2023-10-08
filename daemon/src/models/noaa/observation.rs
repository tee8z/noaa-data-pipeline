use serde::{Deserialize, Serialize};

// station observation
// https://api.weather.gov/stations/KPVG/observations/latest?require_qc=false

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    #[serde(rename = "@context")]
    pub context: Option<(String, Context)>,
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub type_field: String,
    pub geometry: Option<Geometry2>,
    #[serde(rename = "properties")]
    pub properties: ObservationProperties,
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
    pub vocab: Option<String>,
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
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    #[serde(rename = "@id")]
    pub id: Option<String>,
    #[serde(rename = "@type")]
    pub type_field: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Distance {
    #[serde(rename = "@id")]
    pub id: Option<String>,
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
    pub id: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnitCode {
    #[serde(rename = "@id")]
    pub id: Option<String>,
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
pub struct Geometry2 {
    #[serde(rename = "type")]
    pub type_field: String,
    pub coordinates: Vec<f64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationProperties {
    #[serde(rename = "@id")]
    pub id: Option<String>,
    #[serde(rename = "@type")]
    pub type_field: String,
    pub elevation: Elevation,
    pub station: String,
    pub timestamp: String,
    pub raw_message: String,
    pub text_description: String,
    pub icon: String,
    pub present_weather: Vec<Value>,
    pub temperature: Temperature,
    pub dewpoint: Dewpoint,
    pub wind_direction: WindDirection,
    pub wind_speed: WindSpeed,
    pub wind_gust: WindGust,
    pub barometric_pressure: BarometricPressure,
    pub sea_level_pressure: SeaLevelPressure,
    pub visibility: Visibility,
    #[serde(rename = "maxTemperatureLast24Hours")]
    pub max_temperature_last24hours: MaxTemperatureLast24Hours,
    #[serde(rename = "minTemperatureLast24Hours")]
    pub min_temperature_last24hours: MinTemperatureLast24Hours,
    pub precipitation_last_hour: PrecipitationLastHour,
    #[serde(rename = "precipitationLast3Hours")]
    pub precipitation_last3hours: PrecipitationLast3Hours,
    #[serde(rename = "precipitationLast6Hours")]
    pub precipitation_last6hours: PrecipitationLast6Hours,
    pub relative_humidity: RelativeHumidity,
    pub wind_chill: WindChill,
    pub heat_index: HeatIndex,
    pub cloud_layers: Vec<CloudLayer>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Elevation {
    pub unit_code: String,
    pub value: Option<f64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Temperature {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dewpoint {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindDirection {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindSpeed {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindGust {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BarometricPressure {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeaLevelPressure {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Visibility {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaxTemperatureLast24Hours {
    pub unit_code: String,
    pub value: Option<f64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MinTemperatureLast24Hours {
    pub unit_code: String,
    pub value: Option<f64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrecipitationLastHour {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrecipitationLast3Hours {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrecipitationLast6Hours {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelativeHumidity {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindChill {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeatIndex {
    pub unit_code: String,
    pub value: Option<f64>,
    pub quality_control: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudLayer {
    pub base: Base,
    pub amount: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Base {
    pub unit_code: String,
    pub value: f64,
}
