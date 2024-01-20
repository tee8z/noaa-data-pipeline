use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::{Point, XmlFetcher};
use anyhow::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WeatherStation {
    pub station_id: String,
    pub station_name: String,
    pub latitude: String,
    pub longitude: String,
}
impl fmt::Display for WeatherStation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Station ID: {}, Station Name: {}, Latitude: {}, Longitude: {}",
            self.station_id, self.station_name, self.latitude, self.longitude
        )
    }
}

impl From<Station> for WeatherStation {
    fn from(value: Station) -> Self {
        WeatherStation {
            station_id: value.station_id,
            station_name: value.station_name,
            latitude: value.latitude,
            longitude: value.longitude,
        }
    }
}

impl WeatherStation {
    pub fn get_latitude(&self) -> String {
        format!("{:.2}", self.latitude.parse::<f64>().unwrap())
    }
    pub fn get_longitude(&self) -> String {
        format!("{:.2}", self.longitude.parse::<f64>().unwrap())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CityWeather {
    #[serde(flatten)]
    pub city_data: HashMap<String, WeatherStation>,
}

impl fmt::Display for CityWeather {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (city, station) in &self.city_data {
            writeln!(
                f,
                "City: {}, Station ID: {}, Latitude: {}, Longitude: {}",
                city, station.station_id, station.latitude, station.longitude
            )?;
        }
        Ok(())
    }
}
impl CityWeather {
    pub fn get_coordinates_url(&self) -> String {
        self.get_coordinates().join("%20")
    }

    pub fn get_coordinates(&self) -> Vec<String> {
        self.city_data
            .values()
            .map(|weather_station| {
                format!(
                    "{},{}",
                    weather_station.get_latitude(),
                    weather_station.get_longitude()
                )
            })
            .collect::<Vec<String>>()
    }
    pub fn remove_coordinates(mut self, point: Point) {
        self.city_data
            .retain(|_, v| !(v.latitude == point.latitude && v.longitude == point.longitude));
    }
    pub fn get_station_ids(&self) -> HashSet<String> {
        let mut station_ids: HashSet<String> = HashSet::new();
        self.city_data.iter().for_each(|(_city_name, city_data)| {
            station_ids.insert(city_data.station_id.clone());
        });
        station_ids
    }
}

pub fn split_cityweather(original: CityWeather, max_keys_per_map: usize) -> Vec<CityWeather> {
    let mut result: Vec<CityWeather> = Vec::new();
    let mut current_map = HashMap::new();
    let mut current_keys = 0;

    for (key, value) in original.city_data {
        // Check if adding the entry exceeds the maximum keys
        if current_keys + 1 > max_keys_per_map {
            // If yes, start a new map
            result.push(CityWeather {
                city_data: std::mem::take(&mut current_map),
            });
            current_keys = 0;
        }

        // Add the entry to the current map
        current_map.insert(key.clone(), value.clone());
        current_keys += 1;
    }

    // Add the last map if not empty
    if !current_map.is_empty() {
        result.push(CityWeather {
            city_data: current_map,
        });
    }

    result
}

static STATE_ABBERVIATIONS: &[&str] = &[
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR",
    "MD", "MA", "MI", "MN", "MS", "MO", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
];

pub async fn get_coordinates(fetcher: Arc<XmlFetcher>) -> Result<CityWeather, Error> {
    let mut city_data: HashMap<String, WeatherStation> = HashMap::new();
    let raw_xml = fetcher
        .fetch_xml("https://w1.weather.gov/xml/current_obs/index.xml")
        .await?;
    let converted_xml: WxStationIndex = serde_xml_rs::from_str(&raw_xml)?;

    for station in converted_xml.station {
        // Skip any place not in the US
        if !STATE_ABBERVIATIONS.contains(&station.state.as_str()) {
            continue;
        }
        let weather_station: WeatherStation = station.clone().into();
        city_data.insert(station.station_id, weather_station);
    }

    Ok(CityWeather { city_data })
}

#[derive(Serialize, Deserialize)]
pub struct WxStationIndex {
    #[serde(rename = "credit")]
    credit: String,

    #[serde(rename = "credit_URL")]
    credit_url: String,

    #[serde(rename = "image")]
    image: StationImage,

    #[serde(rename = "suggested_pickup")]
    suggested_pickup: String,

    #[serde(rename = "suggested_pickup_period")]
    suggested_pickup_period: String,

    #[serde(rename = "station")]
    station: Vec<Station>,
}

#[derive(Serialize, Deserialize)]
pub struct StationImage {
    #[serde(rename = "url")]
    url: String,

    #[serde(rename = "title")]
    title: String,

    #[serde(rename = "link")]
    link: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Station {
    #[serde(rename = "station_id")]
    station_id: String,

    #[serde(rename = "state")]
    state: String,

    #[serde(rename = "station_name")]
    station_name: String,

    #[serde(rename = "latitude")]
    latitude: String,

    #[serde(rename = "longitude")]
    longitude: String,

    #[serde(rename = "html_url")]
    html_url: String,

    #[serde(rename = "rss_url")]
    rss_url: String,

    #[serde(rename = "xml_url")]
    xml_url: String,
}
