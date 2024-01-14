use std::{collections::HashSet, fmt, fs::File, io::Read};

use serde::{Deserialize, Serialize};

use crate::get_full_path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WeatherStation {
    pub station_id: String,
    pub latitude: String,
    pub longitude: String,
}
impl fmt::Display for WeatherStation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Station ID: {}, Latitude: {}, Longitude: {}",
            self.station_id, self.latitude, self.longitude
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CityWeather {
    #[serde(flatten)]
    pub city_data: std::collections::HashMap<String, WeatherStation>,
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
    pub fn get_coordinates(&self) -> String {
        self.city_data
            .values()
            .map(|weather_station| {
                format!("{},{}", weather_station.latitude, weather_station.longitude)
            })
            .collect::<Vec<String>>()
            .join("%20")
    }

    pub fn get_station_ids(&self) -> HashSet<String> {
        let mut station_ids: HashSet<String> = HashSet::new();
        self.city_data.iter().for_each(|(_city_name, city_data)| {
            station_ids.insert(city_data.station_id.clone());
        });
        station_ids
    }
}

pub fn get_coordinates() -> CityWeather {
    //TODO: change to pull this list down from https://w1.weather.gov/xml/current_obs/index.xml
    let full_path = get_full_path(String::from("./static_data/station_coordinates.json"));
    let mut file_content = String::new();
    File::open(full_path)
        .expect("Unable to open the file")
        .read_to_string(&mut file_content)
        .expect("Unable to read the file");

    serde_json::from_str(&file_content).unwrap()
}
