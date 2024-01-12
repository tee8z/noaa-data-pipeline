use std::{fs::File, io::Read, env, fmt};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
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


#[derive(Debug, Deserialize, Serialize)]
pub struct CityWeather {
    #[serde(flatten)]
    pub city_data: std::collections::HashMap<String, WeatherStation>,
}

impl fmt::Display for CityWeather {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (city, station) in &self.city_data {
            writeln!(
                f,
                "City: {}, {}",
                city,
                format!(
                    "Station ID: {}, Latitude: {}, Longitude: {}",
                    station.station_id, station.latitude, station.longitude
                )
            )?;
        }
        Ok(())
    }
}


pub fn get_coordinates() -> CityWeather {
    let full_path = get_full_path(String::from("./static_data/station_coordinates.json"));
    let mut file_content = String::new();
    File::open(full_path)
        .expect("Unable to open the file")
        .read_to_string(&mut file_content)
        .expect("Unable to read the file");
    
    serde_json::from_str(&file_content).unwrap()
 }

fn get_full_path(relative_path: String) -> String {
    let mut current_dir = env::current_dir().expect("Failed to get current directory");

    // Append the relative path to the current working directory
    current_dir.push(relative_path);

    // Convert the `PathBuf` to a `String` if needed
    current_dir.to_string_lossy().to_string()
}