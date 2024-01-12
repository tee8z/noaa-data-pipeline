use std::fmt;

use serde::{Deserialize, Serialize};

use crate::CityWeather;
/*
https://graphical.weather.gov/xml/docs/elementInputNames.php

Maximum Temperature 	maxt
Minimum Temperature 	mint
Wind Speed 	wspd
Wind Direction 	wdir
12 Hour Probability of Precipitation 	pop12
Liquid Precipitation Amount 	qpf
Wind Gust 	wgust
Maximum Relative Humidity 	maxrh
Minimum Relative Humidity 	minrh 
*/

#[derive(Debug, Deserialize, Serialize)]
pub struct WeatherForecast {
    pub station_id: String,
    pub max_temp: f64,
}
impl fmt::Display for WeatherForecast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Station ID: {}, Max Temp: {}, Longitude: {}",
            self.station_id, self.max_temp, self.longitude
        )
    }
}


#[derive(Debug, Deserialize, Serialize)]
pub struct CityForecasts {
    #[serde(flatten)]
    pub city_data: std::collections::HashMap<String, WeatherForecast>,
}


pub fn get_forecasts(city_weather: CityWeather) -> CityForecasts {

}