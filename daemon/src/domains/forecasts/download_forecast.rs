use std::{fmt, ops::Add};

use anyhow::Error;
use serde::{Deserialize, Serialize};
use slog::Logger;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};
use serde_xml_rs::{from_str};
use crate::{CityWeather, fetch_xml, Dwml};

/*
More Options defined  here:
https://graphical.weather.gov/xml/docs/elementInputNames.php

Maximum Temperature 	maxt
Minimum Temperature 	mint
Wind Speed 	wspd
Wind Direction 	wdir
12 Hour Probability of Precipitation 	pop12
Liquid Precipitation Amount 	qpf
Maximum Relative Humidity 	maxrh
Minimum Relative Humidity 	minrh
*/
#[derive(Debug)]
pub struct WeatherForecast {
    pub station_id: String,
    pub request_time: OffsetDateTime,
    pub being_time: OffsetDateTime,
    pub end_time: OffsetDateTime,
    pub max_temp: f64,
    pub min_temp: f64,
    pub wind_speed: f64,
    pub wind_direction: String,
    pub max_relative_humidity: f64,
    pub min_relative_humidity: f64,
    pub liquid_precipitation_amt: f64,
    pub twelve_hour_probability_of_precipitation: f64,
}

impl fmt::Display for WeatherForecast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Station ID: {}, Max Temp: {}, Longitude: {}",
            self.station_id, self.max_temp, self.min_temp
        )
    }
}

#[derive(Debug)]
pub struct CityForecasts {
    pub city_data: std::collections::HashMap<String, WeatherForecast>,
}


pub async fn get_forecasts(logger: &Logger, city_weather: CityWeather) -> Result<(),Error> {
   // let url = get_url(city_weather);
    //println!("url: {}", url);
    let url = "https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?listLatLon=36.2,-95.88&product=time-series&begin=2024-01-12T19:19:45.652171155Z&end=2024-01-19T19:19:45.652171155Z&Unit=e&maxt=maxt&mint=mint&wspd=wspd&wdir=wdir&pop12=pop12&qpf=qpf&maxrh=maxrh&minrh=minrh";
    let raw_xml = fetch_xml(logger, &url).await?;
    println!("{}", raw_xml);
    let converted_xml: Dwml = serde_xml_rs::from_str(&raw_xml)?;
    println!("{:?}", converted_xml);
    Ok(())
}


fn get_url(city_weather: CityWeather) -> String {
    let current_time = OffsetDateTime::now_utc();
    let format_description = Rfc3339;
    let now = current_time.format(&format_description).unwrap();
    // Define the duration of one week (7 days)
    let one_week_duration = Duration::weeks(1);
    let one_week_from_now = current_time.add(one_week_duration);
    let one_week = one_week_from_now.format(&format_description).unwrap();

    format!("https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?listLatLon={}&product=time-series&begin={}&end={}&Unit=e&maxt=maxt&mint=mint&wspd=wspd&wdir=wdir&pop12=pop12&qpf=qpf&maxrh=maxrh&minrh=minrh", city_weather.get_coordinates(),now,one_week)
}

