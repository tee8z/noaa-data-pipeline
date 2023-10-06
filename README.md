
data pulled from
https://w1.weather.gov/xml/current_obs/index.xml
https://www.weather.gov/documentation/services-web-api#/


https://api.weather.gov/stations/KPVG
https://api.weather.gov/stations?limit=500
no central DB needed, cheaper to run
[noaa api] <- [pulling service] -> parquet files -> [api file server] <- parquet files <- [browser duck db]


1) lookup stations
2) lookup zone by pulling forecast stations out of "forecast" payload
3) lookup forecast by office pulled from zone and coordinats (need to change to positive integers to work)
