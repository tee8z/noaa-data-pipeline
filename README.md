
data pulled from
https://w1.weather.gov/xml/current_obs/index.xml
https://www.weather.gov/documentation/services-web-api#/

no central DB needed, cheaper to run
[noaa api] <- [pulling service] -> parquet files -> [api file server] <- parquet files <- [browser duck db]