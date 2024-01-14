## A simple system showing how to create a data pipeline from NOAA

### Where data comes from:
- Info on where the data used to generate the parquet files comes from:
    - Observations: https://w1.weather.gov/xml/current_obs/all_xml.zip
    - Forecasts (Multiple Point Unsummarized Data): https://graphical.weather.gov/xml/rest.php
- These xml data files are update once an hour by NOAA, so to be respectful of their services we run our data pulling process once an hour as well

### How the system works:
- daemon:
    - Background process to pull down data from NOAA and transform it into flatted parquet files. These files are then pushed to the `parquet_file_service` via the REST endpoint `POST http://localhost:9100/file` (via multipart form)
- parquet_file_service:
    - A REST API that takes in the parquet files and allows downloading of them from a arebones browser UI that is also hosts.
- assets:
    - Holds the browser UI that's just an index.html and main.js file. It uses `@duckdb/duckdb-wasm` to allow the end user to query directly against the download parquet files
    - It uses `https://bulma.io/` for css styling

### Why build a data pipeline like this:
- No remote DB needed, only a dumb file server, makes this cheap to run
- Faster and more flexible querying ability provided to the end user, allowing them to find unique insights that the original system design may not be looking to find
- Each piece is a 'simple' logical item, allowing for scalabilty for however large the usage is on the service
- Would NOT recommend using this approach if the data being tracked needs to be updated in the parquet files and stored as a relational model, only really works if the data model can be snapshots and immutable over time


### View of how the piece talk with each other: 
```
[noaa api] <- [daemon] -> parquet files -> [parquet_file_service] <- parquet files <- [browser duck_db]
```

### How to use:
- [Daemon](./daemon/README.md)
- [Parquet_file_service](./parquet_file_service/README.md)
- [Browser] (./assets/README.md)