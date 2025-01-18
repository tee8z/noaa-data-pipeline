### Get duckdb library for compling
```
When linking against a DuckDB library already on the system (so not using any of the bundled features), you can set the DUCKDB_LIB_DIR environment variable to point to a directory containing the library. You can also set the DUCKDB_INCLUDE_DIR variable to point to the directory containing duckdb.h.
```
```
wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip
mkdir duckdb_lib
unzip libduckdb-linux-amd64.zip -d duckdb_lib
cp duckdb_lib/lib*.so* /usr/local/lib/
ldconfig
```
* Then set the DUCKDB_LIB_DIR var to (or whatever the full path to the folder holding the library is at)
```
DUCKDB_LIB_DIR="/home/<user>/duckdb_lib"
```

### Get list of files (optional params for filtering)
##### Request:
```
curl http://localhost:9100/files
```
##### Response:
```json
{"file_names":["observations_2024-01-14T04:44:22.246930703Z.parquet","forecasts_2024-01-14T04:44:22.246930703Z.parquet"]}
```

#### Request:
```
curl "http://localhost:9100/files?start=2024-01-15T00:00:00.00Z&end=2024-01-16T00:00:00.00Z"
```

#### Response:
```json
{"file_names":["observations_2024-01-15T04:36:33.95238406Z.parquet","forecasts_2024-01-15T04:36:33.95238406Z.parquet"]}
```

#### Request:
```
curl "http://localhost:9100/files?start=2024-01-15T00:00:00.00Z&end=2024-01-16T00:00:00.00Z&forecasts=true"
```

#### Response:
```json
{"file_names":["forecasts_2024-01-15T04:36:33.95238406Z.parquet"]}
```

#### Request:
```
curl "http://localhost:9100/files?start=2024-01-15T00:00:00.00Z&end=2024-01-16T00:00:00.00Z&observations=true"
```

#### Response:
```json
{"file_names":["observations_2024-01-15T04:36:33.95238406Z.parquet"]}
```

### Get a single file
##### Request:
```
curl -L -O http://localhost:9100/file/observations_2024-01-14T04:44:22.246930703Z.parquet
```
##### Reponse:
```
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  7540    0  7540    0     0  1830k      0 --:--:-- --:--:-- --:--:-- 2454k
```

### Post a file (can't be larger than 1mb and needs to be a parquet file)
##### Request:
```
curl -H "Content-Type: multipart/form-data" -F "file=@/home/tee8z/repos/noaa-data-pipeline/data/forecasts_2024-01-14T04:44:22.246930703Z.parquet" http://localhost:9100/file/forecasts_2024-01-14T04:44:22.246930703Z.parquet -v
```
##### Reponse:
```
*   Trying 127.0.0.1:9100...
* Connected to localhost (127.0.0.1) port 9100 (#0)
> POST /file/forecasts_2024-01-14T04:44:22.246930703Z.parquet HTTP/1.1
> Host: localhost:9100
> User-Agent: curl/7.81.0
> Accept: */*
> Content-Length: 36281
> Content-Type: multipart/form-data; boundary=------------------------d8b9fa0cf983802b
>
* We are completely uploaded and fine
* Mark bundle as not supporting multiuse
< HTTP/1.1 200 OK
< access-control-allow-origin: *
< vary: origin
< vary: access-control-request-method
< vary: access-control-request-headers
< content-length: 0
< date: Sun, 14 Jan 2024 14:43:23 GMT
<
* Connection #0 to host localhost left intact
```

### Get small subset of observation data
curl -v "http://localhost:9100/stations/observations?start=2024-02-15T00:00:00.00Z&end=2024-02-25T00:00:00.00Z&station_ids=KLWV,KLBB,KTOA"

### Get small subset of forecast data
curl -v "http://localhost:9100/stations/forecasts?start=2024-02-15T00:00:00.00Z&end=2024-02-25T00:00:00.00Z&station_ids=KLWV,KLBB,KTOA"

### Get stations stored in observation data
curl -v "http://localhost:9100/stations


### The service expects the following folders in the working directory path (where the binary is running)
- `./ui`
- `./weather_data`
