### Get list of files
##### Request:
```
curl http://localhost:9100/files
```
##### Request:
```json
{"file_names":["observations_2024-01-14T04:44:22.246930703Z.parquet","forecasts_2024-01-14T04:44:22.246930703Z.parquet"]}
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

### The service expects the following folders in the working directory path (where the binary is running)
- `./ui`
- `./weather_data`
