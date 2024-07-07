#!/bin/bash
if ! command -v unzip &> /dev/null; then
    apt-get update && apt-get install -y unzip
fi
if [ ! -d "duckdb_lib" ]; then
    mkdir duckdb_lib
else
    rm -rf duckdb_lib
    mkdir duckdb_lib
    echo "Directory $dir already exists."
fi

if [ -f "libduckdb-linux-amd64.zip" ]; then
    # File exists, remove it
    rm "libduckdb-linux-amd64.zip"
fi

wget "https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip"
unzip libduckdb-linux-amd64.zip -d duckdb_lib
rm libduckdb-linux-amd64.zip
echo "$(pwd)/duckdb_lib"
DUCKDB_LIB_DIR="$(pwd)/duckdb_lib" cargo build