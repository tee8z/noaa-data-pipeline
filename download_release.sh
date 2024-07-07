#!/bin/bash

# Define variables
owner="tee8z"
repo="noaa-data-pipeline"
tag="v0.3.0"
daemon="daemon-x86_64-unknown-linux-gnu.tar.xz"
file_servide="parquet_file_service-x86_64-unknown-linux-gnu.tar.xz"

# Construct URL
daemon_url="https://github.com/$owner/$repo/releases/download/$tag/$daemon"
parquet_file_service_url="https://github.com/$owner/$repo/releases/download/$tag/$file_servide"

# Download the binaries
curl -LJO $daemon_url
curl -LJO $parquet_file_service_url

# Create a directory with the repository name
mkdir "release-$tag"

full_path=$(pwd)/"release-$tag"

# Unzip the downloaded file into the temporary directory
tar -xf $daemon -C $full_path
tar -xf $file_servide -C $full_path

rm $asset_name