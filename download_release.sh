#!/bin/bash

# Define variables
owner="tee8z"
repo="noaa-data-pipeline"
tag="v0.0.1"
asset_name="app-x86_64-unknown-linux-gnu.tar.gz"

# Construct URL
url="https://github.com/$owner/$repo/releases/download/$tag/$asset_name"

# Download the binary
curl -LJO $url

# Create a directory with the repository name
mkdir "release-$tag"

full_path=$(pwd)/"release-$tag"

# Unzip the downloaded file into the temporary directory
tar -xzf $asset_name -C $full_path

rm $asset_name