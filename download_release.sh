#!/bin/bash

# Define variables
owner="tee8z"
repo="noaa-data-pipeline"
tag="v0.3.0"
daemon="daemon-x86_64-unknown-linux-gnu.tar.xz"
oracle="oracle-x86_64-unknown-linux-gnu.tar.xz"

# Construct URL
daemon_url="https://github.com/$owner/$repo/releases/download/$tag/$daemon"
oracle_url="https://github.com/$owner/$repo/releases/download/$tag/$oracle"

# Download the binaries
curl -LJO $daemon_url
curl -LJO $oracle_url

# Create a directory with the repository name
mkdir "release-$tag"

full_path=$(pwd)/"release-$tag"

# Unzip the downloaded file into the temporary directory
tar -xf $daemon -C $full_path
tar -xf $oracle -C $full_path

rm $daemon
rm $oracle
