#!/usr/bin/env bash
set -euo pipefail

# Paths
GEOJSON="../data/aois/toolik_aoi.geojson"
LOGPATH="../data/logs/download"
OUTPATH="../data/s2"

# Set up user/password for CDSE
# Note: you need to have an account with CDSE to download data
export CDSE_USERNAME="morganrsteckler@gmail.com"
export CDSE_PASSWORD_FILE="$HOME/.cdse/cdse_pw"  # ensure chmod 600 on this file or it won't read

# Create an AOI
sentinel-py aoi \
  --bbox "-150 67 -148 69" \
  --crs "EPSG:4326" \
  --output-file $GEOJSON \
  --log-path $LOGPATH \

# Download all Sentinel-2 summer scenes for 2019–2024
sentinel-py s2 download \
  --input-aoi $GEOJSON \
  --output-dir $OUTPATH/raw \
  --years "2020 2021 2022 2023 2024" \
  --speriod "06-01" \
  --eperiod "08-31" \
  --collection "SENTINEL-2" \
  --product "S2MSI2A" \
  --bands B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 \
  --res 20 \
  --include-scl \
  --res 20 \
  --max-workers 4 \
  --log-path $LOGPATH \