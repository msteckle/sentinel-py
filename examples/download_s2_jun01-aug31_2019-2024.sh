#!/usr/bin/env bash
set -euo pipefail

# Params used more than once
GEOJSON="data/aois/toolik_aoi.geojson"
LOGPATH="data/logs"

# Set up user/password for CDSE
# Note: you need to have an account with CDSE to download data
export CDSE_USERNAME="morganrsteckler@gmail.com"
export CDSE_PASSWORD_FILE="$HOME/.cdse/cdse_pw"  # ensure chmod 600 on this file or it won't read

# Create an AOI
sentinel-py aoi \
  --bbox "-150 67 -148 69" \
  --out-file $GEOJSON \
  --log-path $LOGPATH \
  --verbose

# Download all Sentinel-2 summer scenes for 2019–2024
sentinel-py s2 download \
  --aoi $GEOJSON \
  --output "data/s2" \
  --start-year 2019 \
  --start-month 6 \
  --start-day 1 \
  --end-year 2024 \
  --end-month 8 \
  --end-day 31 \
  --collection-name "Sentinel-2 MSI Level-2A" \
  --product-type "S2MSI2A" \
  --bands B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 \
  --target-res-m 20 \
  --max-workers-files 2 \
  --log-path $LOGPATH \
  --verbose