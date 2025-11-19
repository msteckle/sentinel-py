#!/usr/bin/env bash
set -euo pipefail

# Paths
GEOJSON="../data/aois/toolik_aoi.geojson"
LOGPATH="../data/logs/download"
OUTPATH="../data/s2/raw"

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
  --output $OUTPATH \
  --years "2020 2021 2022 2023 2024" \
  --period-start "06-01" \
  --period-end "08-31" \
  --target-res-m 20 \
  --max-workers-files 2 \
  --log-path $LOGPATH \
  --verbose