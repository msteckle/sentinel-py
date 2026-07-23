#!/usr/bin/env bash
set -euo pipefail

# Paths
AOI="../data/aois/toolik_025_aoi.geojson"
LOGPATH="../data/logs/download"
OUTPATH="../data"

# Set up user/password for CDSE
# Note: you need to have an account with CDSE to download data
export CDSE_USERNAME="morganrsteckler@gmail.com"
export CDSE_PASSWORD_FILE="$HOME/.cdse/cdse_pw"  # ensure chmod 600 on this file or it won't read

# Query/Download all Sentinel-2 summer scenes for 2019–2024
sentinel-py cdse query \
  --aoi $AOI \
  --crs EPSG:4326 \
  --years "2023 2024" \
  --speriod 06-01 \
  --eperiod 08-31 \
  --product S2MSI2A \
  --log $LOGPATH/s2_pavc_query

sentinel-py cdse download \
  --mission S2 \
  --bands "B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 SCL" \
  --outdir $OUTPATH/s2/raw \
  --res 20 \
  --config $HOME/.s5cfg \
  --log $LOGPATH/s2_pavc_download

# Query/Download all Sentinel-1 summer scenes for 2019–2024
sentinel-py asf query \
  --aoi $AOI \
  --years "2023 2024" \
  --speriod 06-01 \
  --eperiod 08-31 \

sentinel-py asf download \
  --outdir $OUTPATH/s1/raw \
  --config $HOME/.netrc \
  --processes 8 \



  