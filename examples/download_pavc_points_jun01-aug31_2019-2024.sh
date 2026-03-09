#!/usr/bin/env bash

set -euo pipefail

LOGPATH="../data/logs"

# Set up user/password for CDSE
# Note: you need to have an account with CDSE to download data
export CDSE_USERNAME="morganrsteckler@gmail.com"
export CDSE_PASSWORD_FILE="$HOME/.cdse/cdse_pw"  # ensure chmod 600 on this file or it won't read

sentinel-py csv2geojson \
  --csv /mnt/poseidon/remotesensing/arctic/pavc/data/misc_data/filtered_lt25gps_mostrecent_outlier-removed_per_site_fcover_with_aux.csv \
  --lon longitude \
  --lat latitude \
  --output /mnt/poseidon/remotesensing/6ru/sentinel-py/data/aois/pavc_lt25gps_mostrecent_outlier-removed.geojson \
  --log $LOGPATH/geojson

sentinel-py s2 download \
  --aoi /mnt/poseidon/remotesensing/6ru/sentinel-py/data/aois/pavc_lt25gps_mostrecent_outlier-removed.geojson \
  --outdir /mnt/poseidon/remotesensing/6ru/sentinel-py/data/s2/raw \
  --years "2019 2020 2021 2022 2023 2024" \
  --speriod 06-01 \
  --eperiod 08-31 \
  --log $LOGPATH/download_pavc_pts
