#!/usr/bin/env bash

set -euo pipefail

LOGPATH="../data/logs"
OUTPATH="../data/outputs"
AOI="../data/aois/pavc_lt25gps_mostrecent_outlier-removed.geojson"

export CDSE_USERNAME="morganrsteckler@gmail.com"
export CDSE_PASSWORD_FILE="$HOME/.cdse/cdse_pw"

sentinel-py csv2geojson \
  --csv /mnt/poseidon/remotesensing/arctic/pavc/data/misc_data/filtered_lt25gps_mostrecent_outlier-removed_per_site_fcover_with_aux.csv \
  --lon longitude \
  --lat latitude \
  --output /mnt/poseidon/remotesensing/6ru/sentinel-py/data/aois/pavc_lt25gps_mostrecent_outlier-removed.geojson \

sentinel-py query \
  --aoi $AOI \
  --cache-dir /mnt/poseidon/remotesensing/6ru/sentinel-py/examples/cache \
  --crs EPSG:4326 \
  --years "2019 2020 2021 2022 2023 2024" \
  --speriod 06-01 \
  --eperiod 08-31 \
  --collection SENTINEL-2 \
  --product S2MSI2A \
  --ops-mode INS-NOBS \
  --log $LOGPATH/s2_pavc_query

sentinel-py download \
  --mission S2 \
  --bands "B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 SCL" \
  --outdir /mnt/poseidon/remotesensing/6ru/sentinel-py/data/s2/raw \
  --res 20 \
  --config $HOME/.s5cfg \
  --cache-dir /mnt/poseidon/remotesensing/6ru/sentinel-py/examples/cache \
  --log $LOGPATH/s2_pavc_download