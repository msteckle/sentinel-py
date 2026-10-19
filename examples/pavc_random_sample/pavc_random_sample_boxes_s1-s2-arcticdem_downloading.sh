#!/usr/bin/env bash
set -euo pipefail

AOI="../data/aois/pavc_random_sample_boxes.geojson"
LOGPATH="../data/logs/download"
LOGFILENAME=$(basename "$0")
OUTPATH="../data"
RES=20
YEARS="2023 2024"
SPERIOD=06-01
EPERIOD=08-31

# Sentinel-2: configure CDSE credentials before running these commands.
sentinel-py cdse query \
  --aoi "$AOI" --crs EPSG:4326 --years "$YEARS" \
  --speriod "$SPERIOD" --eperiod "$EPERIOD" --product S2MSI2A \
  --log "$LOGPATH/$LOGFILENAME"
sentinel-py cdse download \
  --mission S2 --bands "B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 SCL" \
  --outdir "$OUTPATH/s2/raw" --res "$RES" --config "$HOME/.s5cfg" \
  --log "$LOGPATH/$LOGFILENAME"

# Sentinel-1: configure Earthdata credentials before running these commands.
sentinel-py asf query \
  --aoi "$AOI" --years "$YEARS" --speriod "$SPERIOD" --eperiod "$EPERIOD"
sentinel-py asf download \
  --outdir "$OUTPATH/s1/raw" --config "$HOME/.netrc" --processes 8

# ArcticDEM: PGC access is public.
sentinel-py pgc query --aoi "$AOI" --crs EPSG:4326 --log "$LOGPATH/$LOGFILENAME"
sentinel-py pgc download \
  --outdir "$OUTPATH/arcticdem/10m" --processes 8 --retries 3 \
  --log "$LOGPATH/$LOGFILENAME"
