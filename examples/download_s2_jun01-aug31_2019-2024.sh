#!/usr/bin/env bash
set -euo pipefail

# Example AOI and output directory
AOI_FILE="data/aoi_example.geojson"
OUT_DIR="data/s2"
CAT="https://cdse-catalogue.dataspace.copernicus.eu/odata/v1/Products"
CREDS="path/to/credentials.json"
LOG_FILE="data/sentinel-py_download.log"

# Download all Sentinel-2 summer scenes for 2019–2024
sentinel-py s2 download-seasonally \
  --aoi "$AOI_FILE" \
  --output "$OUT_DIR" \
  --start-year 2019 \
  --start-month 6 \
  --start-day 1 \
  --end-year 2024 \
  --end-month 8 \
  --end-day 31 \
  --catalogue-odata "$CAT" \
  --collection-name "Sentinel-2 MSI Level-2A" \
  --product-type "S2MSI2A" \
  --bands B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 \
  --target-res-m 20 \
  --credentials "$CREDS" \
  --max-workers-files 2 \
  --log-file "$LOG_FILE"