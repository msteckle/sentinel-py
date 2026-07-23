#!/usr/bin/env bash
set -euo pipefail

sentinel-py bbox2geojson \
  --bounds -149.725 68.75 -149.475 69.0 \
  --output ../data/aois/toolik_025_aoi.geojson