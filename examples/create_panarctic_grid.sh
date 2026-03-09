#!/usr/bin/env bash
set -euo pipefail

# Paths
AOI="../data/aois/bioclimate_latlon/bioclimate_latlon.shp"
AOIOUT="../data/aois/bioclimate_latlon_grid.geojson"
LOGPATH="../data/logs/grid"

sentinel-py grid \
  --aoi $AOI \
  --px 0.5 0.5 \
  --output $AOIOUT \
  --log $LOGPATH