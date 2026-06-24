#!/usr/bin/env bash
set -euo pipefail

# Paths
AOI="../data/aois/bioclimate_latlon/bioclimate_latlon_noglacier.shp"
OUT="../data/aois/bioclimate_latlon_noglacier_grid.geojson"

sentinel-py grid \
  --aoi $AOI \
  --px 0.5 0.5 \
  --output $OUT \