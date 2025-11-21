#!/usr/bin/env bash
set -euo pipefail

# Paths
IN="/mnt/poseidon/remotesensing/6ru/sentinel-py/data/s2/pb_offset_vrts/T05WPQ_20200704T214531_B04_20m.pb_offset.vrt"
OUT="/mnt/poseidon/remotesensing/6ru/sentinel-py/data/tests/T05WPQ_20200704T214531_B04_20m.pb_offset.tif"

sentinel-py translate \
  --src-file $IN \
  --dst-file $OUT