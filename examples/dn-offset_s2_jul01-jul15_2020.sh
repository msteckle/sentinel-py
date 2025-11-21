#!/usr/bin/env bash
set -euo pipefail

# Paths
LOGPATH="../data/logs/download"
OUTPATH="../data/s2"

# Create PB-offset VRTs for a test downloaded band
sentinel-py s2 dn-offset \
  --input-dir $OUTPATH/raw \
  --output-dir $OUTPATH/pb_offset_vrts \
  --years "2020" \
  --speriod "07-01" \
  --eperiod "07-15" \
  --bands B04 \
  --res 20 \
  --log-path $LOGPATH \