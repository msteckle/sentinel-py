#!/usr/bin/env bash
set -euo pipefail

LOGPATH="../data/logs/download"
LOGFILENAME=$(basename "$0")
PIPELINE="$(dirname "$0")/pavc_gridded_rf_points_local_training.yaml"

sentinel-py run "$PIPELINE" \
  --log $LOGPATH/${LOGFILENAME}