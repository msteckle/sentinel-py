#!/usr/bin/env bash
set -euo pipefail

LOGPATH="../data/logs/processing"
LOGFILENAME=$(basename "$0")
PIPELINE="$(dirname "$0")/pavc_gridded_rf_boxes_local_training.yaml"

export SNAP_HOME=/Applications/esa-snap

sentinel-py run "$PIPELINE" --log "$LOGPATH/$LOGFILENAME"
