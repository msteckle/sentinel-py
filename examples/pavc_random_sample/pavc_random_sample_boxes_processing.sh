#!/usr/bin/env bash
set -euo pipefail

LOGPATH="../data/logs/download"
LOGFILENAME=$(basename "$0")
PIPELINE="$(dirname "$0")/pavc_random_sample_boxes_processing.yaml"

export SNAP_HOME=/Applications/esa-snap

sentinel-py run "$PIPELINE" --log "$LOGPATH/$LOGFILENAME"
