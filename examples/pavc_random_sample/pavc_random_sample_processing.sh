#!/usr/bin/env bash
set -euo pipefail

LOGPATH="../data/logs/download"
LOGFILENAME=$(basename "$0")
PIPELINE="$(dirname "$0")/pavc_random_sample_processing.yaml"

sentinel-py run "$PIPELINE" \
  --log $LOGPATH/${LOGFILENAME}