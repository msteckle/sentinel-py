#!/usr/bin/env bash
set -euo pipefail

LOGPATH="../data/logs/download"
LOGFILENAME=$(basename "$0")
PIPELINE="$(dirname "$0")/toolik025_jun01-aug31_2023-2024_processing.yaml"

sentinel-py run "$PIPELINE" \
  --log $LOGPATH/${LOGFILENAME}
  
