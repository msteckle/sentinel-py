#!/usr/bin/env bash
set -euo pipefail

# Paths
AOI="../data/aois/toolik_025_aoi.geojson"
LOGPATH="../data/logs/download"
LOGFILENAME=$(basename "$0")
OUTPATH="../data"
PIPELINE="$(dirname "$0")/s2_preprocess_pipeline.yaml"

# Re-used params
RES=20
YEARS="2023 2024"
SPERIOD=06-01
EPERIOD=08-31

################################
# S2
################################

# Set up user/password for CDSE
# Note: you need to have an account with CDSE to download S2 data
# export CDSE_USERNAME="<email>"
# export CDSE_PASSWORD_FILE="$HOME/.cdse/cdse_pw"  # ensure chmod 600 on this file or it won't read

# Query/Download all Sentinel-2 summer scenes for 2023–2024
sentinel-py cdse query \
  --aoi $AOI \
  --crs EPSG:4326 \
  --years "$YEARS" \
  --speriod "$SPERIOD" \
  --eperiod "$EPERIOD" \
  --product S2MSI2A \
  --log $LOGPATH/${LOGFILENAME}

sentinel-py cdse download \
  --mission S2 \
  --bands "B02 B03 B04 B05 B06 B07 B08 B8A B11 B12 SCL" \
  --outdir $OUTPATH/s2/raw \
  --res $RES \
  --config $HOME/.s5cfg \
  --log $LOGPATH/${LOGFILENAME}

sentinel-py run "$PIPELINE" \
  --log $LOGPATH/${LOGFILENAME}

################################
# S1
################################

# Set up user/password for ASF
# Note: you need to have an account with earthdata to download. You can do:

# cat > "$HOME/.earthdata.netrc" <<'EOF'
# machine urs.earthdata.nasa.gov
#     login YOUR_EARTHDATA_USERNAME
#     password YOUR_EARTHDATA_PASSWORD
# EOF
# chmod 600 "$HOME/.earthdata.netrc"

# And then set the --config flag to point to your .netrc file

# # Query/Download all Sentinel-1 summer scenes for 2023–2024
# sentinel-py asf query \
#   --aoi $AOI \
#   --years "$YEARS" \
#   --speriod "$SPERIOD" \
#   --eperiod "$EPERIOD" \

# sentinel-py asf download \
#   --outdir $OUTPATH/s1/raw \
#   --config $HOME/.earthdata.netrc \
#   --processes 8 \
  
