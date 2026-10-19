#!/usr/bin/env bash
#SBATCH -p serial
#SBATCH -q normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH --mem=128g
#SBATCH -t 24:00:00
#SBATCH -J pavc_rf19
#SBATCH -o %x_%j.out
set -euo pipefail

# Usage (from the examples directory):
#   sbatch pavc_gridded_rf_points_2019_hpc_training.sbatch
cd "${SLURM_SUBMIT_DIR:?run with sbatch}"

# Compute nodes don't inherit the login shell's PATH setup
source /projects/hpcl-cli185/proj-shared/sentinel-py/.venv/bin/activate
export SNAP_HOME=/projects/hpcl-cli185/proj-shared/sentinel-py/.snap-install
which sentinel-py
"$SNAP_HOME/bin/gpt" --diag | head -n 5

LOGPATH="../data/logs/processing"
LOGFILENAME="pavc_gridded_rf_points_2019_hpc_training.sbatch"
PIPELINE="pavc_gridded_rf_points_2019_hpc_training.yaml"
mkdir -p "$LOGPATH"

# Keep the Dask/SNAP scratch on node-local or project storage, not $HOME
export TMPDIR="${TMPDIR:-/tmp}"

sentinel-py run "$PIPELINE" --log "$LOGPATH/$LOGFILENAME"
