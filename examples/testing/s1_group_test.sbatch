#!/usr/bin/env bash
#SBATCH -p serial
#SBATCH -q normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH --mem=64g
#SBATCH -t 04:00:00
#SBATCH -J s1_group_test
#SBATCH -o %x_%j.out
set -euo pipefail

# Usage (from the examples directory):
#   sbatch s1_group_test.sbatch baseline     # one gpt run per plot
#   sbatch s1_group_test.sbatch grouped      # one gpt run per scene and 25 km cell
# Then compare:  python s1_group_test_compare.py
MODE="${1:?usage: sbatch s1_group_test.sbatch baseline|grouped}"
case "$MODE" in
  baseline) GROUP=0 ;;
  grouped) GROUP=25 ;;
  *) echo "mode must be baseline or grouped" >&2; exit 2 ;;
esac

cd "${SLURM_SUBMIT_DIR:?run with sbatch}"

# Compute nodes don't inherit the login shell's PATH setup
source /projects/hpcl-cli185/proj-shared/sentinel-py/.venv/bin/activate
export SNAP_HOME=/projects/hpcl-cli185/proj-shared/sentinel-py/.snap-install
which sentinel-py
"$SNAP_HOME/bin/gpt" --diag | head -n 5

LOGPATH="../data/logs/processing"
mkdir -p "$LOGPATH"
export TMPDIR="${TMPDIR:-/tmp}"

# Paths in a pipeline resolve against its own folder, so write the filled-in copy here.
PIPELINE=".s1_group_test_${MODE}.yaml"
sed -e "s/__MODE__/${MODE}/g" -e "s/__GROUP__/${GROUP}/g" s1_group_test.yaml.tmpl > "$PIPELINE"

START=$(date +%s)
sentinel-py run "$PIPELINE" --log "$LOGPATH/s1_group_test_${MODE}.log"
echo "mode=${MODE} group_cell_km=${GROUP} wall_seconds=$(( $(date +%s) - START ))"
