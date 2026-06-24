"""
Single-gridcell S2 pipeline on openEO/CDSE for credit-cost measurement.

Pipeline:
  load SENTINEL2_L2A -> SCL mask -> resample to 20 m -> TIFF

Goal: measure actual credit cost for ONE gridcell so I can extrapolate to the full
pan-Arctic grid.
"""

from functools import reduce
from operator import or_
from pathlib import Path

import geopandas as gpd
import openeo

# --------------------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------------------

# cdse openeo endpoint
BACKEND = "openeofed.dataspace.copernicus.eu"

# aoi to narrow down search
datadir = Path("/mnt/poseidon/remotesensing/arctic/data/vectors/supplementary")
gridcells = datadir / "tundra_alaska_grid_latlon_aoi" / "tundra_alaska_grid_aoi.shp"
gc_gdf = gpd.read_file(gridcells)
gc_1096 = gc_gdf[gc_gdf["FID"] == 1096]
GC_1096_GEOM = gc_1096.geometry.values[0]

# other config for narrowing down search
YEARS = range(2019, 2025)
DATA_BANDS = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]
SCL_MASK_BITS = [0, 1, 2, 3, 7, 8, 9, 10, 11]  # keep 4=veg, 5=bare, 6=water
MAX_CLOUD_COVER = 99  # drops scenes >99% cloud

# outputs
OUT_DIR = Path("/mnt/poseidon/remotesensing/6ru/sentinel-py/data/openeo")
JOB_TITLE = "single-gridcell credit test"

# --------------------------------------------------------------------------------------
# CONNECT to backend
# --------------------------------------------------------------------------------------

connection = openeo.connect(BACKEND).authenticate_oidc()

# --------------------------------------------------------------------------------------
# LOAD one datacube per summer, then merge
# --------------------------------------------------------------------------------------


def summer_cube(year: int):
    return connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=GC_1096_GEOM,
        temporal_extent=[f"{year}-06-01", f"{year}-09-01"],  # e.g., [Jun 1, Sep 1)
        bands=DATA_BANDS + ["SCL"],
        max_cloud_cover=MAX_CLOUD_COVER,
    )


cube = reduce(
    lambda a, b: a.merge_cubes(b, overlap_resolver="max"),
    (summer_cube(y) for y in YEARS),
)

# --------------------------------------------------------------------------------------
# MASK using SCL band
# --------------------------------------------------------------------------------------

scl = cube.band("SCL")
flagged = reduce(or_, [scl == v for v in SCL_MASK_BITS])
cube = cube.mask(flagged)
cube = cube.filter_bands(DATA_BANDS)  # type: ignore[arg-type]

# --------------------------------------------------------------------------------------
# RESAMPLE to 20 m in native UTM
# --------------------------------------------------------------------------------------

cube = cube.resample_spatial(resolution=20, method="average")
cube = cube.save_result(format="GTiff")

# --------------------------------------------------------------------------------------
# CREATE JOB + ESTIMATE (free, doesn't start processing)
# --------------------------------------------------------------------------------------

# check the process graph is valid before creating the job
errors = cube.validate()
assert not errors, errors

# create the job (doesn't start processing yet)
job = cube.create_job(title=JOB_TITLE)
print(f"\nJob created: {job.job_id}")
print("View at: https://openeo.dataspace.copernicus.eu/?discover=0\n")

# --------------------------------------------------------------------------------------
# CONFIRM, RUN, REPORT ACTUAL USAGE
# --------------------------------------------------------------------------------------

if input("\nStart the job now? [y/N]: ").strip().lower() != "y":
    print("Job created but not started. To run it later:")
    print(f'  job = connection.job("{job.job_id}")')
    print("  job.start_and_wait()")
    raise SystemExit

# start the job and wait for it to finish
job.start_and_wait()

# download results
Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
job.get_results().download_files(OUT_DIR)
print(f"\nResults downloaded to: {OUT_DIR}")

# the real test: how many credits did it actually cost?
info = job.describe()
print("\n=== ACTUAL USAGE ===")
print(f"Status : {info.get('status')}")
print(f"Costs  : {info.get('costs')} credits")
print(f"Usage  : {info.get('usage')}")
print("\nExtrapolate: total_credits_for_panarctic ≈ above x (N_gridcells)")
print("Apply ~0.7x for larger-job efficiency if your full run will be one big job,")
print("or ~1.0x if you'll submit one job per gridcell (+2 credits overhead each).")
