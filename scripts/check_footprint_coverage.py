from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely import wkt

# set paths
basedir = Path(__file__).parent.parent
gridcells_path = basedir / "data" / "aois" / "bioclimate_latlon_noglacier_grid.geojson"
registry_path = (
    basedir / "examples" / "cache" / "all_downloaded_images_w_footprints.parquet"
)

# read data
grid = gpd.read_file(gridcells_path).to_crs("EPSG:4326")
images = pd.read_parquet(registry_path)
footprints = (
    images.dropna(subset=["geofootprint"]).drop_duplicates(subset="safedir").copy()
)
footprints["geometry"] = footprints["geofootprint"].apply(wkt.loads)  # type: ignore
footprints = gpd.GeoDataFrame(footprints, geometry="geometry", crs="EPSG:4326")

# read AOI
aoi_path = (
    basedir / "data" / "aois" / "bioclimate_latlon" / "bioclimate_latlon_noglacier.shp"
)
aoi = gpd.read_file(aoi_path).to_crs("EPSG:4326")

# get overlap
hits = gpd.sjoin(footprints, grid[["geometry"]], how="inner", predicate="intersects")
counts = hits.groupby("index_right").size()
grid["n_scenes"] = grid.index.map(counts).fillna(0).astype(int)  # type: ignore

# choropleth map of footprints + AOI boundary
fig, ax = plt.subplots(figsize=(14, 8))

uncovered = grid[grid["n_scenes"] == 0]
covered = grid[grid["n_scenes"] > 0]

uncovered.plot(ax=ax, color="lightgrey", edgecolor="white", linewidth=0.2)
covered.plot(
    column="n_scenes",
    ax=ax,
    legend=True,
    cmap="magma",
    edgecolor="white",
    linewidth=0.2,
    legend_kwds={"label": "Scenes downloaded", "shrink": 0.6},
)
aoi.boundary.plot(ax=ax, color="black", linewidth=0.5)

ax.set_title(
    f"Downloaded scene coverage: {len(uncovered)}/{len(grid)} ({len(uncovered) / len(grid) * 100:.2f}%) cells uncovered"
)
plt.tight_layout()
plt.savefig("coverage.png", dpi=150)
