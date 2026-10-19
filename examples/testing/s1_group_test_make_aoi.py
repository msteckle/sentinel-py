"""Build a small, spatially clustered point AOI to test ``snap_gpt`` plot grouping.

Picks the plot with the most neighbours within ``--radius-km`` and keeps it plus its
nearest neighbours, so the test plots share Sentinel-1 scenes and fall into one or a
few grouping cells. Run from the examples directory:

    python s1_group_test_make_aoi.py \
        ../data/aois/pavc2_2010-2024_filtered-lt20m-mostrecent_tier03_2026-10-16.geojson \
        ../data/aois/s1_group_test_25.geojson
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from pyproj import CRS, Transformer


def main() -> None:
    """Write the clustered subset and print what was picked."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Point GeoJSON with all plots.")
    parser.add_argument("destination", type=Path, help="GeoJSON to write.")
    parser.add_argument("--count", type=int, default=25, help="Plots to keep.")
    parser.add_argument("--radius-km", type=float, default=25.0)
    args = parser.parse_args()

    collection = json.loads(args.source.read_text())
    features = [f for f in collection["features"] if f["geometry"]["type"] == "Point"]
    name = (collection.get("crs") or {}).get("properties", {}).get("name", "EPSG:4326")
    to_metres = Transformer.from_crs(CRS.from_user_input(name), "EPSG:3338", always_xy=True)
    x, y = to_metres.transform(
        [f["geometry"]["coordinates"][0] for f in features],
        [f["geometry"]["coordinates"][1] for f in features],
    )
    xy = np.column_stack([x, y])

    distances = np.hypot(xy[:, None, 0] - xy[None, :, 0], xy[:, None, 1] - xy[None, :, 1])
    centre = int(np.argmax((distances < args.radius_km * 1000).sum(axis=1)))
    keep = np.argsort(distances[centre])[: args.count]

    subset = {**collection, "features": [features[int(i)] for i in sorted(keep)]}
    args.destination.parent.mkdir(parents=True, exist_ok=True)
    args.destination.write_text(json.dumps(subset))
    reach = distances[centre, keep].max() / 1000
    print(
        f"wrote {len(keep)} plots to {args.destination}: "
        f"{int((distances[centre] < args.radius_km * 1000).sum())} plots lie within "
        f"{args.radius_km:g} km of the centre; the kept ones span {reach:.1f} km"
    )


if __name__ == "__main__":
    main()
