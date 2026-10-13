from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from sentinel_py.pipeline.processors.dem import (
    DEMFeature,
    DEMNodeConfig,
    DEMProcessor,
)


def test_dem_features_are_registered_and_configured() -> None:
    assert set(("elevation", "aspect", "slope", "hillshade")) <= set(DEMFeature.registry)
    config = DEMNodeConfig(source="arcticdem", features=["ELEVATION", "slope"])
    assert config.features == ("elevation", "slope")
    assert config.algorithm == "Horn"


def test_dem_config_rejects_duplicate_or_unknown_features() -> None:
    with pytest.raises(ValueError, match="duplicates"):
        DEMNodeConfig(source="dem", features=["slope", "SLOPE"])
    with pytest.raises(ValueError, match="unsupported"):
        DEMNodeConfig(source="dem", features=["roughness"])


def test_flat_dem_has_zero_slope_and_nodata_aspect() -> None:
    elevation = np.ones((5, 5), dtype=np.float32)
    slope = DEMFeature.registry["slope"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    aspect = DEMFeature.registry["aspect"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    assert np.allclose(slope, 0)
    assert np.isnan(aspect).all()


def test_horn_slope_matches_planar_surface() -> None:
    # z increases 10 m for every 10 m eastward pixel: 45 degrees.
    elevation = np.tile(np.arange(5, dtype=np.float32) * 10, (5, 1))
    slope = DEMFeature.registry["slope"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    aspect = DEMFeature.registry["aspect"].calculate(
        elevation, x_resolution=10.0, y_resolution=10.0, algorithm="Horn"
    )
    assert np.allclose(slope, 45.0)
    assert np.allclose(aspect, 90.0)


def test_builtin_registry_contains_dem_processor() -> None:
    from sentinel_py.pipeline.processors.base import get_default_registry

    assert isinstance(get_default_registry().get("dem"), DEMProcessor)


def test_select_dem_paths_keeps_only_tiles_intersecting_the_aoi_component(tmp_path):
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin
    from shapely.geometry import box

    from sentinel_py.pipeline.grid import AOIUnit
    from sentinel_py.pipeline.readers.arcticdem import select_dem_paths
    from types import SimpleNamespace

    def tile(name: str, west: float) -> Path:
        path = tmp_path / name
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=10,
            height=10,
            count=1,
            dtype="float32",
            crs="EPSG:32606",
            transform=from_origin(west, 1000.0, 10.0, 10.0),
        ) as dataset:
            dataset.write(np.ones((1, 10, 10), dtype=np.float32))
        return path

    near = tile("near.tif", 0.0)
    far = tile("far.tif", 50_000.0)
    unit = AOIUnit(
        aoi_id="site",
        component_id="part-0000",
        kind="polygon",
        geometry=box(20.0, 920.0, 60.0, 980.0),
        geometry_wgs84=box(0, 0, 1, 1),
        grid=None,
    )

    selected = select_dem_paths(
        (near, far), unit, grid=SimpleNamespace(crs="EPSG:32606")
    )

    assert selected == (near,)
