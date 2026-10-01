from __future__ import annotations

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
