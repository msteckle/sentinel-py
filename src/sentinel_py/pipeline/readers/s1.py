"""Sentinel-1 GRD source discovery for SNAP-based processing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel

from sentinel_py.enums import ASFOrbitDirection, S1OrbitPolicy
from sentinel_py.pipeline.readers.base import BaseReader


class _S1ReaderConfig(BaseModel):
    """Marker model for the built-in local Sentinel-1 GRD reader."""


class S1Reader(BaseReader):
    """Reader adapter for ASF-downloaded Sentinel-1 GRD ZIP archives.

    Sentinel-1 GRD archives are not read lazily. They are preprocessed scene by
    scene by SNAP (the ``snap_gpt`` processor), so this reader only validates the
    source configuration and provides the scene catalog.
    """

    source_type = "s1.grd.local"
    config_model = _S1ReaderConfig

    def parse_config(self, source_id: str, raw: dict[str, Any], base_dir: Any) -> Any:
        """Parse and validate one ``s1.grd.local`` source mapping."""
        from sentinel_py.pipeline.config import (
            PipelineConfigError,
            S1SourceConfig,
            _only_keys,
            _required,
            _resolve_path,
        )

        location = f"sources.{source_id}"
        _only_keys(raw, {"type", "data_dir", "flight_direction", "orbit"}, location)
        data_dir = _resolve_path(
            _required(raw, "data_dir", location), base_dir, f"{location}.data_dir"
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"S1 source directory does not exist: {data_dir}")

        direction = raw.get("flight_direction", "BOTH")
        allowed_directions = {item.value for item in ASFOrbitDirection}
        if not isinstance(direction, str) or direction.upper() not in allowed_directions:
            raise PipelineConfigError(
                f"{location}.flight_direction must be one of: "
                + ", ".join(sorted(allowed_directions))
            )
        orbit = raw.get("orbit", S1OrbitPolicy.precise_or_restituted.value)
        allowed_orbits = {
            item.value for item in S1OrbitPolicy if item is not S1OrbitPolicy.none
        }
        if not isinstance(orbit, str) or orbit not in allowed_orbits:
            raise PipelineConfigError(
                f"{location}.orbit must be one of: " + ", ".join(sorted(allowed_orbits))
            )
        return S1SourceConfig(source_id, data_dir, direction.upper(), orbit)

    def scene_catalog(self, source: Any, **filters: Any) -> pd.DataFrame:
        """Return catalog rows, with staged orbit files, selected for one source.

        Parameters
        ----------
        source : S1SourceConfig
            Parsed source configuration.
        **filters
            Optional ``start``, ``end`` and ``geometry_wgs84`` arguments forwarded
            to :func:`sentinel_py.s1.catalog.select_scenes`.
        """
        from sentinel_py.s1.catalog import (
            attach_orbits,
            build_scene_catalog,
            select_scenes,
        )

        catalog = build_scene_catalog(Path(source.data_dir))
        selected = select_scenes(
            catalog, flight_direction=source.flight_direction, **filters
        )
        return attach_orbits(selected, Path(source.data_dir))

    def read_raster(self, *args: Any, **kwargs: Any) -> Any:
        """S1 raster execution is owned by the SNAP processor."""
        raise NotImplementedError(
            "Sentinel-1 GRD data is preprocessed by a SNAP node, not read directly"
        )

    def sample_points(self, *args: Any, **kwargs: Any) -> Any:
        """S1 point execution is owned by the SNAP processor."""
        raise NotImplementedError(
            "Sentinel-1 GRD data is preprocessed by a SNAP node, not read directly"
        )
