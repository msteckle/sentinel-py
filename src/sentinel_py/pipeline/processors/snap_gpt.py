"""Sentinel-1 GRD preprocessing by SNAP ``gpt``, one scene per task.

The node runs a graph (calibration, terrain correction, ...) with the external SNAP
executable for every selected scene and AOI unit, then exposes the cached GeoTIFFs
as a lazy dataset on the pipeline's canonical grid. Scene runs are Dask tasks inside
the shared graph for raster AOIs, so they spread over workers and resume from the
per-scene cache; point AOIs run the same cached scene jobs and sample one pixel.
"""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pyproj import CRS

import sentinel_py.pipeline.grid as pipeline_grid
from sentinel_py.cache import REGISTRY
from sentinel_py.download.s1_orbits import parse_scene_name
from sentinel_py.download.snap_auxdata import EGM96_RELPATH
from sentinel_py.enums import SourceType
from sentinel_py.pipeline import snap
from sentinel_py.pipeline.artifacts import PointSampleArtifact, RasterArtifact
from sentinel_py.pipeline.grid import ChunkShape, grid_for_unit, point_grid
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
    make_recipe_id,
)
from sentinel_py.pipeline.readers.base import apply_polygon_mask, point_row
from sentinel_py.pipeline.readers.s1 import (
    attach_orbits,
    build_scene_catalog,
    select_scenes,
)

# Bump when the produced backscatter values change (built-in graph, grid handling,
# nodata convention) so cached recipes and COG resume keys are invalidated.
PROCESSOR_VERSION = 1

_ALLOWED_ORBITS = {
    "precise": {"POEORB"},
    "restituted": {"RESORB"},
    "precise-or-restituted": {"POEORB", "RESORB"},
}


########################################################################################
# Node configuration
########################################################################################


class SnapGPTNodeConfig(BaseModel):
    """Validated parameters of the ``snap_gpt`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1)
    dem: str = Field(min_length=1)
    graph: str = Field(default="builtin:grd_sigma0", min_length=1)
    polarizations: tuple[Literal["VV", "VH", "HH", "HV"], ...] = Field(
        default=("VV", "VH"), min_length=1
    )
    dem_vertical_datum: Literal["ellipsoid", "egm96"] = "ellipsoid"
    subset: Literal["aoi", "scene"] = "aoi"
    subset_margin_m: float = Field(default=2000.0, ge=0)
    dem_margin_m: float = Field(default=15000.0, ge=0)
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    work_dir: str | None = None
    gpt: str | None = None
    memory_gb: int = Field(default=16, ge=1)
    threads: int = Field(default=4, ge=1)
    timeout_s: int | None = Field(default=None, gt=0)

    @field_validator("polarizations", mode="before")
    @classmethod
    def validate_polarizations(cls, value: Any) -> tuple[str, ...]:
        """Accept a YAML list and normalize to unique, upper-case polarizations."""
        if not isinstance(value, (list, tuple)):
            raise TypeError("polarizations must be a list")
        return tuple(dict.fromkeys(str(item).upper() for item in value))


########################################################################################
# Scene selection and job construction
########################################################################################


def _parameter_items(parameters: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    """Return extra graph variables as sorted, command-line-ready strings."""
    return tuple(
        sorted(
            (name, str(value).lower() if isinstance(value, bool) else str(value))
            for name, value in parameters.items()
        )
    )


def _unit_jobs(
    unit: Any,
    catalog: pd.DataFrame,
    *,
    config: SnapGPTNodeConfig,
    pipeline: Any,
    source: Any,
    dem_tiles: tuple[Path, ...],
    graph: tuple[Path, str],
    gpt: str,
    version: str,
    work_dir: Path,
) -> list[tuple[Any, snap.SceneJob]]:
    """Select scenes for one AOI unit and build one SNAP job per scene.

    Raises
    ------
    RuntimeError
        If no scene intersects the unit, a selected scene lacks an acceptable staged
        orbit or a requested polarization, or the DEM does not cover its region.
    """
    from shapely import wkt

    selection = pipeline.selection
    scenes = select_scenes(
        catalog,
        geometry_wgs84=unit.geometry_wgs84,
        flight_direction=source.flight_direction,
        years=selection.years,
        speriod=selection.start_period,
        eperiod=selection.end_period,
    )
    if scenes.empty:
        raise RuntimeError(f"Found 0 Sentinel-1 scenes for AOI {unit.aoi_id}")

    allowed = _ALLOWED_ORBITS[source.orbit]
    no_orbit = scenes[~scenes["orbit_type"].isin(allowed)]
    if not no_orbit.empty:
        raise RuntimeError(
            f"{len(no_orbit)} scene(s) for AOI {unit.aoi_id} have no staged "
            f"{source.orbit} orbit file (e.g. {no_orbit['scene'].iloc[0]}); run "
            "'sentinel-py asf download' on a node with network access first"
        )
    wanted = set(config.polarizations)
    lacking = scenes[
        [not wanted <= set(pols.split(",")) for pols in scenes["polarizations"]]
    ]
    if not lacking.empty:
        raise RuntimeError(
            f"Scene(s) lack requested polarization(s) {sorted(wanted)}: "
            + ", ".join(lacking["scene"].head(3))
        )

    egm96 = Path(source.data_dir) / EGM96_RELPATH
    resolution, anchor = pipeline.output_grid.resolution, pipeline.output_grid.anchor
    origin = (anchor[0] + resolution[0] / 2, anchor[1] + resolution[1] / 2)
    jobs = []
    for row in scenes.itertuples():
        footprint = wkt.loads(row.footprint_wkt)
        if config.subset == "scene":
            region = footprint.bounds
        else:
            region = unit.geometry_wgs84.intersection(footprint).bounds
            region = snap.expand_bounds(region, config.subset_margin_m)
        dem_bounds = snap.expand_bounds(region, config.dem_margin_m)
        tiles = snap.select_dem_tiles(dem_tiles, dem_bounds)
        if not snap.dem_covers(tiles, region):
            raise RuntimeError(
                f"DEM source {config.dem!r} does not cover the processing region of "
                f"scene {row.scene} (lon/lat bounds {region}); add ArcticDEM tiles "
                "for it or narrow the AOI"
            )
        times = parse_scene_name(row.scene)
        assert times is not None  # catalog rows come from validated scene names
        jobs.append(
            (
                row,
                snap.SceneJob(
                    scene=row.scene,
                    zip_path=str(Path(source.data_dir) / row.zip_name),
                    orbit_path=str(row.orbit_path),
                    orbit_kind=str(row.orbit_type),
                    platform=times.platform,
                    acquired=times.start.strftime("%Y%m%dT%H%M%S"),
                    egm96_path=str(egm96),
                    region_wkt=snap.region_polygon_wkt(region),
                    dem_tiles=tuple(str(tile) for tile in tiles),
                    dem_bounds_wgs84=dem_bounds,
                    dem_apply_egm=config.dem_vertical_datum == "egm96",
                    polarizations=config.polarizations,
                    crs=pipeline.output_grid.crs,
                    pixel_spacing=float(resolution[0]),
                    grid_origin=origin,
                    graph_path=str(graph[0]),
                    graph_sha256=graph[1],
                    parameters=_parameter_items(config.parameters),
                    work_dir=str(work_dir),
                    gpt=gpt,
                    snap_version=version,
                    memory_gb=config.memory_gb,
                    threads=config.threads,
                    timeout_s=config.timeout_s,
                ),
            )
        )
    return jobs


########################################################################################
# Lazy reads of per-scene outputs
########################################################################################


def _read_window(
    path: str, grid: Any, window: Any, bands: int
) -> np.ndarray:
    """Read one canonical-grid window of a scene output as float32 with NaN nodata.

    Parameters
    ----------
    path : str
        Terrain-corrected GeoTIFF produced by :func:`snap.run_scene`.
    grid : GridSpec
        Canonical grid of the AOI unit.
    window : GridWindow
        Output window to produce.
    bands : int
        Number of polarization bands in the file.

    Returns
    -------
    numpy.ndarray
        Array of shape ``(bands, window.height, window.width)``.
    """
    import rasterio
    from rasterio.windows import Window

    output = np.full((bands, window.height, window.width), np.nan, dtype=np.float32)
    with pipeline_grid.RASTERIO_LOCK:  # module access keeps the lock unpickled
        with rasterio.open(path) as source:
            found = pipeline_grid.native_pixel_window(source, window.bounds, grid.crs)
            if found is None:
                return output
            x_offset, y_offset, width, height = found
            raster_window = Window(x_offset, y_offset, width, height)
            transform = source.window_transform(raster_window)
            resolution = abs(float(source.transform.a))
            for index in range(bands):
                values = source.read(index + 1, window=raster_window).astype(np.float32)
                output[index] = pipeline_grid.warp_to_window(
                    values,
                    transform,
                    source.crs,
                    grid,
                    window,
                    resampling=pipeline_grid.select_resampling(resolution, grid),
                    src_nodata=snap.OUTPUT_NODATA,
                    dst_nodata=np.nan,
                    dtype=np.float32,
                )
    return output


def _lazy_dataset(
    jobs: list[tuple[Any, snap.SceneJob]], grid: Any, polarizations: tuple[str, ...]
) -> Any:
    """Build the unit's dataset with each scene's SNAP run as a Dask task."""
    import dask.array as da
    import xarray as xr
    from dask import delayed

    bands = len(polarizations)
    windows = tuple(grid.iter_windows())
    scene_arrays = []
    for _, job in jobs:
        path = delayed(snap.run_scene, pure=True)(job)
        rows = []
        for row_index in range(grid.chunk_grid_shape[0]):
            blocks = [
                da.from_delayed(
                    delayed(_read_window, pure=True)(path, grid, window, bands),
                    shape=(bands, window.height, window.width),
                    dtype=np.float32,
                )
                for window in windows
                if window.row_index == row_index
            ]
            rows.append(da.concatenate(blocks, axis=2))
        scene_arrays.append(da.concatenate(rows, axis=1))

    x = grid.bounds[0] + (np.arange(grid.width) + 0.5) * grid.resolution[0]
    y = grid.bounds[3] - (np.arange(grid.height) + 0.5) * grid.resolution[1]
    rows = [row for row, _ in jobs]
    times = pd.to_datetime([r.start_time for r in rows], utc=True).tz_localize(None)
    return xr.Dataset(
        data_vars={
            "backscatter": (
                ("time", "band", "y", "x"),
                da.stack(scene_arrays, axis=0).rechunk({1: -1}),
                {
                    "long_name": "terrain-corrected sigma0 backscatter, linear power",
                    "nodata": float("nan"),
                },
            )
        },
        coords={
            "time": times.values.astype("datetime64[ns]"),
            "band": list(polarizations),
            "y": y,
            "x": x,
            "product_id": ("time", [r.scene for r in rows]),
            "granule_id": ("time", [r.scene for r in rows]),
            "source_fingerprint": ("time", [job.key() for _, job in jobs]),
            "flight_direction": ("time", [r.flight_direction for r in rows]),
            "relative_orbit": ("time", [int(r.relative_orbit) for r in rows]),
            "orbit_type": ("time", [str(r.orbit_type) for r in rows]),
        },
        attrs={
            "crs": grid.crs,
            "transform": tuple(grid.affine[:6]),
            "bounds": grid.bounds,
            "nodata": float("nan"),
        },
    )


########################################################################################
# Processor class for snap_gpt
########################################################################################


class SnapGPTProcessor(BaseProcessor):
    """Run a SNAP graph per Sentinel-1 scene and expose lazy backscatter."""

    type_name = "snap_gpt"
    config_model = SnapGPTNodeConfig
    version = PROCESSOR_VERSION

    def validate(
        self,
        config: ProcessorConfig,
        *,
        node_id: str,
        inputs: Mapping[str, str],
        sources: Mapping[str, Any],
        outputs: tuple[Any, ...],
        output_grid: Any,
    ) -> None:
        """Validate source references, the output grid, and the graph variables."""
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, SnapGPTNodeConfig):
            raise TypeError(f"{node_id} has an invalid snap_gpt configuration")
        if inputs:
            raise ValueError("snap_gpt does not accept node inputs")
        for key, expected in (
            (config.source, SourceType.s1_grd_local),
            (config.dem, SourceType.dem_local),
        ):
            source = sources.get(key)
            if source is None:
                raise ValueError(f"{node_id} references unconfigured source {key!r}")
            if getattr(source, "source_type", None) != expected:
                raise ValueError(f"{node_id} source {key!r} must be {expected}")
        if output_grid.chunks is None:
            raise ValueError("snap_gpt requires a canonical output grid")
        crs = CRS.from_user_input(output_grid.crs)
        if not crs.is_projected or any(
            abs(axis.unit_conversion_factor - 1.0) > 1e-12 for axis in crs.axis_info
        ):
            raise ValueError("snap_gpt requires a projected metre output CRS")
        if output_grid.resolution[0] != output_grid.resolution[1]:
            raise ValueError("snap_gpt requires square output pixels")
        overlap = set(config.parameters) & snap.RESERVED_VARIABLES
        if overlap:
            raise ValueError(
                f"{node_id}.parameters cannot override reserved variable(s): "
                + ", ".join(sorted(overlap))
            )
        graph = config.graph
        # Relative custom graphs resolve against the pipeline file at execution.
        if graph.startswith(snap.BUILTIN_GRAPH_PREFIX) or Path(graph).is_absolute():
            path, _ = snap.load_graph(graph)
            self._check_graph_variables(node_id, path, config)

    @staticmethod
    def _check_graph_variables(
        node_id: str, path: Path, config: SnapGPTNodeConfig
    ) -> None:
        """Require every graph variable to be reserved or supplied, and vice versa."""
        used = snap.graph_variables(path)
        unknown = used - snap.RESERVED_VARIABLES - set(config.parameters)
        if unknown:
            raise ValueError(
                f"{node_id}: graph variable(s) {sorted(unknown)} are neither "
                "provided by sentinel-py nor set under 'parameters'"
            )
        unused = set(config.parameters) - used
        if unused:
            raise ValueError(
                f"{node_id}.parameters not used by the graph: {sorted(unused)}"
            )

    def _prepare(self, config: SnapGPTNodeConfig, context: ProcessorContext) -> dict:
        """Resolve gpt, graph, work directory, DEM tiles, and the scene catalog."""
        pipeline = context.pipeline
        source = pipeline.sources[config.source]
        dem = pipeline.sources[config.dem]
        gpt = snap.resolve_gpt(config.gpt)
        version = snap.snap_version(str(gpt))
        base = Path(pipeline.path).parent
        graph = snap.load_graph(config.graph, base)
        self._check_graph_variables(context.node.node_id, graph[0], config)
        if config.work_dir is None:
            work_dir = REGISTRY["snap_outputs"].path(source.data_dir)
        else:
            work_dir = (base / config.work_dir).resolve()
        catalog = attach_orbits(build_scene_catalog(source.data_dir), source.data_dir)
        context.logger.info(
            "snap_gpt node=%s gpt=%s snap=%s graph=%s work_dir=%s",
            context.node.node_id, gpt, version, graph[0].name, work_dir,
        )
        return {
            "source": source,
            "gpt": str(gpt),
            "version": version,
            "graph": graph,
            "work_dir": work_dir,
            "catalog": catalog,
            "dem_tiles": tuple(sorted(Path(dem.data_dir).glob(dem.pattern))),
        }

    def _jobs(self, unit: Any, config: SnapGPTNodeConfig, context: ProcessorContext, state: dict):
        """Build the (catalog row, job) pairs for one AOI unit."""
        return _unit_jobs(
            unit, state["catalog"], config=config, pipeline=context.pipeline,
            source=state["source"], dem_tiles=state["dem_tiles"], graph=state["graph"],
            gpt=state["gpt"], version=state["version"], work_dir=state["work_dir"],
        )

    def _recipe(self, config: SnapGPTNodeConfig, state: dict, grid: Any) -> str:
        """Return the recipe ID covering every result-affecting node setting."""
        return make_recipe_id(
            self.type_name,
            self.version,
            graph=state["graph"][1],
            snap=state["version"],
            polarizations=config.polarizations,
            datum=config.dem_vertical_datum,
            subset=config.subset,
            subset_margin_m=config.subset_margin_m,
            dem_margin_m=config.dem_margin_m,
            parameters=_parameter_items(config.parameters),
            orbit=state["source"].orbit,
            flight_direction=state["source"].flight_direction,
            crs=grid.crs,
            transform=tuple(grid.affine[:6]),
        )

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Build lazy per-unit backscatter datasets; SNAP runs as Dask tasks."""
        if not isinstance(config, SnapGPTNodeConfig) or inputs:
            raise TypeError("Invalid snap_gpt configuration or inputs")
        pipeline = context.pipeline
        state = self._prepare(config, context)
        datasets = {}
        for unit in pipeline.aoi_plan.units:
            grid = grid_for_unit(unit, pipeline.output_grid)
            jobs = self._jobs(unit, config, context, state)
            dataset = _lazy_dataset(jobs, grid, config.polarizations)
            dataset.attrs.update(
                pipeline=str(pipeline.path), processor=self.type_name,
                node=context.node.node_id, time_representation="acquisition",
                recipe_id=self._recipe(config, state, grid),
            )
            datasets[(unit.aoi_id, unit.component_id)] = apply_polygon_mask(
                dataset, unit
            )
        context.logger.info(
            "Building lazy node=%s AOI units=%d scenes=%d",
            context.node.node_id, len(datasets),
            sum(ds.sizes["time"] for ds in datasets.values()),
        )
        return RasterArtifact(pipeline.aoi_plan, datasets)

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Run the cached scene jobs and sample one pixel per point and scene.

        SNAP runs on the client with ``execution.workers`` concurrent ``gpt``
        processes, matching how the Sentinel-2 point path samples on the client.
        """
        if not isinstance(config, SnapGPTNodeConfig) or inputs:
            raise TypeError("Invalid snap_gpt configuration or inputs")
        pipeline = context.pipeline
        state = self._prepare(config, context)
        per_unit = [
            (unit, self._jobs(unit, config, context, state))
            for unit in pipeline.aoi_plan.units
        ]
        unique = {job.key(): job for _, jobs in per_unit for _, job in jobs}
        context.logger.info(
            "snap_gpt points=%d scene runs=%d workers=%d",
            len(per_unit), len(unique), pipeline.execution.workers,
        )
        with ThreadPoolExecutor(max_workers=max(1, pipeline.execution.workers)) as pool:
            outputs = dict(
                zip(unique, pool.map(snap.run_scene, unique.values()), strict=True)
            )

        rows = []
        for unit, jobs in per_unit:
            grid = point_grid(
                unit, crs=pipeline.output_grid.crs,
                resolution=pipeline.output_grid.resolution,
                anchor=pipeline.output_grid.anchor, chunks=ChunkShape(y=1, x=1),
            )
            window = next(grid.iter_windows())
            for scene_row, job in jobs:
                values = _read_window(
                    outputs[job.key()], grid, window, len(config.polarizations)
                )[:, 0, 0]
                row = point_row(
                    unit, time=job.acquired, product_id=job.scene,
                    granule_id=job.scene, time_representation="acquisition",
                    source_fingerprint=job.key(),
                )
                row["flight_direction"] = scene_row.flight_direction
                row["relative_orbit"] = int(scene_row.relative_orbit)
                row["orbit_type"] = str(scene_row.orbit_type)
                row["units"] = "linear"
                row.update(
                    {pol: float(v) for pol, v in zip(config.polarizations, values, strict=True)}
                )
                rows.append(row)
        return PointSampleArtifact(pd.DataFrame(rows), pipeline.aoi_plan)

