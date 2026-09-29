"""Preprocess Sentinel-2 data with lazy offset-corrected and SCL-masked VRTs."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

import pandas as pd

from sentinel_py.cache import (
    deterministic_cache_key,
    merge_state_rows,
    write_parquet_atomic,
)
from sentinel_py.s2.base import S2Granule, S2PreprocessConfig, S2PreprocessResult
from sentinel_py.s2.discover import read_l2a_radiometry, read_l2a_special_values

PREPROCESS_ALGORITHM_VERSION = 4
PREPROCESS_STATE_NAME = "s2_preprocess.parquet"


class _RasterGrid(TypedDict):
    """Spatial properties needed to align preprocessing inputs."""

    x_size: int
    y_size: int
    transform: tuple[float, float, float, float, float, float]
    projection: str
    bounds: tuple[float, float, float, float]
    x_resolution: float
    y_resolution: float


########################################################################################
# GDAL capabilities and preprocessing recipes
########################################################################################


def _import_error_chain(error: ImportError) -> str:
    """Return concise details from every exception in an import failure chain."""
    details = []
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        details.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(details)


def _gdal_import_error(error: ImportError) -> RuntimeError:
    """Build an actionable error for missing or unloadable GDAL bindings."""
    details = _import_error_chain(error)
    linker_failure = any(
        marker in details
        for marker in (
            "Library not loaded",
            "cannot open shared object file",
            "image not found",
            "Symbol not found",
        )
    )
    if linker_failure:
        fallback_path = os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", "<unset>")
        return RuntimeError(
            "GDAL Python bindings are installed, but the native GDAL library could "
            "not be loaded. On macOS, load your shell profile before running the "
            "command (for example, 'source ~/.bash_profile') and verify that "
            "DYLD_FALLBACK_LIBRARY_PATH includes the GDAL lib directory. "
            f"Current DYLD_FALLBACK_LIBRARY_PATH: {fallback_path}. "
            f"Import details: {details}"
        )

    # Distinguish an absent Python package from an installed package that cannot load.
    if "No module named 'osgeo'" in details or 'No module named "osgeo"' in details:
        return RuntimeError(
            "GDAL Python bindings are not installed in the active Python environment. "
            "Install the processing dependencies with 'uv sync --extra processing', "
            "then verify with 'python -c \"from osgeo import gdal; "
            "print(gdal.VersionInfo())\"'. "
            f"Import details: {details}"
        )
    return RuntimeError(
        "GDAL Python bindings could not be imported. Confirm that the Python GDAL "
        "package matches the native gdal-config version. "
        f"Import details: {details}"
    )


def _gdal():
    """Import GDAL only when preprocessing is invoked and enable exceptions."""
    try:
        from osgeo import gdal
    except ImportError as error:
        raise _gdal_import_error(error) from error
    gdal.UseExceptions()
    return gdal


def validate_gdal_for_preprocessing() -> str:
    """Verify that GDAL provides the native muparser VRT expression dialect.

    Returns
    -------
    str
        The installed GDAL release name.

    Examples
    --------
    ``version = validate_gdal_for_preprocessing()``
    """
    gdal = _gdal()
    if not hasattr(gdal, "Run"):
        raise RuntimeError(
            "This GDAL build does not provide the unified Python algorithm API. "
            "GDAL 3.11 or newer is required for 's2 preprocess'."
        )
    try:
        gdal.Algorithm("raster", "calc")
    except Exception as error:
        raise RuntimeError(
            "This GDAL build does not provide the 'raster calc' algorithm."
        ) from error
    vrt_driver = gdal.GetDriverByName("VRT")
    dialects = vrt_driver.GetMetadataItem("ExpressionDialects") if vrt_driver else None
    supported = {value.strip() for value in (dialects or "").split(",")}
    if "muparser" not in supported:
        raise RuntimeError(
            "This GDAL build does not provide the muparser VRT expression dialect. "
            "Rebuild GDAL with muparser before running 'sentinel-py s2 preprocess'."
        )
    return gdal.VersionInfo("RELEASE_NAME")


def preprocess_recipe_id(config: S2PreprocessConfig) -> str:
    """Return the deterministic identifier for a preprocessing configuration."""
    payload = {
        "algorithm_version": PREPROCESS_ALGORITHM_VERSION,
        "bands": config.bands,
        "resolution_m": config.resolution_m,
        "mask_classes": config.mask_classes,
        "nodata": config.nodata,
    }
    return deterministic_cache_key(payload)[:12]


########################################################################################
# Preprocessing state and source fingerprints
########################################################################################


def preprocess_state_path(output_dir: Path) -> Path:
    """Return the output-scoped Sentinel-2 preprocessing state path."""
    return Path(output_dir) / ".sentinel-py" / PREPROCESS_STATE_NAME


def read_preprocess_state(output_dir: Path) -> pd.DataFrame:
    """Read preprocessing state, returning an empty frame before the first run."""
    path = preprocess_state_path(output_dir)
    return pd.read_parquet(path) if path.is_file() else pd.DataFrame()


def write_preprocess_results(
    output_dir: Path,
    existing_state: pd.DataFrame,
    results: list[S2PreprocessResult],
) -> Path:
    """Merge worker results and atomically persist output-scoped state."""
    state = merge_state_rows(
        existing_state,
        (result.state_row for result in results),
        key_columns=["task_id"],
    )
    path = preprocess_state_path(output_dir)
    write_parquet_atomic(state, path, index=False)
    return path


def _source_fingerprint(paths: list[Path]) -> str:
    """Capture enough source identity to invalidate stale preprocessing recipes."""
    values = []
    for path in sorted({Path(path).resolve() for path in paths}, key=str):
        stat = path.stat()
        values.append(
            {"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
        )
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


########################################################################################
# Raster grids for preprocessing
########################################################################################


def _dataset_grid(path: Path) -> _RasterGrid:
    """Read the spatial grid needed to safely assemble a native-CRS VRT."""
    gdal = _gdal()
    dataset = gdal.OpenEx(str(path), gdal.OF_RASTER | gdal.OF_READONLY)
    if dataset is None:
        raise RuntimeError(f"GDAL could not open raster source: {path}")
    transform = dataset.GetGeoTransform()
    if transform is None or transform[2] != 0 or transform[4] != 0:
        raise ValueError(f"Expected a north-up geotransform: {path}")
    x_size, y_size = dataset.RasterXSize, dataset.RasterYSize
    bounds = (
        transform[0],
        transform[3] + transform[5] * y_size,
        transform[0] + transform[1] * x_size,
        transform[3],
    )
    grid: _RasterGrid = {
        "x_size": x_size,
        "y_size": y_size,
        "transform": transform,
        "projection": dataset.GetProjectionRef(),
        "bounds": bounds,
        "x_resolution": abs(transform[1]),
        "y_resolution": abs(transform[5]),
    }
    dataset = None
    return grid


def _output_grid(grids: dict[str, _RasterGrid], resolution_m: int) -> _RasterGrid:
    """Validate common native geometry and derive the requested-resolution grid."""
    reference = min(
        grids.values(),
        key=lambda grid: abs(float(grid["x_resolution"]) - resolution_m),
    )
    reference_bounds = reference["bounds"]
    reference_projection = reference["projection"]
    if not reference_projection:
        raise ValueError("A source raster has no coordinate reference system")

    # Ensure no scene is silently shifted or reprojected during preprocessing.
    for name, grid in grids.items():
        bounds = grid["bounds"]
        if any(
            abs(left - right) > 0.1 for left, right in zip(bounds, reference_bounds)
        ):
            raise ValueError(f"Source extent does not match the granule grid: {name}")
        if grid["projection"] != reference_projection:
            raise ValueError(f"Source CRS does not match the granule grid: {name}")

    xmin, ymin, xmax, ymax = reference_bounds
    x_size = round((xmax - xmin) / resolution_m)
    y_size = round((ymax - ymin) / resolution_m)
    if x_size <= 0 or y_size <= 0:
        raise ValueError("Invalid Sentinel-2 output grid dimensions")
    return {
        "x_size": x_size,
        "y_size": y_size,
        "transform": (xmin, resolution_m, 0.0, ymax, 0.0, -resolution_m),
        "projection": reference_projection,
        "bounds": reference_bounds,
        "x_resolution": resolution_m,
        "y_resolution": resolution_m,
    }


########################################################################################
# Lazy VRT construction
########################################################################################


def _calculation_input(
    name: str,
    path: Path,
    source_grid: _RasterGrid,
    output_grid: _RasterGrid,
    resampling: str,
    *,
    input_nodata: int | None = None,
    output_nodata: int | None = None,
) -> str:
    """Return a named direct input or lazy alignment pipeline for raster calc."""
    source_path = Path(path).resolve()
    source_transform = tuple(float(value) for value in source_grid["transform"])
    output_transform = tuple(float(value) for value in output_grid["transform"])
    same_grid = (
        source_grid["x_size"] == output_grid["x_size"]
        and source_grid["y_size"] == output_grid["y_size"]
        and all(
            abs(source - output) < 1e-9
            for source, output in zip(source_transform, output_transform)
        )
    )
    if same_grid and input_nodata is None and output_nodata is None:
        return f"{name}={source_path}"

    # Ask GDAL to serialize an on-the-fly alignment pipeline into the output VRT.
    xmin, ymin, xmax, ymax = (float(value) for value in output_grid["bounds"])
    resolution = float(output_grid["x_resolution"])
    quoted_path = str(source_path).replace("\\", "\\\\").replace('"', '\\"')
    pipeline = (
        f'read "{quoted_path}" ! reproject '
        f"--resolution {resolution},{resolution} "
        f"--bbox {xmin},{ymin},{xmax},{ymax} --resampling {resampling}"
    )
    if input_nodata is not None:
        pipeline += f" --input-nodata {input_nodata}"
    if output_nodata is not None:
        pipeline += f" --output-nodata {output_nodata}"
    return f"{name}=[ {pipeline} ]"


def _write_preprocessed_vrt(
    granule: S2Granule,
    config: S2PreprocessConfig,
    output_path: Path,
    offsets: dict[str, int],
    quantification_value: int,
    special_values: dict[str, int],
) -> None:
    """Build and validate one atomic VRT with GDAL's raster calc algorithm."""
    gdal = _gdal()
    scl_path = granule.scl_path
    if scl_path is None:
        raise FileNotFoundError("Missing SCL asset")
    source_paths = {**granule.band_paths, "SCL": scl_path}
    grids = {name: _dataset_grid(path) for name, path in source_paths.items()}
    output_grid = _output_grid(grids, config.resolution_m)

    # Apply radiometric special values and offsets on each band's native grid before
    # resampling. This prevents NODATA or SATURATED DNs from contaminating an average
    # or interpolation, while preserving valid corrected reflectance values of zero.
    radiometry_dir = output_path.parent / f".{output_path.stem}.radiometry"
    radiometry_dir.mkdir(parents=True, exist_ok=True)
    radiometry_paths: dict[str, Path] = {}
    invalid_dn = "||".join(
        f"(A[1]=={value})" for value in sorted(set(special_values.values()))
    )
    for band_name in config.bands:
        radiometry_path = radiometry_dir / f"{band_name}.vrt"
        temporary_radiometry = radiometry_path.with_name(
            f".{radiometry_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            algorithm = gdal.Run(
                "raster calc",
                input=[f"A={Path(granule.band_paths[band_name]).resolve()}"],
                calc=[
                    f"({invalid_dn})?{config.nodata}:"
                    f"max(A[1]+({offsets[band_name]}),0)"
                ],
                output=temporary_radiometry,
                output_format="VRT",
                output_data_type="UInt16",
                dialect="muparser",
                nodata=config.nodata,
                overwrite=True,
            )
            algorithm.Finalize()
            os.replace(temporary_radiometry, radiometry_path)
        finally:
            temporary_radiometry.unlink(missing_ok=True)
        radiometry_paths[band_name] = radiometry_path

    # Align the corrected inputs and apply the categorical SCL mask on the output grid.
    mask_terms = [
        f"(A[1]=={config.nodata})",
        "(SCL[1]==0)",
        "(SCL[1]>11)",
    ] + [
        f"(SCL[1]=={value})" for value in config.mask_classes if value != 0
    ]
    mask_expression = "||".join(mask_terms)
    inputs = []
    calculations = []
    for band_name in config.bands:
        source_resolution = float(grids[band_name]["x_resolution"])
        resampling = (
            "average" if source_resolution < config.resolution_m else "bilinear"
        )
        if source_resolution == config.resolution_m:
            resampling = "near"
        inputs.append(
            _calculation_input(
                band_name,
                radiometry_paths[band_name],
                grids[band_name],
                output_grid,
                resampling,
                input_nodata=config.nodata,
                output_nodata=config.nodata,
            )
        )
        band_mask = mask_expression.replace("A[1]", f"{band_name}[1]")
        calculations.append(
            f"({band_mask})?{config.nodata}:{band_name}[1]"
        )
    inputs.append(
        _calculation_input(
            "SCL",
            scl_path,
            grids["SCL"],
            output_grid,
            "nearest",
        )
    )

    # Let GDAL serialize the derived bands and streamed alignment pipelines.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        algorithm = gdal.Run(
            "raster calc",
            input=inputs,
            calc=calculations,
            output=temporary,
            output_format="VRT",
            output_data_type="UInt16",
            dialect="muparser",
            nodata=config.nodata,
            overwrite=True,
        )
        algorithm.Finalize()

        # Describe each output band and record its physical reflectance scale.
        dataset = gdal.Open(str(temporary), gdal.GA_Update)
        if dataset is None:
            raise RuntimeError(f"GDAL could not update preprocessed VRT: {temporary}")
        for band_number, band_name in enumerate(config.bands, start=1):
            band = dataset.GetRasterBand(band_number)
            band.SetDescription(band_name)
            band.SetScale(1.0 / quantification_value)
            band.SetUnitType("reflectance")
        dataset.FlushCache()
        dataset = None

        # Validate the complete VRT before atomically exposing it to other workers.
        _validate_preprocessed_vrt(temporary, config)
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_preprocessed_vrt(path: Path, config: S2PreprocessConfig) -> None:
    """Open a preprocessed VRT and evaluate every derived band."""
    gdal = _gdal()
    dataset = gdal.OpenEx(str(path), gdal.OF_RASTER | gdal.OF_READONLY)
    if dataset is None:
        raise RuntimeError(f"GDAL could not open preprocessed VRT: {path}")
    if dataset.RasterCount != len(config.bands):
        raise RuntimeError(f"Preprocessed VRT has an unexpected band count: {path}")
    x_offset = max(0, dataset.RasterXSize // 2)
    y_offset = max(0, dataset.RasterYSize // 2)
    for band_number, band_name in enumerate(config.bands, start=1):
        band = dataset.GetRasterBand(band_number)
        if band.GetDescription() != band_name:
            raise RuntimeError(f"Preprocessed VRT band order is invalid: {path}")
        if band.ReadRaster(x_offset, y_offset, 1, 1) is None:
            raise RuntimeError(
                f"Could not evaluate preprocessed VRT band {band_name}: {path}"
            )
    dataset = None


########################################################################################
# Per-granule preprocessing worker
########################################################################################


def preprocess_s2_granule(
    granule: S2Granule,
    config: S2PreprocessConfig,
    output_dir: Path,
    cached_row: dict[str, object] | None = None,
) -> S2PreprocessResult:
    """Preprocess one granule in a serial, process-pool, or MPI worker.

    Parameters
    ----------
    granule
        Local Level-2A granule and source assets.
    config
        Bands, resolution, SCL classes, and nodata defining the recipe.
    output_dir
        Root directory for preprocessed VRT recipes.
    cached_row
        Previous state row for this exact task, when available.

    Returns
    -------
    S2PreprocessResult
        A completed, skipped, or failed result plus its persistent state row.

    Examples
    --------
    ``result = preprocess_s2_granule(granule, config, output_dir)``
    """
    recipe_id = preprocess_recipe_id(config)
    task_id = deterministic_cache_key(
        {
            "recipe_id": recipe_id,
            "product_id": granule.product_id,
            "granule_id": granule.granule_id,
        }
    )
    filename = f"{granule.granule_id}.preprocessed.vrt"
    output_path = (
        Path(output_dir).resolve()
        / recipe_id
        / granule.tile_id
        / granule.product_id
        / filename
    )
    timestamp = datetime.now(UTC).isoformat()

    try:
        # Fail the task explicitly when an incomplete download omitted a required asset.
        missing_bands = [
            band for band in config.bands if band not in granule.band_paths
        ]
        if missing_bands:
            raise FileNotFoundError(
                f"Missing requested band(s): {', '.join(missing_bands)}"
            )
        scl_path = granule.scl_path
        if scl_path is None:
            raise FileNotFoundError("Missing SCL asset")
        source_paths = [
            granule.product_metadata,
            scl_path,
            *(granule.band_paths[band] for band in config.bands),
        ]
        fingerprint = _source_fingerprint(source_paths)

        # Reuse a validated output only when the recipe and all source files match.
        if (
            cached_row
            and cached_row.get("status") == "complete"
            and cached_row.get("source_fingerprint") == fingerprint
            and Path(str(cached_row.get("output_path", ""))) == output_path
            and output_path.is_file()
        ):
            _validate_preprocessed_vrt(output_path, config)
            cached_state_row: dict[str, object] = dict(cached_row)
            cached_state_row["checked_at"] = timestamp
            return S2PreprocessResult(
                task_id,
                granule.product_id,
                granule.granule_id,
                output_path,
                "skipped",
                None,
                cached_state_row,
            )

        # Read authoritative product metadata and construct the lazy derived VRT.
        offsets, quantification_value = read_l2a_radiometry(granule.product_metadata)
        special_values = read_l2a_special_values(granule.product_metadata)
        _write_preprocessed_vrt(
            granule,
            config,
            output_path,
            offsets,
            quantification_value,
            special_values,
        )
        complete_state_row: dict[str, object] = {
            "task_id": task_id,
            "recipe_id": recipe_id,
            "product_id": granule.product_id,
            "granule_id": granule.granule_id,
            "tile_id": granule.tile_id,
            "acquisition_time": granule.acquisition_time,
            "output_path": str(output_path),
            "source_fingerprint": fingerprint,
            "bands": json.dumps(config.bands),
            "resolution_m": config.resolution_m,
            "mask_classes": json.dumps(config.mask_classes),
            "nodata": config.nodata,
            "status": "complete",
            "error": None,
            "checked_at": timestamp,
        }
        return S2PreprocessResult(
            task_id,
            granule.product_id,
            granule.granule_id,
            output_path,
            "preprocessed",
            None,
            complete_state_row,
        )
    # Record any per-granule failure without aborting the remaining batch.
    except Exception as error:  # noqa: BLE001
        failed_state_row: dict[str, object] = {
            "task_id": task_id,
            "recipe_id": recipe_id,
            "product_id": granule.product_id,
            "granule_id": granule.granule_id,
            "tile_id": granule.tile_id,
            "acquisition_time": granule.acquisition_time,
            "output_path": str(output_path),
            "status": "failed",
            "error": str(error),
            "checked_at": timestamp,
        }
        return S2PreprocessResult(
            task_id,
            granule.product_id,
            granule.granule_id,
            output_path,
            "failed",
            str(error),
            failed_state_row,
        )
