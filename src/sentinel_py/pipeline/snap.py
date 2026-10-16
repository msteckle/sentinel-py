"""Run SNAP ``gpt`` once per Sentinel-1 scene and cache the terrain-corrected output.

This module owns everything that touches the external SNAP executable so that
processors only describe *what* to compute: resolving ``gpt`` (never from a bare
``PATH`` lookup, because macOS ships an unrelated ``/usr/sbin/gpt``), checking its
version, clipping the external DEM, building a per-run SNAP user directory from the
orbit and EGM96 files staged at download time (so ``gpt`` never needs the network),
and running one graph per scene with atomic outputs and a reproducibility manifest.
"""

from __future__ import annotations

import functools
import hashlib
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from typing import Any

import sentinel_py.pipeline.grid as pipeline_grid
from sentinel_py.cache import deterministic_cache_key, write_json_atomic

SNAP_MAJOR_VERSION = 14
SNAP_HOME_ENV = "SNAP_HOME"
BUILTIN_GRAPH_PREFIX = "builtin:"
GRAPH_DIR = Path(__file__).with_name("snap_graphs")
# SNAP names its orbit choice per file type; both stay "Auto Download" labelled but
# use a file already present in the user directory before touching the network.
ORBIT_TYPES = {
    "POEORB": "Sentinel Precise (Auto Download)",
    "RESORB": "Sentinel Restituted (Auto Download)",
}
# Variables sentinel-py always supplies to a graph (see ``_graph_arguments``).
RESERVED_VARIABLES = frozenset(
    {
        "input",
        "output",
        "orbitType",
        "polarisations",
        "sourceBands",
        "geoRegion",
        "demFile",
        "demNoData",
        "demApplyEGM",
        "mapProjection",
        "pixelSpacing",
        "gridOriginX",
        "gridOriginY",
    }
)
# SNAP writes 0 outside the geocoded footprint and gives the GeoTIFF no nodata tag.
OUTPUT_NODATA = 0.0
DEFAULT_DEM_NODATA = -9999.0
# Gaps below this area (degrees squared, about 10 square metres) are float noise.
MIN_GAP_DEG2 = 1e-9
_VARIABLE_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")
_VERSION_RE = re.compile(r"SNAP Release version\s+(\S+)")
_DEGREES_PER_METER = 1.0 / 111_320.0
SLOT_DIR_PREFIX = "sentinel-py-snap-slots"
SLOT_POLL_SECONDS = 0.25


########################################################################################
# gpt and graph resolution
########################################################################################


def resolve_gpt(configured: str | None = None) -> Path:
    """Locate SNAP's ``gpt`` executable without searching ``PATH``.

    Parameters
    ----------
    configured : str, optional
        Explicit path to ``gpt`` (the node's ``gpt`` key).

    Returns
    -------
    pathlib.Path
        Absolute path of an executable file.

    Raises
    ------
    RuntimeError
        If neither ``configured`` nor ``$SNAP_HOME/bin/gpt`` is an executable file.
    """
    candidates = [Path(configured).expanduser()] if configured else []
    if not candidates and os.environ.get(SNAP_HOME_ENV):
        candidates.append(Path(os.environ[SNAP_HOME_ENV]).expanduser() / "bin" / "gpt")
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate.resolve()
    raise RuntimeError(
        "SNAP gpt was not found. Install SNAP "
        f"{SNAP_MAJOR_VERSION} (docs/quickstart/install.md) and set ${SNAP_HOME_ENV} "
        "or the node's 'gpt' key; a bare 'gpt' on PATH is never used because macOS "
        "ships an unrelated /usr/sbin/gpt."
    )


@functools.lru_cache(maxsize=8)
def snap_version(gpt: str) -> str:
    """Return the SNAP release version reported by ``gpt --diag``.

    Parameters
    ----------
    gpt : str
        Path to the ``gpt`` executable.

    Returns
    -------
    str
        Version string such as ``"14.0.0"``.

    Raises
    ------
    RuntimeError
        If ``gpt`` is not SNAP's tool or its major version is not supported.
    """
    result = subprocess.run(
        [gpt, "--diag"], capture_output=True, text=True, timeout=120, check=False
    )
    match = _VERSION_RE.search(result.stdout + result.stderr)
    if match is None:
        raise RuntimeError(f"{gpt} is not SNAP's gpt: 'gpt --diag' printed no version")
    version = match.group(1)
    if version.split(".")[0] != str(SNAP_MAJOR_VERSION):
        raise RuntimeError(
            f"SNAP {version} found at {gpt}; sentinel-py requires SNAP "
            f"{SNAP_MAJOR_VERSION}.x"
        )
    return version


def load_graph(spec: str, base_dir: Path | None = None) -> tuple[Path, str]:
    """Resolve a graph reference to a file and its content hash.

    Parameters
    ----------
    spec : str
        ``builtin:<name>`` for a graph shipped in ``pipeline/snap_graphs`` or a path
        to a graph XML file.
    base_dir : pathlib.Path, optional
        Directory against which a relative path is resolved.

    Returns
    -------
    tuple[pathlib.Path, str]
        The graph file and the SHA-256 hex digest of its bytes.

    Raises
    ------
    ValueError
        If the graph does not exist.
    """
    if spec.startswith(BUILTIN_GRAPH_PREFIX):
        path = GRAPH_DIR / f"{spec.removeprefix(BUILTIN_GRAPH_PREFIX)}.xml"
    else:
        path = Path(spec).expanduser()
        if not path.is_absolute() and base_dir is not None:
            path = base_dir / path
    if not path.is_file():
        raise ValueError(f"SNAP graph does not exist: {spec}")
    return path.resolve(), hashlib.sha256(path.read_bytes()).hexdigest()


def graph_variables(path: Path) -> frozenset[str]:
    """Return the ``${variable}`` names a graph XML file uses outside comments."""
    text = re.sub(r"<!--.*?-->", "", path.read_text(), flags=re.DOTALL)
    return frozenset(_VARIABLE_RE.findall(text))


########################################################################################
# Scene jobs
########################################################################################


@dataclass(frozen=True)
class SceneJob:
    """Everything one ``gpt`` run needs; small, immutable, and picklable.

    Attributes
    ----------
    scene : str
        Scene name without suffix.
    zip_path, orbit_path, egm96_path : str
        Input archive, staged orbit file, and staged EGM96 archive.
    orbit_kind : str
        ``"POEORB"`` or ``"RESORB"``; selects SNAP's ``orbitType``.
    platform, acquired : str
        Satellite (``S1A``) and acquisition start (``%Y%m%dT%H%M%S``), used to place
        the orbit file in SNAP's ``YYYY/MM`` directory layout.
    region_wkt : str
        Subset region in EPSG:4326 (lon/lat order).
    dem_tiles : tuple[str, ...]
        DEM tiles intersecting the region plus margin.
    dem_bounds_wgs84 : tuple[float, float, float, float]
        Region plus DEM margin, used to clip the external DEM.
    dem_apply_egm : bool
        Whether SNAP adds the EGM96 geoid to DEM heights.
    polarizations : tuple[str, ...]
        Requested polarizations; also the output band order.
    crs, pixel_spacing, grid_origin : Any
        Output grid: CRS string, square pixel size in metres, and the SNAP
        standard-grid origin (grid anchor plus half a pixel).
    graph_path, graph_sha256 : str
        Graph file and its content hash.
    parameters : tuple[tuple[str, str], ...]
        Extra graph variables, sorted by name.
    work_dir : str
        Root for outputs, DEM clips, logs, and temporary user directories.
    gpt, snap_version : str
        Executable and release version.
    memory_gb, threads : int
        JVM heap and tile parallelism for the run.
    timeout_s : int, optional
        Wall-clock limit for the ``gpt`` process.
    max_concurrent : int
        Most ``gpt`` processes allowed to run at once on this machine, across every
        worker and pipeline (see :func:`snap_slot`); not part of the cache key.
    """

    scene: str
    zip_path: str
    orbit_path: str
    orbit_kind: str
    platform: str
    acquired: str
    egm96_path: str
    region_wkt: str
    dem_tiles: tuple[str, ...]
    dem_bounds_wgs84: tuple[float, float, float, float]
    dem_apply_egm: bool
    polarizations: tuple[str, ...]
    crs: str
    pixel_spacing: float
    grid_origin: tuple[float, float]
    graph_path: str
    graph_sha256: str
    parameters: tuple[tuple[str, str], ...]
    work_dir: str
    gpt: str
    snap_version: str
    memory_gb: int = 16
    threads: int = 4
    timeout_s: int | None = None
    max_concurrent: int = 2

    def key(self) -> str:
        """Return the deterministic key of every input that affects the output.

        Returns
        -------
        str
            32-character hex key; identical inputs always share one cached output.
        """
        archive = Path(self.zip_path).stat()
        tiles = [
            (Path(tile).name, Path(tile).stat().st_size, Path(tile).stat().st_mtime_ns)
            for tile in self.dem_tiles
        ]
        return deterministic_cache_key(
            {
                "scene": self.scene,
                "zip": (archive.st_size, archive.st_mtime_ns),
                "orbit": (self.orbit_kind, Path(self.orbit_path).name),
                "region": self.region_wkt,
                "dem": (tiles, self.dem_bounds_wgs84, self.dem_apply_egm),
                "polarizations": self.polarizations,
                "grid": (self.crs, self.pixel_spacing, self.grid_origin),
                "graph": self.graph_sha256,
                "parameters": self.parameters,
                "snap": self.snap_version,
            }
        )

    @property
    def output_path(self) -> Path:
        """Return the cached GeoTIFF path for this job."""
        return Path(self.work_dir) / "scenes" / self.scene / f"{self.key()}.tif"


def expand_bounds(
    bounds: tuple[float, float, float, float], margin_m: float
) -> tuple[float, float, float, float]:
    """Return lon/lat ``bounds`` grown by ``margin_m`` metres on every side.

    Longitude growth is scaled by the cosine of the mid latitude, so the margin is
    approximately constant on the ground at Arctic latitudes.
    """
    min_x, min_y, max_x, max_y = bounds
    d_lat = margin_m * _DEGREES_PER_METER
    d_lon = d_lat / max(math.cos(math.radians((min_y + max_y) / 2)), 1e-6)
    return (
        round(min_x - d_lon, 6),
        round(min_y - d_lat, 6),
        round(max_x + d_lon, 6),
        round(max_y + d_lat, 6),
    )


def region_polygon_wkt(bounds: tuple[float, float, float, float]) -> str:
    """Return the closed lon/lat WKT box SNAP's ``Subset`` ``geoRegion`` expects."""
    west, south, east, north = bounds
    return (
        f"POLYGON(({west} {south},{east} {south},{east} {north},"
        f"{west} {north},{west} {south}))"
    )


########################################################################################
# DEM tiles and clipping
########################################################################################


@functools.lru_cache(maxsize=4096)
def _footprint(path: str, mtime_ns: int) -> tuple[float, float, float, float]:
    """Read a tile's EPSG:4326 bounds; ``mtime_ns`` keys the cache to the file."""
    import rasterio
    from rasterio.warp import transform_bounds

    del mtime_ns
    with pipeline_grid.RASTERIO_LOCK, rasterio.open(path) as dataset:
        return tuple(
            transform_bounds(dataset.crs, "EPSG:4326", *dataset.bounds, densify_pts=21)
        )


def tile_footprint_wgs84(path: Path) -> tuple[float, float, float, float]:
    """Return a DEM tile's ``(west, south, east, north)`` bounds in EPSG:4326.

    Bounds are densified for polar CRSs and cached per file and modification time,
    so planning many scenes against one tile set opens each tile once.
    """
    path = Path(path)
    return _footprint(str(path), path.stat().st_mtime_ns)


def select_dem_tiles(
    tiles: tuple[Path, ...], bounds_wgs84: tuple[float, float, float, float]
) -> tuple[Path, ...]:
    """Return the DEM tiles whose footprints intersect a lon/lat box."""
    west, south, east, north = bounds_wgs84
    selected = []
    for tile in tiles:
        t_west, t_south, t_east, t_north = tile_footprint_wgs84(tile)
        if t_west < east and t_east > west and t_south < north and t_north > south:
            selected.append(tile)
    return tuple(selected)


def scene_region_bounds(
    geometry_wgs84: Any,
    footprint_wkt: str,
    *,
    subset: str,
    subset_margin_m: float,
    dem_margin_m: float,
) -> tuple[tuple[float, float, float, float], tuple[float, float, float, float]]:
    """Return a scene's processing region and the larger DEM extent it needs.

    Parameters
    ----------
    geometry_wgs84 : shapely geometry
        AOI unit geometry in EPSG:4326.
    footprint_wkt : str
        Scene footprint (lon/lat WKT) from the catalog.
    subset : str
        ``"aoi"`` processes the AOI-footprint overlap plus ``subset_margin_m``;
        ``"scene"`` processes the whole footprint.
    subset_margin_m, dem_margin_m : float
        Margins in metres around the processing region and around the DEM extent.

    Returns
    -------
    tuple
        ``(region, dem_bounds)``, each ``(west, south, east, north)``.
    """
    from shapely import wkt

    footprint = wkt.loads(footprint_wkt)
    if subset == "scene":
        region = footprint.bounds
    else:
        region = expand_bounds(
            geometry_wgs84.intersection(footprint).bounds, subset_margin_m
        )
    return region, expand_bounds(region, dem_margin_m)


def clip_dem(
    tiles: tuple[str, ...],
    bounds_wgs84: tuple[float, float, float, float],
    destination: Path,
) -> float:
    """Write a single GeoTIFF clip of DEM tiles for SNAP's external-DEM input.

    SNAP accepts one DEM file, so tiles are merged and cropped to the region plus
    margin in the tiles' native CRS (SNAP reads polar stereographic DEMs directly).

    Parameters
    ----------
    tiles : tuple[str, ...]
        DEM tile paths sharing one CRS.
    bounds_wgs84 : tuple[float, float, float, float]
        Region plus DEM margin as ``(west, south, east, north)``.
    destination : pathlib.Path
        Output GeoTIFF; written atomically.

    Returns
    -------
    float
        DEM nodata value of the clip (SNAP's ``externalDEMNoDataValue``).

    Raises
    ------
    ValueError
        If the tiles do not share a CRS.
    """
    import rasterio
    from rasterio.merge import merge
    from rasterio.warp import transform_bounds

    with pipeline_grid.RASTERIO_LOCK:
        sources = [rasterio.open(tile) for tile in tiles]
        try:
            crs = sources[0].crs
            if any(source.crs != crs for source in sources):
                raise ValueError("DEM tiles must share one CRS to be mosaicked")
            nodata = sources[0].nodata
            nodata = DEFAULT_DEM_NODATA if nodata is None else float(nodata)
            native = transform_bounds("EPSG:4326", crs, *bounds_wgs84, densify_pts=21)
            array, transform = merge(
                sources, bounds=native, nodata=nodata, masked=False
            )
        finally:
            for source in sources:
                source.close()
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Unique per writer: concurrent scene jobs share one clip when they share
        # a region, and each must be able to replace the final file atomically.
        partial = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
        try:
            with rasterio.open(
                partial,
                "w",
                driver="GTiff",
                height=array.shape[1],
                width=array.shape[2],
                count=1,
                dtype="float32",
                crs=crs,
                transform=transform,
                nodata=nodata,
                compress="deflate",
                tiled=True,
            ) as target:
                target.write(array[0].astype("float32"), 1)
            os.replace(partial, destination)
        finally:
            partial.unlink(missing_ok=True)
    return nodata


########################################################################################
# Running gpt
########################################################################################


@contextmanager
def snap_slot(limit: int) -> Iterator[int]:
    """Hold one of ``limit`` per-machine slots while a ``gpt`` process runs.

    Every ``gpt`` is a JVM with its own heap and worker threads, so running as many
    as there are Dask workers or threads can exhaust memory and CPU. Slots are
    advisory ``flock`` files in the machine's temporary directory, so the bound
    holds across all worker processes and pipelines on one machine, and a crashed
    process releases its slot automatically. Slots are per machine on purpose:
    on Slurm each node enforces its own limit.

    Parameters
    ----------
    limit : int
        Number of slots; the caller blocks until one is free.

    Yields
    ------
    int
        Index of the slot held.
    """
    import fcntl

    directory = Path(tempfile.gettempdir()) / f"{SLOT_DIR_PREFIX}-{os.getuid()}"
    directory.mkdir(parents=True, exist_ok=True)
    slot, handle = 0, None
    while handle is None:
        for candidate in range(limit):
            attempt = open(directory / f"slot-{candidate}.lock", "a+")
            try:
                fcntl.flock(attempt, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                attempt.close()
                continue
            slot, handle = candidate, attempt
            break
        else:
            time.sleep(SLOT_POLL_SECONDS)
    try:
        yield slot
    finally:
        handle.close()  # closing the descriptor releases the lock


def prepare_userdir(job: SceneJob, userdir: Path) -> None:
    """Link staged orbit and EGM96 files into a throwaway SNAP user directory.

    Parameters
    ----------
    job : SceneJob
        Job supplying the staged file paths and acquisition date.
    userdir : pathlib.Path
        Empty directory to populate as ``snap.userdir``.

    Raises
    ------
    FileNotFoundError
        If a staged file is missing; ``gpt`` must never fetch it at run time.
    """
    orbit, egm96 = Path(job.orbit_path), Path(job.egm96_path)
    for staged in (orbit, egm96):
        if not staged.is_file():
            raise FileNotFoundError(
                f"{staged} is missing; run 'sentinel-py asf download' to stage "
                "orbit files and the EGM96 grid before processing offline"
            )
    # SNAP looks for orbit files under <kind>/<satellite>/<year>/<month>.
    orbit_dir = (
        userdir / "auxdata" / "Orbits" / "Sentinel-1" / job.orbit_kind / job.platform
        / job.acquired[:4] / job.acquired[4:6]
    )
    egm_dir = userdir / "auxdata" / "dem" / "egm96"
    for directory, source in ((orbit_dir, orbit), (egm_dir, egm96)):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / source.name).symlink_to(source.resolve())


def _graph_arguments(job: SceneJob, dem_file: Path, dem_nodata: float, output: Path) -> dict[str, str]:
    """Return the ``-P`` variables of one run, reserved names first."""
    pols = ",".join(job.polarizations)
    arguments = {
        "input": job.zip_path,
        "output": str(output),
        "orbitType": ORBIT_TYPES[job.orbit_kind],
        "polarisations": pols,
        "sourceBands": ",".join(f"Sigma0_{pol}" for pol in job.polarizations),
        "geoRegion": job.region_wkt,
        "demFile": str(dem_file),
        "demNoData": repr(dem_nodata),
        "demApplyEGM": str(job.dem_apply_egm).lower(),
        "mapProjection": job.crs,
        "pixelSpacing": repr(float(job.pixel_spacing)),
        "gridOriginX": repr(float(job.grid_origin[0])),
        "gridOriginY": repr(float(job.grid_origin[1])),
    }
    arguments.update(dict(job.parameters))
    return arguments


def run_scene(job: SceneJob, logger: logging.Logger | None = None) -> str:
    """Run the graph for one scene, or reuse a verified cached output.

    The GeoTIFF is written to a ``.part`` sibling and renamed only after ``gpt``
    exits cleanly and the file validates, so an interrupted run never leaves a
    plausible-looking partial output. A JSON manifest beside the output records
    the exact command, versions, inputs, and timings for reproduction.

    Parameters
    ----------
    job : SceneJob
        Fully resolved inputs for one scene.
    logger : logging.Logger, optional
        Logger for cache decisions and failures.

    Returns
    -------
    str
        Path of the terrain-corrected GeoTIFF (one band per polarization).

    Raises
    ------
    RuntimeError
        If ``gpt`` fails, times out, or produces an invalid file; the message
        includes the end of the run log.
    """
    logger = logger or logging.getLogger(__name__)
    output = job.output_path
    if output.is_file():
        logger.debug("SNAP output cached: %s", output)
        return str(output)

    output.parent.mkdir(parents=True, exist_ok=True)
    work = Path(job.work_dir)
    key = output.stem
    log_path = output.with_suffix(".log")
    # SNAP's GeoTIFF writer uses the file's base name as the product node name, which
    # rejects leading dots and similar, and appends ".tif" to other extensions; so the
    # temporary name must be a plain identifier ending in ".tif".
    partial = output.with_name(f"{key}_partial{uuid.uuid4().hex[:8]}.tif")
    dem_key = deterministic_cache_key(
        {
            "tiles": [
                (Path(t).name, Path(t).stat().st_size, Path(t).stat().st_mtime_ns)
                for t in job.dem_tiles
            ],
            "bounds": job.dem_bounds_wgs84,
        }
    )
    dem_file = work / "dem" / f"{dem_key}.tif"
    userdir: Path | None = None
    started = time.monotonic()
    try:
        if dem_file.is_file():
            import rasterio

            with pipeline_grid.RASTERIO_LOCK, rasterio.open(dem_file) as dataset:
                dem_nodata = float(dataset.nodata)
        else:
            dem_nodata = clip_dem(job.dem_tiles, job.dem_bounds_wgs84, dem_file)

        (work / "tmp").mkdir(parents=True, exist_ok=True)
        userdir = Path(tempfile.mkdtemp(dir=work / "tmp", prefix="userdir-"))
        prepare_userdir(job, userdir)
        arguments = _graph_arguments(job, dem_file, dem_nodata, partial)
        command = [
            job.gpt,
            job.graph_path,
            "-e",
            "-q",
            str(job.threads),
            f"-J-Xmx{job.memory_gb}G",
            f"-Dsnap.userdir={userdir}",
            *[f"-P{name}={value}" for name, value in arguments.items()],
        ]
        logger.info("Running SNAP for %s (%s)", job.scene, key)
        try:
            with snap_slot(job.max_concurrent), log_path.open("w") as log:
                log.write("$ " + " ".join(command) + "\n")
                log.flush()
                completed = subprocess.run(
                    command,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=job.timeout_s,
                    check=False,
                )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"SNAP timed out after {job.timeout_s}s for {job.scene}; see {log_path}"
            ) from error
        if completed.returncode != 0 or not partial.is_file():
            tail = "\n".join(log_path.read_text().splitlines()[-15:])
            raise RuntimeError(
                f"SNAP gpt failed (exit {completed.returncode}) for {job.scene}; "
                f"log: {log_path}\n{tail}"
            )
        _validate_output(partial, len(job.polarizations), job.scene)
        os.replace(partial, output)
    finally:
        partial.unlink(missing_ok=True)
        if userdir is not None:
            shutil.rmtree(userdir, ignore_errors=True)

    manifest = {
        "scene": job.scene,
        "key": key,
        "created_at": datetime.now(UTC).isoformat(),
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "command": [
            part.replace(str(partial), str(output)) for part in command
        ],
        "job": asdict(job),
        "output": str(output),
    }
    write_json_atomic(output.with_suffix(".json"), manifest)
    return str(output)


def _validate_output(path: Path, bands: int, scene: str) -> None:
    """Raise unless ``path`` opens as a raster with the expected band count."""
    import rasterio

    try:
        with pipeline_grid.RASTERIO_LOCK, rasterio.open(path) as dataset:
            count, crs = dataset.count, dataset.crs
    except rasterio.errors.RasterioIOError as error:
        raise RuntimeError(f"SNAP output for {scene} is not a valid raster") from error
    if count != bands or crs is None:
        raise RuntimeError(
            f"SNAP output for {scene} has {count} band(s) and CRS {crs}; "
            f"expected {bands} band(s) with a CRS"
        )
