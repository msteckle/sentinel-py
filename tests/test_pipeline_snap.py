import json
import stat
from dataclasses import replace
from pathlib import Path

import pytest
import rasterio
from helpers import write_s1_project, write_stub_gpt

from sentinel_py.pipeline import PipelineConfig, snap


def _job(tmp_path: Path, **overrides) -> snap.SceneJob:
    """Return a runnable job from a synthetic project (first scene, first AOI)."""
    import logging

    from sentinel_py.pipeline.processors.base import ProcessorContext
    from sentinel_py.pipeline.processors.snap_gpt import SnapGPTProcessor

    config = PipelineConfig.from_file(write_s1_project(tmp_path))
    node = config.nodes[0]
    processor = SnapGPTProcessor()
    context = ProcessorContext(config, node, (), logging.getLogger("test"))
    state = processor._prepare(node.config, context)
    unit = config.aoi_plan.units[0]
    _, job = processor._jobs(unit, node.config, context, state)[0]
    return replace(job, **overrides) if overrides else job


def test_resolve_gpt_never_uses_path(tmp_path: Path, monkeypatch):
    stub = write_stub_gpt(tmp_path / "gpt")
    assert snap.resolve_gpt(str(stub)) == stub.resolve()

    monkeypatch.setenv(snap.SNAP_HOME_ENV, str(tmp_path))
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "gpt").write_text("#!/bin/sh\n")
    (tmp_path / "bin" / "gpt").chmod(0o755)
    assert snap.resolve_gpt() == (tmp_path / "bin" / "gpt").resolve()

    monkeypatch.delenv(snap.SNAP_HOME_ENV)
    monkeypatch.setenv("PATH", str(tmp_path))  # a gpt on PATH must be ignored
    (tmp_path / "gpt").write_text("#!/bin/sh\n")
    (tmp_path / "gpt").chmod(0o755)
    with pytest.raises(RuntimeError, match="/usr/sbin/gpt"):
        snap.resolve_gpt()


def test_snap_version_accepts_14_and_rejects_other_tools(tmp_path: Path):
    assert snap.snap_version(str(write_stub_gpt(tmp_path / "ok"))) == "14.0.0"

    foreign = tmp_path / "foreign"
    foreign.write_text("#!/bin/sh\necho 'GPT fdisk'\n")
    foreign.chmod(foreign.stat().st_mode | stat.S_IXUSR)
    with pytest.raises(RuntimeError, match="not SNAP's gpt"):
        snap.snap_version(str(foreign))

    old = tmp_path / "old"
    old.write_text("#!/bin/sh\necho 'SNAP Release version 9.0.0'\n")
    old.chmod(old.stat().st_mode | stat.S_IXUSR)
    with pytest.raises(RuntimeError, match="requires SNAP 14"):
        snap.snap_version(str(old))


def test_builtin_graph_variables_are_all_reserved():
    path, digest = snap.load_graph("builtin:grd_sigma0")
    assert len(digest) == 64
    assert snap.graph_variables(path) == snap.RESERVED_VARIABLES
    with pytest.raises(ValueError, match="does not exist"):
        snap.load_graph("builtin:missing")


def test_expand_bounds_scales_longitude_at_high_latitude():
    west, south, east, north = snap.expand_bounds((-150.0, 68.0, -149.0, 69.0), 10_000)
    assert south == pytest.approx(68.0 - 10_000 / 111_320, abs=1e-5)
    assert -150.0 - west > 2 * (68.0 - south)  # longitude degrees are shorter
    wkt = snap.region_polygon_wkt((west, south, east, north))
    assert wkt.startswith("POLYGON((") and wkt.count(",") == 4


def test_run_scene_builds_offline_userdir_caches_and_records_manifest(tmp_path: Path):
    job = _job(tmp_path)
    output = Path(snap.run_scene(job))

    assert output == job.output_path and output.is_file()
    manifest = json.loads(output.with_suffix(".json").read_text())
    assert manifest["scene"] == job.scene and manifest["job"]["snap_version"] == "14.0.0"
    command = manifest["command"]
    assert command[0] == job.gpt and "-e" in command
    assert any(part.startswith("-J-Xmx") for part in command)
    assert "-PorbitType=Sentinel Precise (Auto Download)" in command
    assert "-PdemApplyEGM=false" in command
    assert "-PsourceBands=Sigma0_VV,Sigma0_VH" in command
    # Grid origin is the anchor plus half a pixel so SNAP's pixel centres line up.
    assert "-PgridOriginX=10.0" in command and "-PpixelSpacing=20.0" in command
    with rasterio.open(output) as dataset:
        assert dataset.count == 2 and dataset.crs.to_string() == "EPSG:3338"
    assert not any(p.name.startswith("userdir-") for p in (Path(job.work_dir) / "tmp").iterdir())

    before = output.stat().st_mtime_ns
    assert snap.run_scene(job) == str(output)  # cache hit: no second run
    assert output.stat().st_mtime_ns == before


def test_run_scene_failure_leaves_no_output_and_cleans_up(tmp_path: Path, monkeypatch):
    job = _job(tmp_path)
    monkeypatch.setenv("STUB_GPT_FAIL", "1")
    with pytest.raises(RuntimeError, match="simulated failure"):
        snap.run_scene(job)
    assert not job.output_path.exists()
    assert not list(job.output_path.parent.glob("*_partial*.tif"))
    assert not any((Path(job.work_dir) / "tmp").iterdir())


def test_run_scene_requires_staged_orbit_and_egm96(tmp_path: Path):
    job = _job(tmp_path)
    Path(job.orbit_path).unlink()
    with pytest.raises(FileNotFoundError, match="asf download"):
        snap.run_scene(job)

    (tmp_path / "second").mkdir()
    job = _job(tmp_path / "second")
    Path(job.egm96_path).unlink()
    with pytest.raises(FileNotFoundError, match="asf download"):
        snap.run_scene(job)


def test_job_key_changes_with_every_result_affecting_input(tmp_path: Path):
    job = _job(tmp_path)
    base = job.key()
    variants = [
        replace(job, graph_sha256="0" * 64),
        replace(job, polarizations=("VV",)),
        replace(job, dem_apply_egm=True),
        replace(job, orbit_kind="RESORB"),
        replace(job, pixel_spacing=10.0),
        replace(job, snap_version="14.0.1"),
        replace(job, region_wkt=job.region_wkt.replace("-155", "-156")),
        replace(job, parameters=(("extra", "1"),)),
    ]
    assert len({v.key() for v in variants} | {base}) == len(variants) + 1
    # Process-local settings must not invalidate cached outputs.
    assert replace(job, memory_gb=64, threads=1, timeout_s=5).key() == base


def test_tile_selection_and_footprints(tmp_path: Path):
    job = _job(tmp_path)
    tiles = tuple(Path(t) for t in job.dem_tiles)
    west, south, east, north = job.dem_bounds_wgs84
    footprint = snap.tile_footprint_wgs84(tiles[0])
    assert footprint[0] < west and footprint[2] > east  # the tile spans the DEM extent
    assert snap.tile_footprint_wgs84(tiles[0]) == footprint  # served from the cache
    assert snap.select_dem_tiles(tiles, (west, south, east, north)) == tiles
    assert snap.select_dem_tiles(tiles, (0.0, 0.0, 1.0, 1.0)) == ()


def test_scene_region_bounds_for_aoi_and_scene_subsets():
    from shapely.geometry import box

    footprint = box(-157, 68, -154, 69).wkt
    aoi = box(-155.8, 68.4, -155.7, 68.45)
    region, dem_bounds = snap.scene_region_bounds(
        aoi, footprint, subset="aoi", subset_margin_m=1000, dem_margin_m=5000
    )
    assert region[0] < -155.8 and region[2] > -155.7  # margin added around the AOI
    assert dem_bounds[0] < region[0] and dem_bounds[3] > region[3]
    whole, _ = snap.scene_region_bounds(
        aoi, footprint, subset="scene", subset_margin_m=1000, dem_margin_m=5000
    )
    assert whole == (-157.0, 68.0, -154.0, 69.0)


# Bounded concurrency ---------------------------------------------------------------


def _isolated_slots(monkeypatch, tmp_path: Path) -> None:
    """Give a test its own slot directory so tests cannot block each other."""
    monkeypatch.setattr(snap.tempfile, "gettempdir", lambda: str(tmp_path / "slots"))


def test_snap_slot_blocks_until_one_of_the_slots_is_free(monkeypatch, tmp_path: Path):
    import threading
    import time

    _isolated_slots(monkeypatch, tmp_path)
    entered = threading.Event()
    release = threading.Event()

    def holder():
        with snap.snap_slot(1):
            entered.set()
            release.wait(5)

    thread = threading.Thread(target=holder)
    thread.start()
    entered.wait(5)
    started = time.monotonic()
    waiter_done = threading.Event()

    def waiter():
        with snap.snap_slot(1):
            waiter_done.set()

    other = threading.Thread(target=waiter)
    other.start()
    assert not waiter_done.wait(0.6)  # the only slot is taken
    release.set()
    assert waiter_done.wait(5)
    thread.join()
    other.join()
    assert time.monotonic() - started >= 0.6

    # With two slots both holders run together.
    inside = []
    barrier = threading.Barrier(2, timeout=5)

    def pair():
        with snap.snap_slot(2) as slot:
            inside.append(slot)
            barrier.wait()

    threads = [threading.Thread(target=pair) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sorted(inside) == [0, 1]


@pytest.mark.parametrize(("limit", "expected_max_overlap"), [(1, 1), (3, 3)])
def test_run_scene_never_exceeds_max_concurrent(
    tmp_path: Path, monkeypatch, limit: int, expected_max_overlap: int
):
    from concurrent.futures import ThreadPoolExecutor

    _isolated_slots(monkeypatch, tmp_path)
    log = tmp_path / "gpt-times.log"
    monkeypatch.setenv("STUB_GPT_SLEEP", "0.8")
    monkeypatch.setenv("STUB_GPT_LOG", str(log))
    (tmp_path / "project").mkdir()
    base = _job(tmp_path / "project", max_concurrent=limit)
    jobs = [replace(base, parameters=((f"run{i}", "1"),)) for i in range(3)]

    with ThreadPoolExecutor(3) as pool:
        list(pool.map(snap.run_scene, jobs))

    spans = sorted(tuple(map(float, line.split())) for line in log.read_text().splitlines())
    assert len(spans) == 3
    overlap = max(
        sum(1 for other in spans if other[0] < end and other[1] > start)
        for start, end in spans
    )
    assert overlap == expected_max_overlap
