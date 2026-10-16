from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.run import run_pipeline
from sentinel_py.pipeline.readers.s2 import _correct_dn_values


def _write_raster(path: Path, values: np.ndarray, resolution: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[1],
        height=values.shape[0],
        count=1,
        dtype=values.dtype,
        crs="EPSG:32606",
        transform=from_origin(500000, 1000000, resolution, resolution),
    ) as dataset:
        dataset.write(values, 1)


def test_dn_correction_masks_special_codes_but_preserves_valid_zero() -> None:
    """A valid offset-corrected zero must not be confused with raw nodata."""
    corrected = _correct_dn_values(
        np.array([0, 1000, 65535], dtype=np.uint16),
        offset=-1000,
        nodata=65535,
        invalid_values=(0, 65535),
    )

    np.testing.assert_array_equal(corrected, [65535, 0, 65535])


def _synthetic_pipeline(tmp_path: Path) -> tuple[Path, Path]:
    acquired = "20240615T120000"
    safe = tmp_path / "raw" / f"S2A_MSIL2A_{acquired}_N0500_R001_T06ABC_{acquired}.SAFE"
    granule = safe / "GRANULE" / f"L2A_T06ABC_A000001_{acquired}"
    offsets = "".join(
        f'<BOA_ADD_OFFSET band_id="{band_id}">-1000</BOA_ADD_OFFSET>'
        for band_id in range(13)
    )
    safe.mkdir(parents=True)
    (safe / "MTD_MSIL2A.xml").write_text(
        "<Level2A><BOA_QUANTIFICATION_VALUE>10000</BOA_QUANTIFICATION_VALUE>"
        f"{offsets}"
        "<Special_Values><SPECIAL_VALUE_TEXT>NODATA</SPECIAL_VALUE_TEXT>"
        "<SPECIAL_VALUE_INDEX>0</SPECIAL_VALUE_INDEX></Special_Values>"
        "<Special_Values><SPECIAL_VALUE_TEXT>SATURATED</SPECIAL_VALUE_TEXT>"
        "<SPECIAL_VALUE_INDEX>65535</SPECIAL_VALUE_INDEX></Special_Values>"
        "</Level2A>"
    )
    (granule / "MTD_TL.xml").parent.mkdir(parents=True)
    (granule / "MTD_TL.xml").write_text(
        "<Tile><HORIZONTAL_CS_CODE>EPSG:32606</HORIZONTAL_CS_CODE>"
        '<Size resolution="10"><NROWS>4</NROWS><NCOLS>4</NCOLS></Size>'
        '<Geoposition resolution="10"><ULX>500000</ULX>'
        "<ULY>1000000</ULY></Geoposition></Tile>"
    )
    _write_raster(
        granule / "IMG_DATA" / "R10m" / f"T06ABC_{acquired}_B02_10m.jp2",
        np.array(
            [
                [0, 1100, 2000, 2200],
                [1300, 1500, 2400, 2600],
                [65535, 3000, 4000, 4200],
                [3200, 3400, 4400, 4600],
            ],
            dtype=np.uint16,
        ),
        10,
    )
    _write_raster(
        granule / "IMG_DATA" / "R20m" / f"T06ABC_{acquired}_B03_20m.jp2",
        np.array([[1000, 1100], [0, 65535]], dtype=np.uint16),
        20,
    )
    _write_raster(
        granule / "IMG_DATA" / "R20m" / f"T06ABC_{acquired}_SCL_20m.jp2",
        np.array([[4, 4], [4, 9]], dtype=np.uint16),
        20,
    )
    aoi = tmp_path / "aoi.geojson"
    gpd.GeoDataFrame(
        {"site": ["test-site"]},
        geometry=[box(500000, 999960, 500040, 1000000)], crs="EPSG:32606"
    ).to_file(aoi, driver="GeoJSON")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        """
version: 1
execution: {method: local}
selection:
  aoi: aoi.geojson
  aoi_id_field: site
  years: [2024]
  speriod: "06-01"
  eperiod: "08-31"
output_grid:
  crs: EPSG:32606
  resolution: 20
  extent: aoi
  anchor: [0, 0]
  chunks: {y: 1, x: 1}
sources:
  s2: {type: s2.l2a.local, data_dir: raw}
nodes:
  - id: preprocess
    type: s2.preprocess
    source: s2
    bands: [B02, B03]
outputs:
  result: {from: preprocess, path: unused, format: xarray}
"""
    )
    return pipeline, granule


def test_lazy_s2_processor_defers_reads_and_preserves_grid_masks_and_dtypes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    da = pytest.importorskip("dask.array")
    from sentinel_py.pipeline.readers import s2 as lazy

    pipeline_path, _ = _synthetic_pipeline(tmp_path)
    calls = []
    original = lazy._read_scene_chunk

    def counted_reader(scene, grid, window):
        calls.append((scene.granule.granule_id, window.row_index, window.column_index))
        return original(scene, grid, window)

    monkeypatch.setattr(lazy, "_read_scene_chunk", counted_reader)
    logger = logging.getLogger("lazy-s2-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(PipelineConfig.from_file(pipeline_path), logger)
    collection = result.outputs["result"]
    dataset = collection.datasets[("test-site", "part-0000")]

    assert isinstance(dataset.reflectance.data, da.Array)
    assert isinstance(dataset.scl.data, da.Array)
    assert calls == []
    assert not (tmp_path / "unused").exists()
    assert dataset.reflectance.dtype == np.dtype("uint16")
    assert dataset.scl.dtype == np.dtype("uint8")
    assert dataset.reflectance.chunks == ((1,), (2,), (1, 1), (1, 1))
    assert dataset.x.values.tolist() == [500010.0, 500030.0]
    assert dataset.y.values.tolist() == [999990.0, 999970.0]
    assert dataset.attrs["crs"] == "EPSG:32606"

    computed = dataset.compute(scheduler="single-threaded")

    assert len(calls) == 4
    np.testing.assert_array_equal(
        computed.reflectance.values,
        np.array(
                [[[[300, 1300], [2200, 65535]], [[0, 100], [65535, 65535]]]],
            dtype=np.uint16,
        ),
    )
    np.testing.assert_array_equal(
        computed.scl.values,
        np.array([[[4, 4], [4, 9]]], dtype=np.uint8),
    )


def test_local_dask_cog_outputs_share_reads_and_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from sentinel_py.pipeline.writers.cog import cog_state_path
    from sentinel_py.pipeline.readers import s2 as lazy

    pipeline_path, _ = _synthetic_pipeline(tmp_path)
    pipeline_path.write_text(
        pipeline_path.read_text()
        .replace(
            "execution: {method: local}",
            "execution: {method: local, workers: 2, threads_per_worker: 2}",
        )
        .replace(
            "  result: {from: preprocess, path: unused, format: xarray}",
            "  first: {from: preprocess, path: first, format: cog}\n"
            "  second: {from: preprocess, path: second, format: cog}",
        )
    )
    reads = tmp_path / "reads.txt"
    original = lazy._read_scene_chunk

    def counted_reader(scene, grid, window):
        with reads.open("a") as stream:
            stream.write(f"{window.row_index},{window.column_index}\n")
        return original(scene, grid, window)

    monkeypatch.setattr(lazy, "_read_scene_chunk", counted_reader)
    logger = logging.getLogger("local-dask-cog-test")
    logger.addHandler(logging.NullHandler())

    first = run_pipeline(PipelineConfig.from_file(pipeline_path), logger)

    assert len(reads.read_text().splitlines()) == 4
    for output_id in ("first", "second"):
        output_result = first.output_results[output_id]
        assert len(output_result.results) == 8
        assert {item.status for item in output_result.results} == {"written"}
        assert cog_state_path(tmp_path / output_id / "test-site" / "part-0000").is_file()
        assert len(list((tmp_path / output_id).rglob("*.tif"))) == 8
        assert not list((tmp_path / output_id).rglob("*.tmp.tif"))

    sample = next((tmp_path / "first").rglob("reflectance*.tif"))
    with rasterio.open(sample) as raster:
        image_structure = raster.tags(ns="IMAGE_STRUCTURE")
        assert image_structure["LAYOUT"] == "COG"
        assert image_structure["COMPRESSION"] == "DEFLATE"
        assert raster.descriptions[0] == "B02"

    reads.unlink()
    second = run_pipeline(PipelineConfig.from_file(pipeline_path), logger)

    assert not reads.exists()
    for output_result in second.output_results.values():
        assert {item.status for item in output_result.results} == {"skipped"}


def test_cog_state_is_checkpointed_after_every_materialization_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from sentinel_py.pipeline import run as run_module
    from sentinel_py.pipeline.writers.cog import COGOutputWriter, cog_state_path

    pipeline_path, _ = _synthetic_pipeline(tmp_path)
    pipeline_path.write_text(
        pipeline_path.read_text().replace(
            "  result: {from: preprocess, path: unused, format: xarray}",
            "  result: {from: preprocess, path: cog-out, format: cog}",
        )
    )
    monkeypatch.setattr(run_module, "MAX_MATERIALIZATION_TASKS_PER_BATCH", 3)
    batches = []
    original = COGOutputWriter.checkpoint

    def recording(self, plan, computed, offset=0):
        if "subplans" in plan.metadata:  # count only the pipeline's own calls
            batches.append((len(computed), offset))
        return original(self, plan, computed, offset)

    monkeypatch.setattr(COGOutputWriter, "checkpoint", recording)
    logger = logging.getLogger("cog-checkpoint-test")
    logger.addHandler(logging.NullHandler())

    result = run_pipeline(PipelineConfig.from_file(pipeline_path), logger)

    # 8 tile tasks in batches of 3: checkpoints cover every task exactly once.
    assert sum(size for size, _ in batches) == 8
    assert len(batches) >= 3
    output_result = result.output_results["result"]
    assert {item.status for item in output_result.results} == {"written"}
    state_path = cog_state_path(tmp_path / "cog-out" / "test-site" / "part-0000")
    assert len(pd.read_parquet(state_path)) == 8
    assert not (state_path.with_name(state_path.name + ".parts")).exists() or not list(
        state_path.with_name(state_path.name + ".parts").glob("part-*.parquet")
    )


def test_s2_point_sampling_reports_node_progress(tmp_path, monkeypatch):
    import logging
    from types import SimpleNamespace

    import pandas as pd

    from sentinel_py.pipeline.processors.base import ProcessorContext
    from sentinel_py.pipeline.processors.s2_preprocess import (
        S2PreprocessNodeConfig,
        S2PreprocessProcessor,
    )

    def fake_sample(units, *, progress, **kwargs):
        progress(0, 4)
        progress(2, 4)
        progress(4, 4)
        return pd.DataFrame({"aoi_id": []})

    monkeypatch.setattr("sentinel_py.pipeline.readers.s2.sample_s2_points", fake_sample)
    pipeline = SimpleNamespace(
        sources={"s2": SimpleNamespace(data_dir=tmp_path)},
        aoi_plan=SimpleNamespace(units=()),
        output_grid=SimpleNamespace(crs="EPSG:3338", resolution=(20.0, 20.0), anchor=(0.0, 0.0)),
        selection=SimpleNamespace(years=(2024,), start_period=(6, 1), end_period=(8, 31)),
        execution=SimpleNamespace(workers=1),
    )
    events = []
    context = ProcessorContext(
        pipeline, SimpleNamespace(node_id="prep"), (), logging.getLogger("s2-progress"),
        progress=lambda *event: events.append(event),
    )

    S2PreprocessProcessor().execute_points(
        S2PreprocessNodeConfig(source="s2"), {}, context
    )

    assert events == [
        ("Sampling Sentinel-2 points (prep)", 0, 4),
        ("Sampling Sentinel-2 points (prep)", 2, 4),
        ("Sampling Sentinel-2 points (prep)", 4, 4),
    ]


# Fully masked windows ------------------------------------------------------------------


def _scene_and_grid(tmp_path: Path, mask_classes=(0, 1, 3, 8, 9, 10, 11)):
    """The synthetic granule as a scene source plus its four 1x1-pixel windows.

    The SCL is ``[[4, 4], [4, 9]]``: only the last window (class 9, cloud) is masked.
    """
    from sentinel_py.pipeline.grid import grid_for_unit
    from sentinel_py.pipeline.readers import s2

    pipeline, _ = _synthetic_pipeline(tmp_path)
    config = PipelineConfig.from_file(pipeline)
    granules = s2.discover_s2_granules(
        config.sources["s2"].data_dir, ("B02", "B03"), 20,
        years=(2024,), speriod=(6, 1), eperiod=(8, 31),
    )
    grid = grid_for_unit(config.aoi_plan.units[0], config.output_grid)
    scene = s2._scene_source(granules[0], ("B02", "B03"), mask_classes, 65535)
    return scene, grid, list(grid.iter_windows())


def _count_reads(monkeypatch) -> list[str]:
    from sentinel_py.pipeline.readers import s2

    opened: list[str] = []
    original = s2._window_dataset

    def counting(path, *args, **kwargs):
        opened.append(Path(path).name)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(s2, "_window_dataset", counting)
    return opened


def test_fully_masked_window_reads_only_the_scl(tmp_path, monkeypatch):
    from sentinel_py.pipeline.readers import s2

    scene, grid, windows = _scene_and_grid(tmp_path)
    opened = _count_reads(monkeypatch)

    masked = s2._read_scene_chunk_locked(scene, grid, windows[3])

    assert [name.split("_")[-2] for name in opened] == ["SCL"]
    np.testing.assert_array_equal(masked[:2, 0, 0], [65535, 65535])  # bands: nodata
    assert masked[2, 0, 0] == 9  # the SCL itself is still returned
    assert masked.dtype == np.uint16 and masked.shape == (3, 1, 1)


def test_clear_and_partly_masked_windows_still_read_every_band(tmp_path, monkeypatch):
    from sentinel_py.pipeline.readers import s2

    scene, grid, windows = _scene_and_grid(tmp_path)
    opened = _count_reads(monkeypatch)
    clear = s2._read_scene_chunk_locked(scene, grid, windows[0])
    assert sorted(name.split("_")[-2] for name in opened) == ["B02", "B03", "SCL"]
    assert clear[2, 0, 0] == 4 and clear[0, 0, 0] != 65535  # valid B02 survives

    # With no masking classes the cloud window is read and keeps real values, which
    # shows the shortcut changes nothing but the skipped work.
    scene_open, grid, windows = _scene_and_grid(tmp_path / "open", mask_classes=())
    opened.clear()
    cloudy = s2._read_scene_chunk_locked(scene_open, grid, windows[3])
    assert sorted(name.split("_")[-2] for name in opened) == ["B02", "B03", "SCL"]
    assert cloudy[0, 0, 0] == 3300  # mean of 4000..4600 after the -1000 BOA offset
    assert cloudy[1, 0, 0] == 65535  # the 20 m B03 pixel is raw saturation


def test_missing_band_asset_is_reported_even_for_a_masked_window(tmp_path):
    from dataclasses import replace

    from sentinel_py.pipeline.readers import s2

    scene, grid, windows = _scene_and_grid(tmp_path)
    paths = {k: v for k, v in scene.granule.band_paths.items() if k != "B03"}
    broken = replace(scene, granule=replace(scene.granule, band_paths=paths))

    for window in windows:  # masked (3) and clear (0..2) alike
        with pytest.raises(FileNotFoundError, match="Missing B03 asset"):
            s2._read_scene_chunk_locked(broken, grid, window)


def test_unreadable_band_file_only_matters_where_it_would_be_read(tmp_path):
    from sentinel_py.pipeline.readers import s2

    scene, grid, windows = _scene_and_grid(tmp_path)
    scene.granule.band_paths["B03"].write_bytes(b"not a raster")

    assert s2._read_scene_chunk_locked(scene, grid, windows[3])[2, 0, 0] == 9  # masked
    with pytest.raises(rasterio.errors.RasterioIOError):
        s2._read_scene_chunk_locked(scene, grid, windows[0])  # needs B03
