from pathlib import Path

from typer.testing import CliRunner

from sentinel_py.cache import DEFAULT_CDSE_CACHE_DIR
from sentinel_py.cli.main import app

runner = CliRunner()


def _write_aoi(path: Path) -> None:
    path.write_text(
        """
        {
          "type": "FeatureCollection",
          "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": {
              "type": "Polygon",
              "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            }
          }]
        }
        """
    )


def _write_s5_config(path: Path) -> None:
    path.write_text(
        "[default]\n"
        "aws_access_key_id = test-access-key\n"
        "aws_secret_access_key = test-secret-key\n"
        "host_base = eodata.dataspace.copernicus.eu\n"
    )


def test_cdse_query_uses_hidden_default_cache(tmp_path: Path, monkeypatch):
    aoi = tmp_path / "aoi.geojson"
    _write_aoi(aoi)
    calls = {}

    def fake_query_cdse(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(
        "sentinel_py.download.cdse.query_cdse",
        fake_query_cdse,
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "query",
            "--aoi",
            str(aoi),
            "--crs",
            "EPSG:4326",
            "--years",
            "2024",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["cache_dir"] == DEFAULT_CDSE_CACHE_DIR


def test_cdse_download_discovers_query_in_hidden_default_cache(
    tmp_path: Path,
    monkeypatch,
):
    config = tmp_path / ".s5cfg"
    _write_s5_config(config)
    query_cache = tmp_path / "scenes.parquet"
    calls = {}

    def fake_find_latest(cache_dir: Path):
        calls["searched_cache_dir"] = cache_dir
        return query_cache

    def fake_resolve_and_download(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(
        "sentinel_py.download.cdse.find_latest_scenes_cache",
        fake_find_latest,
    )
    monkeypatch.setattr(
        "sentinel_py.download.cdse.resolve_and_download",
        fake_resolve_and_download,
    )

    result = runner.invoke(
        app,
        [
            "cdse",
            "download",
            "--mission",
            "S2",
            "--bands",
            "B04",
            "--outdir",
            str(tmp_path),
            "--res",
            "20",
            "--config",
            str(config),
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert calls["searched_cache_dir"] == DEFAULT_CDSE_CACHE_DIR
    assert calls["scenes_cache"] == query_cache
