from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import requests
from typer.testing import CliRunner

from sentinel_py.cli.main import app
from sentinel_py.download.base import DownloadSummary
from sentinel_py.download.s1_orbits import (
    ORBIT_BASE_URLS,
    ORBIT_STATE_RELPATH,
    parse_orbit_filename,
    parse_scene_name,
    select_orbit,
    stage_orbits,
)
from sentinel_py.enums import S1OrbitPolicy

SCENE = "S1A_IW_GRDH_1SDV_20240820T164404_20240820T164429_055299_06BE02_F69F"
PRECISE = "S1A_OPER_AUX_POEORB_OPOD_20240909T080000_V20240819T225942_20240821T005942.EOF"
OLD_PRECISE = "S1A_OPER_AUX_POEORB_OPOD_20240908T080000_V20240819T225942_20240821T005942.EOF"
RESTITUTED = "S1A_OPER_AUX_RESORB_OPOD_20240820T200000_V20240820T160000_20240820T180000.EOF"
OTHER_PLATFORM = "S1B_OPER_AUX_POEORB_OPOD_20240909T080000_V20240819T225942_20240821T005942.EOF"
EOF_BODY = b"<?xml version='1.0'?><Earth_Explorer_File/>"


class FakeResponse:
    def __init__(self, text="", content=b"", status=200):
        self.text = text
        self._content = content
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def iter_content(self, chunk_size):
        yield self._content


class FakeSession:
    """Serve nginx-style listings and orbit bodies without any network access."""

    def __init__(self, listings: dict[str, list[str]], fail_listing: bool = False):
        self.listings = listings
        self.fail_listing = fail_listing
        self.requests: list[str] = []

    def get(self, url, timeout=None, stream=False):
        self.requests.append(url)
        for kind, base in ORBIT_BASE_URLS.items():
            if url == base:
                if self.fail_listing:
                    raise requests.ConnectionError("offline")
                names = self.listings.get(kind, [])
                return FakeResponse(
                    text="".join(f'<a href="{n}">{n[:20]}..&gt;</a>\n' for n in names)
                )
        return FakeResponse(content=EOF_BODY)


def _manifest(scene: str = SCENE) -> pd.DataFrame:
    return pd.DataFrame(
        {"granule": [scene], "url": [f"https://example.test/{scene}.zip"]}
    )


def _stage(tmp_path, session, policy=S1OrbitPolicy.precise_or_restituted, products=None):
    return stage_orbits(
        _manifest() if products is None else products,
        tmp_path,
        policy,
        session=session,
        retries=0,
        retry_backoff=0,
    )


def test_parse_scene_and_orbit_names():
    scene = parse_scene_name(f"{SCENE}.zip")
    assert scene.platform == "S1A"
    assert scene.start == datetime(2024, 8, 20, 16, 44, 4, tzinfo=timezone.utc)
    assert scene.product == "GRDH"
    assert parse_scene_name("one.zip") is None
    orbit = parse_orbit_filename(PRECISE)
    assert orbit.kind == "POEORB"
    assert orbit.url == ORBIT_BASE_URLS["POEORB"] + PRECISE
    assert parse_orbit_filename("not-an-orbit.EOF") is None


def test_select_orbit_requires_full_cover_platform_and_prefers_newest():
    scene = parse_scene_name(SCENE)
    candidates = [parse_orbit_filename(n) for n in (OLD_PRECISE, PRECISE, OTHER_PLATFORM)]
    chosen = select_orbit(candidates, "S1A", scene.start, scene.stop)
    assert chosen.name == PRECISE
    assert select_orbit(candidates, "S1C", scene.start, scene.stop) is None
    outside = parse_orbit_filename(RESTITUTED.replace("V20240820T160000", "V20240820T164410"))
    assert select_orbit([outside], "S1A", scene.start, scene.stop) is None


def test_stage_orbits_downloads_and_records_assignment(tmp_path: Path):
    session = FakeSession({"POEORB": [PRECISE, OTHER_PLATFORM], "RESORB": [RESTITUTED]})
    summary = _stage(tmp_path, session)

    assert summary.staged == 1 and summary.failed == 0 and not summary.missing
    assert (tmp_path / "orbits" / "POEORB" / "S1A" / PRECISE).read_bytes() == EOF_BODY
    state = pd.read_parquet(tmp_path / ORBIT_STATE_RELPATH)
    row = state.iloc[0]
    assert row["scene"] == SCENE and row["orbit_type"] == "POEORB"
    assert row["orbit_path"] == f"orbits/POEORB/S1A/{PRECISE}"
    # The restituted listing is not consulted when a precise orbit resolves.
    assert ORBIT_BASE_URLS["RESORB"] not in session.requests


def test_stage_orbits_is_idempotent_and_makes_no_requests_when_complete(tmp_path: Path):
    _stage(tmp_path, FakeSession({"POEORB": [PRECISE]}))
    session = FakeSession({}, fail_listing=True)
    summary = _stage(tmp_path, session)
    assert summary.reused == 1 and summary.staged == 0
    assert session.requests == []


def test_restituted_fallback_then_upgrade_to_precise(tmp_path: Path):
    first = _stage(tmp_path, FakeSession({"POEORB": [], "RESORB": [RESTITUTED]}))
    assert first.staged == 1
    assert pd.read_parquet(tmp_path / ORBIT_STATE_RELPATH).iloc[0]["orbit_type"] == "RESORB"

    second = _stage(tmp_path, FakeSession({"POEORB": [PRECISE]}))
    assert second.staged == 1
    state = pd.read_parquet(tmp_path / ORBIT_STATE_RELPATH)
    assert len(state) == 1 and state.iloc[0]["orbit_type"] == "POEORB"


def test_restituted_orbit_is_kept_while_precise_is_unpublished(tmp_path: Path):
    _stage(tmp_path, FakeSession({"POEORB": [], "RESORB": [RESTITUTED]}))
    session = FakeSession({"POEORB": [], "RESORB": [RESTITUTED]})
    summary = _stage(tmp_path, session)
    assert summary.reused == 1 and summary.failed == 0
    assert ORBIT_BASE_URLS["RESORB"] not in session.requests
    assert pd.read_parquet(tmp_path / ORBIT_STATE_RELPATH).iloc[0]["orbit_type"] == "RESORB"


def test_precise_policy_reports_missing_without_failing(tmp_path: Path):
    summary = _stage(
        tmp_path,
        FakeSession({"POEORB": [], "RESORB": [RESTITUTED]}),
        policy=S1OrbitPolicy.precise,
    )
    assert summary.missing == (SCENE,) and summary.failed == 0
    assert pd.read_parquet(tmp_path / ORBIT_STATE_RELPATH).iloc[0]["status"] == "missing"


def test_listing_failure_is_recorded_as_failed(tmp_path: Path):
    summary = _stage(tmp_path, FakeSession({}, fail_listing=True))
    assert summary.failed == 1 and summary.staged == 0


def test_non_orbit_products_and_none_policy_skip_network(tmp_path: Path):
    session = FakeSession({}, fail_listing=True)
    slc_metadata = pd.DataFrame({"url": ["https://example.test/one.zip"]})
    assert _stage(tmp_path, session, products=slc_metadata).staged == 0
    assert _stage(tmp_path, session, policy=S1OrbitPolicy.none).staged == 0
    assert session.requests == []


def test_invalid_orbit_body_is_rejected_and_not_left_behind(tmp_path: Path):
    class BadSession(FakeSession):
        def get(self, url, timeout=None, stream=False):
            if url.endswith(".EOF"):
                self.requests.append(url)
                return FakeResponse(content=b"<html>error</html>")
            return super().get(url, timeout, stream)

    summary = _stage(tmp_path, BadSession({"POEORB": [PRECISE]}))
    assert summary.failed == 1
    assert not any(p.is_file() for p in (tmp_path / "orbits").rglob("*"))


def test_download_cli_stages_orbits_and_honors_none(tmp_path: Path, monkeypatch):
    cache_dir = tmp_path / ".asf-cache"
    manifest = cache_dir / "query" / "manifest.parquet"
    manifest.parent.mkdir(parents=True)
    _manifest().to_parquet(manifest, index=False)
    config = tmp_path / "earthdata.netrc"
    config.write_text(
        "machine urs.earthdata.nasa.gov\n    login u\n    password p\n"
    )
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        lambda **kwargs: DownloadSummary(1, 0, 0, ["Results: 1 downloaded"]),
    )
    calls = []

    def fake_stage(products, outdir, policy, **kwargs):
        calls.append((outdir, policy))
        from sentinel_py.download.s1_orbits import OrbitStagingSummary

        return OrbitStagingSummary(staged=1, messages=["Orbit staging: 1 scene(s)"])

    monkeypatch.setattr("sentinel_py.cli.asf.download.stage_orbits", fake_stage)
    egm_calls = []
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.stage_egm96",
        lambda outdir, **kwargs: egm_calls.append(outdir) or outdir / "egm96.zip",
    )
    base = [
        "asf", "download", "--outdir", str(tmp_path / "out"), "--cache-dir",
        str(cache_dir), "--config", str(config), "--yes", "--log", str(tmp_path / "log"),
    ]
    result = CliRunner().invoke(app, base)
    assert result.exit_code == 0, result.stderr
    assert calls == [(tmp_path / "out", S1OrbitPolicy.precise_or_restituted)]
    assert "Orbit staging: 1 scene(s)" in result.stdout
    # The EGM96 grid SNAP needs is staged beside the orbits in the same command.
    assert egm_calls == [tmp_path / "out"]
    assert "EGM96 grid:" in result.stdout

    calls.clear()
    egm_calls.clear()
    result = CliRunner().invoke(app, [*base, "--orbits", "none"])
    assert result.exit_code == 0 and calls == [] and egm_calls == []


SECOND_SCENE = "S1A_IW_GRDH_1SDV_20240901T164404_20240901T164429_055699_06C000_ABCD"
SECOND_PRECISE = (
    "S1A_OPER_AUX_POEORB_OPOD_20240921T080000_V20240831T225942_20240902T005942.EOF"
)


class InterruptingSession(FakeSession):
    """Interrupt while downloading the second orbit file."""

    def get(self, url, timeout=None, stream=False):
        if url.endswith(SECOND_PRECISE):
            raise KeyboardInterrupt
        return super().get(url, timeout=timeout, stream=stream)


def test_interrupted_orbit_staging_keeps_resolved_scenes_and_resumes(tmp_path: Path):
    from sentinel_py.cache import REGISTRY
    from sentinel_py.pipeline.readers.s1 import attach_orbits

    products = pd.DataFrame(
        {
            "granule": [SCENE, SECOND_SCENE],
            "url": [f"https://example.test/{n}.zip" for n in (SCENE, SECOND_SCENE)],
        }
    )
    listings = {"POEORB": [PRECISE, SECOND_PRECISE]}

    with pytest.raises(KeyboardInterrupt):
        stage_orbits(
            products, tmp_path, S1OrbitPolicy.precise_or_restituted,
            session=InterruptingSession(listings), processes=1, retries=0,
            retry_backoff=0,
        )

    store = REGISTRY["s1_orbits"].store(tmp_path)
    recorded = store.read().set_index("scene")
    # The scene whose orbit finished downloading was checkpointed before the abort.
    assert list(recorded.index) == [SCENE]
    assert recorded.loc[SCENE, "status"] == "staged"
    assert store.part_paths()

    # Readers see the checkpoint without waiting for compaction.
    catalog = attach_orbits(pd.DataFrame({"scene": [SCENE, SECOND_SCENE]}), tmp_path)
    assert catalog.loc[0, "orbit_file"] == PRECISE
    assert pd.isna(catalog.loc[1, "orbit_file"])

    session = FakeSession(listings)
    summary = stage_orbits(
        products, tmp_path, S1OrbitPolicy.precise_or_restituted,
        session=session, processes=1, retries=0, retry_backoff=0,
    )

    assert summary.staged == 1 and summary.failed == 0 and not summary.missing
    assert not any(url.endswith(PRECISE) for url in session.requests)  # not refetched
    assert store.part_paths() == []
    final = pd.read_parquet(tmp_path / ORBIT_STATE_RELPATH)
    assert sorted(final["scene"]) == sorted([SCENE, SECOND_SCENE])
    assert set(final["status"]) == {"staged"}
