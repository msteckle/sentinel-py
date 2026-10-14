import io
import zipfile
from pathlib import Path

import pytest
import requests
from typer.testing import CliRunner

from sentinel_py.cli.main import app
from sentinel_py.download.base import DownloadSummary
from sentinel_py.download.s1_orbits import OrbitStagingSummary
from sentinel_py.download.snap_auxdata import (
    EGM96_MEMBER,
    EGM96_RELPATH,
    EGM96_URL,
    stage_egm96,
)


def _archive(member: str = EGM96_MEMBER) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(member, b"0" * 64)
    return buffer.getvalue()


class FakeResponse:
    def __init__(self, content: bytes = b"", status: int = 200):
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
    def __init__(self, *responses: FakeResponse):
        self.responses = list(responses)
        self.requests: list[str] = []

    def get(self, url, timeout=None, stream=False):
        self.requests.append(url)
        return self.responses.pop(0)


def _stage(tmp_path: Path, session: FakeSession) -> Path:
    return stage_egm96(tmp_path, session=session, retries=0, retry_backoff=0)


def test_stage_egm96_downloads_validates_and_reuses(tmp_path: Path):
    session = FakeSession(FakeResponse(_archive()))
    target = _stage(tmp_path, session)

    assert target == tmp_path / EGM96_RELPATH
    assert session.requests == [EGM96_URL]
    assert zipfile.is_zipfile(target)
    assert not list(target.parent.glob(".*.part"))

    again = FakeSession()
    assert _stage(tmp_path, again) == target
    assert again.requests == []


def test_stage_egm96_replaces_a_corrupt_existing_file(tmp_path: Path):
    target = tmp_path / EGM96_RELPATH
    target.parent.mkdir(parents=True)
    target.write_bytes(b"truncated")
    session = FakeSession(FakeResponse(_archive()))

    assert _stage(tmp_path, session) == target
    assert zipfile.is_zipfile(target) and len(session.requests) == 1


@pytest.mark.parametrize(
    "response",
    [FakeResponse(b"<html>error</html>"), FakeResponse(_archive("other.grd"))],
)
def test_stage_egm96_rejects_invalid_content_without_leaving_a_file(
    tmp_path: Path, response: FakeResponse
):
    with pytest.raises(OSError, match="not a valid archive"):
        _stage(tmp_path, FakeSession(response))
    assert not any(p.is_file() for p in (tmp_path / "auxdata").rglob("*"))


def test_stage_egm96_does_not_retry_client_errors(tmp_path: Path):
    session = FakeSession(FakeResponse(status=404), FakeResponse(_archive()))
    with pytest.raises(requests.HTTPError):
        stage_egm96(tmp_path, session=session, retries=3, retry_backoff=0)
    assert len(session.requests) == 1


def test_stage_egm96_retries_server_errors_then_succeeds(tmp_path: Path):
    session = FakeSession(FakeResponse(status=503), FakeResponse(_archive()))
    target = stage_egm96(tmp_path, session=session, retries=1, retry_backoff=0)
    assert zipfile.is_zipfile(target) and len(session.requests) == 2


def test_download_cli_skips_egm96_when_no_scenes_need_orbits(tmp_path: Path, monkeypatch):
    import pandas as pd

    cache_dir = tmp_path / ".asf-cache"
    manifest = cache_dir / "query" / "manifest.parquet"
    manifest.parent.mkdir(parents=True)
    pd.DataFrame({"url": ["https://example.test/one.zip"]}).to_parquet(
        manifest, index=False
    )
    config = tmp_path / "earthdata.netrc"
    config.write_text("machine urs.earthdata.nasa.gov\n    login u\n    password p\n")
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        lambda **kwargs: DownloadSummary(1, 0, 0, ["Results: 1 downloaded"]),
    )
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.stage_orbits",
        lambda *args, **kwargs: OrbitStagingSummary(messages=["no scenes"]),
    )
    calls = []
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.stage_egm96",
        lambda *args, **kwargs: calls.append(args),
    )
    result = CliRunner().invoke(
        app,
        [
            "asf", "download", "--outdir", str(tmp_path / "out"), "--cache-dir",
            str(cache_dir), "--config", str(config), "--yes", "--log",
            str(tmp_path / "log"),
        ],
    )
    assert result.exit_code == 0, result.stderr
    assert calls == []


def test_download_cli_fails_when_egm96_cannot_be_staged(tmp_path: Path, monkeypatch):
    import pandas as pd

    cache_dir = tmp_path / ".asf-cache"
    manifest = cache_dir / "query" / "manifest.parquet"
    manifest.parent.mkdir(parents=True)
    pd.DataFrame({"url": ["https://example.test/one.zip"]}).to_parquet(
        manifest, index=False
    )
    config = tmp_path / "earthdata.netrc"
    config.write_text("machine urs.earthdata.nasa.gov\n    login u\n    password p\n")
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.download_asf",
        lambda **kwargs: DownloadSummary(1, 0, 0, ["Results: 1 downloaded"]),
    )
    monkeypatch.setattr(
        "sentinel_py.cli.asf.download.stage_orbits",
        lambda *args, **kwargs: OrbitStagingSummary(staged=1, messages=["ok"]),
    )

    def offline(*args, **kwargs):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr("sentinel_py.cli.asf.download.stage_egm96", offline)
    result = CliRunner().invoke(
        app,
        [
            "asf", "download", "--outdir", str(tmp_path / "out"), "--cache-dir",
            str(cache_dir), "--config", str(config), "--yes", "--log",
            str(tmp_path / "log"),
        ],
    )
    assert result.exit_code == 1
    assert "EGM96 staging failed" in result.stderr
