from pathlib import Path

from typer.testing import CliRunner

from sentinel_py.cli.main import app

runner = CliRunner()


def test_preprocess_logs_gdal_startup_failure(tmp_path: Path, monkeypatch):
    """A GDAL failure should be recorded after the preprocess logger starts."""
    message = "native GDAL test failure"

    def fail_validation() -> str:
        raise RuntimeError(message)

    monkeypatch.setattr(
        "sentinel_py.s2.preprocess.validate_gdal_for_preprocessing",
        fail_validation,
    )
    log_prefix = tmp_path / "workflow"

    result = runner.invoke(
        app,
        [
            "s2",
            "preprocess",
            "--indir",
            str(tmp_path),
            "--outdir",
            str(tmp_path / "processed"),
            "--log",
            str(log_prefix),
        ],
    )

    assert result.exit_code == 1
    assert message in result.stderr
    logs = list(tmp_path.glob("workflow_*.log"))
    assert len(logs) == 1
    assert "GDAL preprocessing validation failed" in logs[0].read_text()
    assert message in logs[0].read_text()
