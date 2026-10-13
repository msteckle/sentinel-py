"""The ``run`` command: validation, summary output, and exit codes."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from helpers import runner, write_s2_pipeline_yaml
from sentinel_py.cli.main import app


def _result(statuses: list[str]) -> SimpleNamespace:
    started = datetime(2024, 1, 1, 12, 0, 0)
    workers = [SimpleNamespace(status=status) for status in statuses]
    return SimpleNamespace(
        started_at=started,
        ended_at=started,
        elapsed_seconds=1.5,
        output_results={"result": SimpleNamespace(results=workers)},
        outputs={},
    )


def test_validate_only_describes_the_pipeline_without_running_it(
    tmp_path: Path, monkeypatch
):
    path = write_s2_pipeline_yaml(tmp_path)
    called = []
    monkeypatch.setattr(
        "sentinel_py.pipeline.run.run_pipeline", lambda *a, **k: called.append(1)
    )

    result = runner.invoke(app, ["run", str(path), "--validate-only"])

    assert result.exit_code == 0, result.output
    assert "Source:        s2" in result.output
    assert "Node:          preprocess (s2.preprocess" in result.output
    assert "Pipeline is valid. No tasks were executed." in result.output
    assert called == []


def test_invalid_pipeline_reports_a_clear_error(tmp_path: Path):
    path = write_s2_pipeline_yaml(tmp_path, "unexpected: true")

    result = runner.invoke(app, ["run", str(path), "--validate-only"])

    assert result.exit_code != 0
    assert "Invalid pipeline" in result.output


def test_run_summarizes_written_and_skipped_tiles(tmp_path: Path, monkeypatch):
    path = write_s2_pipeline_yaml(tmp_path)
    monkeypatch.setattr(
        "sentinel_py.pipeline.run.run_pipeline",
        lambda *args, **kwargs: _result(["written", "written", "skipped"]),
    )

    result = runner.invoke(app, ["run", str(path)])

    assert result.exit_code == 0, result.output
    assert "COG tiles:   2 written, 1 skipped, 0 failed" in result.output


def test_run_exits_nonzero_when_a_tile_fails(tmp_path: Path, monkeypatch):
    path = write_s2_pipeline_yaml(tmp_path)
    monkeypatch.setattr(
        "sentinel_py.pipeline.run.run_pipeline",
        lambda *args, **kwargs: _result(["written", "failed"]),
    )

    result = runner.invoke(app, ["run", str(path)])

    assert result.exit_code == 1
    assert "1 failed" in result.output


def test_execution_errors_become_a_command_failure(tmp_path: Path, monkeypatch):
    path = write_s2_pipeline_yaml(tmp_path)

    def boom(*args, **kwargs):
        raise RuntimeError("worker exploded")

    monkeypatch.setattr("sentinel_py.pipeline.run.run_pipeline", boom)

    result = runner.invoke(app, ["run", str(path)])

    assert result.exit_code != 0
    assert "Pipeline execution failed: worker exploded" in result.output
