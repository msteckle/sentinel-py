from pathlib import Path

from typer.testing import CliRunner

from sentinel_py.cli.main import app

runner = CliRunner()


def test_legacy_s2_command_is_retired():
    result = runner.invoke(app, ["s2", "preprocess", "--help"])

    assert result.exit_code == 2
    assert "No such command 's2'" in result.output


def test_run_validate_only_does_not_require_gdal(tmp_path: Path):
    (tmp_path / "raw").mkdir()
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        """
version: 1
execution: {method: local}
selection: {years: [2024], speriod: "06-01", eperiod: "08-31"}
output_grid: {crs: native, resolution_m: 20}
sources:
  s2: {type: s2.l2a.local, data_dir: raw}
nodes:
  - {id: preprocess, type: s2.preprocess, source: s2, bands: [B02]}
outputs:
  result: {from: preprocess, path: output, format: vrt}
"""
    )

    result = runner.invoke(app, ["run", str(pipeline), "--validate-only"])

    assert result.exit_code == 0, result.output
    assert "Pipeline is valid" in result.output
    assert str((tmp_path / "raw").resolve()) in result.output


def test_run_reports_yaml_validation_errors(tmp_path: Path):
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("version: 2")

    result = runner.invoke(app, ["run", str(pipeline), "--validate-only"])

    assert result.exit_code == 1
    assert "Invalid pipeline" in result.stderr
