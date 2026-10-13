"""Characterization tests for ``ExecutionConfig.from_mapping`` validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from sentinel_py.enums import ExecutionMethod
from sentinel_py.pipeline.config import ExecutionConfig, PipelineConfigError

SLURM = {
    "method": "slurm",
    "jobs": 4,
    "cores_per_job": 8,
    "processes_per_job": 4,
    "memory_per_job": "16GB",
    "walltime": "01:00:00",
}


def _parse(raw, tmp_path: Path) -> ExecutionConfig:
    return ExecutionConfig.from_mapping(raw, tmp_path)


def test_local_defaults_and_values(tmp_path: Path):
    config = _parse({"method": "local", "workers": 3, "threads_per_worker": 2}, tmp_path)

    assert config.method == ExecutionMethod.local
    assert (config.workers, config.threads_per_worker) == (3, 2)
    assert config.local_directory is None
    assert config.job_extra_directives == () and config.job_script_prologue == ()


def test_dask_requires_scheduler_address(tmp_path: Path):
    with pytest.raises(PipelineConfigError, match="scheduler_address is required"):
        _parse({"method": "dask"}, tmp_path)

    config = _parse({"method": "dask", "scheduler_address": "tcp://h:8786"}, tmp_path)
    assert config.scheduler_address == "tcp://h:8786"


def test_slurm_accepts_a_complete_configuration(tmp_path: Path):
    raw = {
        **SLURM,
        "queue": "short",
        "account": "proj",
        "interface": "ib0",
        "local_directory": "scratch",
        "job_extra_directives": ["--gres=none"],
        "job_script_prologue": ["module load gdal"],
    }

    config = _parse(raw, tmp_path)

    assert config.method == ExecutionMethod.slurm
    assert (config.jobs, config.cores_per_job, config.processes_per_job) == (4, 8, 4)
    assert config.local_directory == (tmp_path / "scratch").resolve()
    assert config.job_extra_directives == ("--gres=none",)
    assert config.job_script_prologue == ("module load gdal",)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"method": "gpu"}, "method must be local, dask, or slurm"),
        ({}, "method"),
        ({"method": "local", "bogus": 1}, "bogus"),
        ({"method": "local", "workers": 0}, "workers"),
        ({"method": "local", "threads_per_worker": "two"}, "threads_per_worker"),
        (
            {"method": "local", "scheduler_address": "tcp://h:8786"},
            "Unsupported local execution value",
        ),
        (
            {"method": "dask", "scheduler_address": "tcp://h:1", "workers": 2},
            "Unsupported external Dask execution value",
        ),
        ({"method": "dask", "scheduler_address": ""}, "scheduler_address must be"),
        ({**SLURM, "workers": 2}, "Unsupported Slurm execution value"),
        ({**SLURM, "jobs": None}, "Missing required Slurm execution value"),
        ({**SLURM, "walltime": None}, "Missing required Slurm execution value"),
        (
            {**SLURM, "cores_per_job": 2, "processes_per_job": 4},
            "processes_per_job cannot exceed",
        ),
        ({**SLURM, "job_extra_directives": "x"}, "job_extra_directives must be a list"),
        ({**SLURM, "job_script_prologue": [1]}, "job_script_prologue must be a list"),
        ({**SLURM, "queue": "  "}, "must be non-empty strings"),
    ],
)
def test_invalid_execution_sections_are_rejected(raw, message, tmp_path: Path):
    with pytest.raises(PipelineConfigError, match=message):
        _parse(raw, tmp_path)
