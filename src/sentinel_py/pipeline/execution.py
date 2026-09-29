"""Execution backends for pipeline preprocessing tasks."""

from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from sentinel_py.pipeline.config import ExecutionConfig
from sentinel_py.s2.base import S2Granule, S2PreprocessConfig, S2PreprocessResult


def _preprocess_one(
    granule: S2Granule,
    config: S2PreprocessConfig,
    output_dir: Path,
    cached_row: dict[str, object] | None,
) -> S2PreprocessResult:
    from sentinel_py.s2.preprocess import preprocess_s2_granule

    return preprocess_s2_granule(granule, config, output_dir, cached_row)


def _cached_row(
    granule: S2Granule,
    rows: dict[tuple[str, str], dict[str, object]],
) -> dict[str, object] | None:
    return rows.get((granule.product_id, granule.granule_id))


def _run_local(
    granules: list[S2Granule],
    config: S2PreprocessConfig,
    output_dir: Path,
    cached_rows: dict[tuple[str, str], dict[str, object]],
    execution: ExecutionConfig,
) -> list[S2PreprocessResult]:
    if execution.workers == 1:
        return [
            _preprocess_one(granule, config, output_dir, _cached_row(granule, cached_rows))
            for granule in granules
        ]
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(
        max_workers=execution.workers, mp_context=context
    ) as executor:
        futures = [
            executor.submit(
                _preprocess_one,
                granule,
                config,
                output_dir,
                _cached_row(granule, cached_rows),
            )
            for granule in granules
        ]
        return [future.result() for future in as_completed(futures)]


def _dask_client(execution: ExecutionConfig):
    try:
        from distributed import Client
    except ImportError as error:
        raise RuntimeError(
            "Dask execution is not installed. Install the processing dependencies."
        ) from error

    if execution.method == "dask":
        return Client(execution.scheduler_address), None

    try:
        from dask_jobqueue import SLURMCluster
    except ImportError as error:
        raise RuntimeError(
            "Slurm execution requires dask-jobqueue. Install the hpc dependencies."
        ) from error
    kwargs = {
        "cores": execution.cores_per_job,
        "processes": execution.processes_per_job,
        "memory": execution.memory_per_job,
        "walltime": execution.walltime,
        "local_directory": (
            str(execution.local_directory) if execution.local_directory else None
        ),
        "job_extra_directives": list(execution.job_extra_directives),
        "job_script_prologue": list(execution.job_script_prologue),
    }
    if execution.queue is not None:
        kwargs["queue"] = execution.queue
    if execution.account is not None:
        kwargs["account"] = execution.account
    if execution.interface is not None:
        kwargs["interface"] = execution.interface
    cluster = SLURMCluster(**kwargs)
    cluster.scale(jobs=execution.jobs)
    return Client(cluster), cluster


def _run_dask(
    granules: list[S2Granule],
    config: S2PreprocessConfig,
    output_dir: Path,
    cached_rows: dict[tuple[str, str], dict[str, object]],
    execution: ExecutionConfig,
) -> list[S2PreprocessResult]:
    try:
        from distributed import as_completed
    except ImportError as error:
        raise RuntimeError(
            "Dask execution is not installed. Install the processing dependencies."
        ) from error
    client, cluster = _dask_client(execution)
    try:
        futures = [
            client.submit(
                _preprocess_one,
                granule,
                config,
                output_dir,
                _cached_row(granule, cached_rows),
                pure=False,
            )
            for granule in granules
        ]
        return [future.result() for future in as_completed(futures)]
    finally:
        client.close()
        if cluster is not None:
            cluster.close()


def run_preprocess_tasks(
    granules: list[S2Granule],
    config: S2PreprocessConfig,
    output_dir: Path,
    cached_rows: dict[tuple[str, str], dict[str, object]],
    execution: ExecutionConfig,
) -> list[S2PreprocessResult]:
    """Execute preprocessing with the configured local or distributed backend."""
    if execution.method == "local":
        return _run_local(granules, config, output_dir, cached_rows, execution)
    return _run_dask(granules, config, output_dir, cached_rows, execution)
