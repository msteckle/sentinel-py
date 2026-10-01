"""Dask execution backends shared by pipeline output writers."""

from __future__ import annotations

from typing import Any

from sentinel_py.pipeline.config import ExecutionConfig


def _collect_results(*results: Any) -> tuple[Any, ...]:
    return tuple(results)


def _dask_client(execution: ExecutionConfig):
    try:
        from distributed import Client, LocalCluster
    except ImportError as error:
        raise RuntimeError(
            "Dask execution is not installed. Install the processing dependencies."
        ) from error

    if execution.method == "local":
        cluster = LocalCluster(
            n_workers=execution.workers,
            threads_per_worker=execution.threads_per_worker,
            processes=True,
            dashboard_address=None,
        )
        return Client(cluster), cluster
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


def compute_dask_tasks(
    tasks: tuple[Any, ...] | list[Any],
    execution: ExecutionConfig,
) -> tuple[Any, ...]:
    """Compute one combined graph on local, external, or Slurm Dask."""
    if not tasks:
        return ()
    try:
        from dask import delayed
    except ImportError as error:
        raise RuntimeError(
            "Dask execution is not installed. Install the processing dependencies."
        ) from error
    combined = delayed(_collect_results, pure=False)(*tasks)
    client, cluster = _dask_client(execution)
    try:
        future = client.compute(combined, optimize_graph=False)
        return tuple(client.gather(future))
    finally:
        client.close()
        if cluster is not None:
            cluster.close()
