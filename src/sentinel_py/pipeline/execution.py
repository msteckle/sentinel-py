"""Dask execution backends shared by pipeline output writers."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from sentinel_py.pipeline.config import ExecutionConfig


class _ProgressListener:
    """Run Dask Distributed's dependency-aware progress listener in a thread."""

    def __init__(
        self,
        futures: list[Any],
        callback: Callable[[int, int], None],
        scheduler: str,
    ):
        from distributed.diagnostics.progressbar import ProgressBar
        from distributed.utils import LoopRunner

        class RichProgressBar(ProgressBar):
            """Adapt Distributed ProgressBar updates to a callback."""

            def _draw_bar(self, remaining: int, all: int, **kwargs: Any) -> None:
                del kwargs
                callback(all - remaining, all)

            def _draw_stop(
                self,
                remaining: int,
                all: int,
                **kwargs: Any,
            ) -> None:
                del kwargs
                callback(all - remaining, all)

        self._bar = RichProgressBar(
            futures,
            scheduler=scheduler,
            interval="100ms",
            complete=True,
        )
        self._runner = LoopRunner()
        self._thread = threading.Thread(
            target=self._run,
            name="sentinel-py-progress",
            daemon=True,
        )

    def _run(self) -> None:
        """Listen for scheduler progress updates until the graph completes."""
        try:
            self._runner.run_sync(self._bar.listen)
        except Exception:
            # Progress rendering must not mask the computation's own result or error.
            pass

    def start(self) -> None:
        """Start listening for scheduler progress updates."""
        self._thread.start()

    def close(self) -> None:
        """Stop the listener and wait for its event loop to finish."""
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            communication = getattr(self._bar, "comm", None)
            if communication is not None:
                communication.abort()
            self._thread.join(timeout=1.0)


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
    *,
    progress_callback: Callable[[Any], None] | None = None,
    task_progress_callback: Callable[[int, int], None] | None = None,
    initial_completed_tasks: int = 0,
) -> tuple[Any, ...]:
    """Compute one graph and optionally report each completed task."""
    if not tasks:
        return ()
    try:
        import dask  # noqa: F401
    except ImportError as error:
        raise RuntimeError(
            "Dask execution is not installed. Install the processing dependencies."
        ) from error
    client, cluster = _dask_client(execution)
    progress_listener: _ProgressListener | None = None
    try:
        futures = client.compute(tuple(tasks), optimize_graph=False)
        if task_progress_callback is not None:

            def update_progress(completed: int, total: int) -> None:
                task_progress_callback(
                    initial_completed_tasks + completed,
                    initial_completed_tasks + total,
                )

            progress_listener = _ProgressListener(
                list(futures),
                update_progress,
                client.scheduler.address,
            )
            progress_listener.start()

        results: list[Any] = [None] * len(futures)
        future_indices = {future: index for index, future in enumerate(futures)}
        from distributed import as_completed

        for future, result in as_completed(futures, with_results=True):
            results[future_indices[future]] = result
            if progress_callback is not None:
                progress_callback(result)
        return tuple(results)
    finally:
        if progress_listener is not None:
            progress_listener.close()
        client.close()
        if cluster is not None:
            cluster.close()
