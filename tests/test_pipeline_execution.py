from __future__ import annotations

import os

from dask.delayed import delayed

from sentinel_py.pipeline.config import ExecutionConfig
from sentinel_py.pipeline.execution import compute_dask_tasks


def _worker_pid() -> int:
    return os.getpid()


def test_local_dask_uses_an_isolated_worker_process():
    parent_pid = os.getpid()
    execution = ExecutionConfig(
        method="local",
        workers=1,
        threads_per_worker=2,
    )

    results = compute_dask_tasks((delayed(_worker_pid)(),), execution)

    assert results[0] != parent_pid


def _return_value(value: int) -> int:
    return value


def test_dask_progress_callback_preserves_result_order():
    execution = ExecutionConfig(
        method="local",
        workers=1,
        threads_per_worker=2,
    )
    completed: list[int] = []
    tasks = (delayed(_return_value)(1), delayed(_return_value)(2))

    results = compute_dask_tasks(
        tasks,
        execution,
        progress_callback=completed.append,
    )

    assert results == (1, 2)
    assert sorted(completed) == [1, 2]


def test_dask_task_progress_includes_dependency_graph():
    execution = ExecutionConfig(
        method="local",
        workers=1,
        threads_per_worker=1,
    )
    progress: list[tuple[int, int]] = []
    source = delayed(_return_value)(1)
    derived = delayed(_return_value)(source)

    results = compute_dask_tasks(
        (derived,),
        execution,
        task_progress_callback=lambda completed, total: progress.append(
            (completed, total)
        ),
    )

    assert results == (1,)
    assert progress
    assert progress[-1][0] == progress[-1][1]
    assert progress[-1][1] >= 2
