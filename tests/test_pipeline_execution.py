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
