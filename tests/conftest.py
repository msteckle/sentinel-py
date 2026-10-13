"""Shared pytest configuration.

sentinel-py resolves its home directory (caches and logs) from ``SENTINEL_PY_HOME`` at
import time. The tests must never read or write a developer's real ``~/.sentinel-py``,
so the variable is pointed at a throwaway directory *before* any test module imports
the package, and the directory is removed when the session ends.
"""

from __future__ import annotations

import os
import shutil
import tempfile

_TEST_HOME = tempfile.mkdtemp(prefix="sentinel-py-test-home-")
os.environ["SENTINEL_PY_HOME"] = _TEST_HOME


def pytest_sessionfinish(session, exitstatus) -> None:
    """Remove the session's temporary sentinel-py home."""
    shutil.rmtree(_TEST_HOME, ignore_errors=True)
