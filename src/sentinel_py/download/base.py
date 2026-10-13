"""Primitives shared by the provider download modules (ASF, CDSE, PGC, orbits).

The helpers here cover behavior every downloader needs and previously copied:
exponential-backoff retries, ``.part`` temporary paths, a consistent Rich progress
bar, a thread-pool loop that checkpoints per-file state rows, and one summary type.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar
from urllib.parse import unquote, urlparse

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

T = TypeVar("T")


########################################################################################
# Paths and retries
########################################################################################


def part_path(directory: Path, filename: str) -> Path:
    """Return the temporary ``.part`` sibling used while a file is downloading.

    Parameters
    ----------
    directory : pathlib.Path
        Directory that will contain the finished file.
    filename : str
        Final file name.

    Returns
    -------
    pathlib.Path
        ``<directory>/.<filename>.part``.
    """
    return Path(directory) / f".{filename}.part"


def url_filename(url: str, fallback: str | None = None) -> str:
    """Return a safe local file name for a download URL.

    Parameters
    ----------
    url : str
        Source URL; the final path component is used.
    fallback : str, optional
        Name (or path whose final component is used) when the URL has no name.

    Returns
    -------
    str
        A bare file name, never ``""``, ``"."``, ``".."`` or a path.

    Raises
    ------
    ValueError
        If no safe file name can be determined.
    """
    name = Path(unquote(urlparse(url).path)).name
    if not name and fallback:
        name = Path(str(fallback)).name
    if name in {"", ".", ".."}:
        raise ValueError(f"Could not determine a safe file name for URL: {url}")
    return name


def retry_call(
    operation: Callable[[], T],
    *,
    retries: int,
    backoff: float,
    is_retryable: Callable[[Exception], bool] | None = None,
    on_retry: Callable[[Exception, int, float], None] | None = None,
    cleanup: Callable[[], None] | None = None,
) -> T:
    """Call ``operation`` with exponential backoff between failed attempts.

    ``KeyboardInterrupt`` and other non-``Exception`` errors are never retried, but
    ``cleanup`` still runs so partial files do not survive an interrupt.

    Parameters
    ----------
    operation : Callable[[], T]
        Zero-argument callable performing one full attempt.
    retries : int
        Retries after the first attempt (total attempts are ``retries + 1``).
    backoff : float
        Base delay in seconds; attempt ``n`` (0-based) waits ``backoff * 2**n``.
    is_retryable : Callable[[Exception], bool], optional
        Return ``False`` to re-raise immediately. Defaults to retrying everything.
    on_retry : Callable[[Exception, int, float], None], optional
        Called as ``on_retry(error, attempt, delay)`` before each sleep.
    cleanup : Callable[[], None], optional
        Called after every failed or interrupted attempt, e.g. to remove a partial
        file.

    Returns
    -------
    T
        The value returned by the first successful attempt.

    Raises
    ------
    Exception
        The last error when attempts are exhausted or it is not retryable.
    """
    attempt = 0
    while True:
        try:
            return operation()
        except BaseException as error:
            if cleanup is not None:
                cleanup()
            if not isinstance(error, Exception):
                raise
            if attempt >= retries or (is_retryable and not is_retryable(error)):
                raise
            delay = backoff * (2**attempt)
            if on_retry is not None:
                on_retry(error, attempt, delay)
            time.sleep(delay)
            attempt += 1


########################################################################################
# Progress, summaries, and the download pool
########################################################################################


def make_progress(*, eta: bool = True, status: bool = False) -> Progress:
    """Build the Rich progress bar used by every long-running download step.

    Parameters
    ----------
    eta : bool, default True
        Include the remaining-time column.
    status : bool, default False
        Include a trailing ``status`` text field (set via ``progress.update``).

    Returns
    -------
    rich.progress.Progress
        Unstarted progress instance; use as a context manager.
    """
    columns: list[Any] = [
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ]
    if eta:
        columns.append(TimeRemainingColumn())
    if status:
        columns.append(TextColumn("[cyan]{task.fields[status]}"))
    return Progress(*columns)


def status_text(
    downloaded: int, skipped: int, failed: int, cache_updated: int | None = None
) -> str:
    """Format the per-run counters shown beside a download progress bar."""
    text = f"downloaded {downloaded} · skipped {skipped} · failed {failed}"
    if cache_updated is not None:
        text += f" · cache updated {cache_updated}"
    return text


@dataclass(frozen=True)
class DownloadSummary:
    """Counts and user-facing messages from a provider download invocation."""

    downloaded: int
    skipped: int
    failed: int
    messages: list[str]

    def __iter__(self):
        """Iterate over the user-facing summary messages."""
        return iter(self.messages)

    def __getitem__(self, index):
        """Index the user-facing summary messages."""
        return self.messages[index]


class RowSink(Protocol):
    """Anything that accepts completed state rows (e.g. ``RowWriter``)."""

    def add(self, rows: Iterable[dict[str, Any]]) -> None:
        """Buffer or persist completed state rows."""


def run_download_pool(
    records: list[dict[str, Any]],
    worker: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    processes: int,
    description: str,
    sink: RowSink,
    show_cache_updated: bool = False,
) -> tuple[int, int, int]:
    """Run per-file download workers in a thread pool with progress and checkpoints.

    Every worker returns a state row whose ``last_action`` is ``"downloaded"``,
    ``"skipped"``, or ``"failed"``. Rows are handed to ``sink`` as they finish so an
    interrupted run keeps completed work; queued tasks are cancelled on interrupt.

    Parameters
    ----------
    records : list[dict[str, Any]]
        Product records, one per file.
    worker : Callable[[dict[str, Any]], dict[str, Any]]
        Function downloading or verifying one record and returning its state row.
    processes : int
        Maximum concurrent workers.
    description : str
        Progress-bar label.
    sink : RowSink
        Receives each completed state row.
    show_cache_updated : bool, default False
        Append the number of state rows written to the status text.

    Returns
    -------
    tuple[int, int, int]
        Counts of downloaded, skipped, and failed files.
    """
    downloaded = skipped = failed = 0
    with make_progress(status=True) as progress:
        task = progress.add_task(
            description,
            total=len(records),
            status=status_text(0, 0, 0, 0 if show_cache_updated else None),
        )
        with ThreadPoolExecutor(max_workers=processes) as pool:
            futures = [pool.submit(worker, record) for record in records]
            try:
                for future in as_completed(futures):
                    row = future.result()
                    sink.add([row])
                    action = row["last_action"]
                    if action == "downloaded":
                        downloaded += 1
                    elif action == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                    progress.update(
                        task,
                        advance=1,
                        status=status_text(
                            downloaded,
                            skipped,
                            failed,
                            downloaded + skipped + failed if show_cache_updated else None,
                        ),
                    )
            except BaseException:
                # Do not start the remaining queued downloads after an interrupt.
                pool.shutdown(wait=False, cancel_futures=True)
                raise
    return downloaded, skipped, failed
