"""Stage SNAP auxiliary data that terrain correction fetches at run time.

SNAP's ``Terrain-Correction`` downloads the EGM96 geoid grid on first use, even when
an external DEM with ellipsoidal heights (such as ArcticDEM) is supplied. Compute
nodes are frequently offline, so the grid is staged during the networked download
stage under ``<out_dir>/auxdata/dem/egm96`` and linked into each run's SNAP user
directory by the processing stage.
"""

from __future__ import annotations

import logging
import os
import zipfile
from pathlib import Path

import requests

from sentinel_py.download.base import part_path, retry_call

EGM96_URL = "https://step.esa.int/auxdata/dem/egm96/ww15mgh_b.zip"
# Mirrors the SNAP user-directory layout so the staged tree can be linked as-is.
EGM96_RELPATH = Path("auxdata") / "dem" / "egm96" / "ww15mgh_b.zip"
EGM96_MEMBER = "ww15mgh_b.grd"


def _valid_egm96(path: Path) -> bool:
    """Return whether ``path`` is an intact archive containing the EGM96 grid."""
    try:
        with zipfile.ZipFile(path) as archive:
            return EGM96_MEMBER in archive.namelist() and archive.testzip() is None
    except (OSError, zipfile.BadZipFile):
        return False


def stage_egm96(
    out_dir: Path,
    *,
    session: requests.Session | None = None,
    retries: int = 3,
    retry_backoff: float = 2.0,
    logger: logging.Logger | None = None,
) -> Path:
    """Download the EGM96 geoid grid used by SNAP terrain correction, once.

    An existing intact archive is reused. The file is written to a ``.part``
    sibling, validated, and atomically renamed, so an interrupted download never
    leaves a corrupt grid that SNAP would silently use.

    Parameters
    ----------
    out_dir : pathlib.Path
        Download directory; the grid is stored at
        ``<out_dir>/auxdata/dem/egm96/ww15mgh_b.zip``.
    session : requests.Session, optional
        HTTP session (injectable for tests).
    retries : int
        Retries after the first attempt for transient network errors.
    retry_backoff : float
        Base delay in seconds for exponential retry backoff.
    logger : logging.Logger, optional
        Logger for cache decisions and retries.

    Returns
    -------
    pathlib.Path
        Location of the staged archive.

    Raises
    ------
    requests.RequestException
        If the download fails after all retries.
    OSError
        If the downloaded file is not a valid EGM96 archive.
    """
    logger = logger or logging.getLogger(__name__)
    target = Path(out_dir) / EGM96_RELPATH
    if target.is_file() and _valid_egm96(target):
        logger.info("EGM96 grid already staged: %s", target)
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    partial = part_path(target.parent, target.name)
    session = session or requests.Session()

    def fetch() -> Path:
        """Stream the archive, validate it, and move it into place."""
        response = session.get(EGM96_URL, timeout=120, stream=True)
        response.raise_for_status()
        with partial.open("wb") as stream:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                stream.write(chunk)
        if not _valid_egm96(partial):
            raise OSError(f"downloaded EGM96 file is not a valid archive: {EGM96_URL}")
        os.replace(partial, target)
        return target

    def is_retryable(error: Exception) -> bool:
        """Retry network errors and server-side failures, not corrupt content."""
        if not isinstance(error, requests.RequestException):
            return False
        status = getattr(getattr(error, "response", None), "status_code", None)
        return status is None or status == 429 or status >= 500

    def log_retry(error: Exception, attempt: int, delay: float) -> None:
        """Log a retryable EGM96 download failure."""
        logger.warning(
            "Retrying EGM96 download after attempt %d/%d failed: %s; waiting %.1fs",
            attempt + 1,
            retries + 1,
            error,
            delay,
        )

    logger.info("Downloading EGM96 grid to %s", target)
    return retry_call(
        fetch,
        retries=retries,
        backoff=retry_backoff,
        is_retryable=is_retryable,
        on_retry=log_retry,
        cleanup=lambda: partial.unlink(missing_ok=True),
    )
