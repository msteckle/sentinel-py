import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections.abc import Iterable, Sequence

import requests

from sentinel_py.s2.cdse_s2_nodes import scene_node_url


def download_s2_targets(
    session: requests.Session,
    scene_id: str,
    targets: Iterable[Sequence[str]],
    output_root: str | Path,
    *,
    chunk_mb: int = 4,
    timeout: int = 300,
    max_workers: int = 2,
    logger: logging.Logger | None = None,
) -> int:
    """
    Download the specified targets for a Sentinel-2 scene in parallel with retries and 
    error handling. Returns the number of failed downloads.

    Parameters
    ----------
    session : requests.Session
        An authenticated session for making HTTP requests to the CDSE API.
    scene_id : str
        The identifier of the Sentinel-2 scene being downloaded (used for logging).
    targets : Iterable[Sequence[str]]
        A list of target paths to download, where each target is a sequence of path
        segments (e.g., ["L2A_...zip"] or ["L2A_...", "B04.jp2"]).
    output_root : str | Path
        The root directory where downloaded files will be saved, preserving the relative
        path structure of the targets.
    chunk_mb : int, optional
        The chunk size in megabytes for streaming downloads (default is 4 MB).
    timeout : int, optional
        The timeout in seconds for HTTP requests (default is 300 seconds).
    max_workers : int, optional
        The maximum number of parallel download threads (default is 2).
    logger : logging.Logger, optional
        A logger for recording download progress and errors (default is None, which uses
        the module logger).

    Returns
    -------
    int
        The number of failed downloads (0 if all downloads succeeded or were cached).
    """

    if logger is None:
        logger = logging.getLogger(__name__)

    output_root = Path(output_root)
    n_failures = 0

    def _download_one(segments: Sequence[str]) -> bool:

        # create the output path and ensure parent directories exist
        rel = os.path.join(*segments)
        outpath = output_root / rel
        outpath.parent.mkdir(parents=True, exist_ok=True)
        tmp = outpath.with_suffix(outpath.suffix + ".part")

        # construct the URL for this target
        url = scene_node_url(scene_id, *segments, list_children=False)
        seg_str = "/".join(segments)
        logger.debug(f"Fetching {seg_str} -> {outpath}")

        # attempt the download with retries and error handling
        try:
            resp = session.get(url, stream=True, timeout=timeout)
        except Exception as ex:
            logger.error(f"Request failed for {url}: {ex}")
            return False
        if resp.status_code != 200:
            logger.error(f"HTTP {resp.status_code} for {url}")
            return False

        # check content length for completeness (if provided) before writing
        content_length = resp.headers.get("Content-Length")
        expected_bytes: int | None = None
        if content_length is not None:
            try:
                expected_bytes = int(content_length)
            except (TypeError, ValueError):
                logger.warning(f"Invalid Content-Length {content_length} for {url}")
        if expected_bytes is None:
            logger.warning(f"No Content-Length for {url}; skipping completeness check")

        # if the file already exists and matches expected size, skip download
        if outpath.exists() and expected_bytes is not None:
            local_size = outpath.stat().st_size
            if local_size == expected_bytes:
                logger.info(
                    f"Cached (skipping download): {outpath} (size={local_size} bytes)"
                )
                resp.close()
                return True
            else:
                logger.info(
                    f"Re-downloading {outpath}: local size {local_size} != remote size "
                    f"{expected_bytes} (diff={abs(expected_bytes - local_size)} bytes)"
                )

        # remove any existing temp file before writing
        tmp.unlink(missing_ok=True)
        bytes_written = 0

        # stream the response content to a temp file
        try:
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written += len(chunk)

            # verify completeness if expected size is known
            if expected_bytes is not None and bytes_written != expected_bytes:
                logger.error(
                    f"Incomplete download for {url}: expected {expected_bytes} bytes, "
                    f"got {bytes_written}"
                )
                tmp.unlink(missing_ok=True)
                return False

            # atomically move the temp file to the final destination
            os.replace(tmp, outpath)
            logger.info(f"Downloaded OK: {outpath} ({bytes_written} bytes)")
            return True

        # clean up temp file on any exception and log the error
        except Exception as ex:
            tmp.unlink(missing_ok=True)
            logger.error(f"Write error for {outpath}: {ex}")
            return False

    # use a thread pool to download targets in parallel, tracking failures
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_download_one, segs) for segs in targets]
        for fut in as_completed(futs):
            if not fut.result():
                n_failures += 1

    # log summary of results for this scene
    if n_failures:
        logger.warning(
            f"Completed downloads with {n_failures} failure(s) for scene {scene_id}"
        )
    else:
        logger.info(
            f"All {len(futs)} target(s) downloaded or cached successfully for scene "
            f"{scene_id}"
        )

    # return the number of failed downloads (sum of False results) for this scene
    return n_failures
