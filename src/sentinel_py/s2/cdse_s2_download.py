import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
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
) -> list[dict]:
    """
    Download a set of Sentinel-2 SAFE files concurrently.

    Uses simple content-based caching:
    - For each target, do a HEAD request to get Content-Length.
    - If a local file exists and its size matches the remote size, we treat it as
      complete and skip re-download.
    - Downloads go to a temporary *.part file and are only moved into place on success.
    - If the server advertises Content-Length, we verify that all bytes were written.

    Returns
    -------
    failures : list of dict
        A list of failures, each like:
        { "segments": segments, "status": "error: ..." }.
        If empty, all downloads completed successfully or were already cached.
    """

    output_root = Path(output_root)
    failures: list[dict] = []

    # helper to get remote file size via HEAD
    def _get_remote_size(url: str) -> int | None:
        """Return Content-Length from a HEAD request, or None if unavailable."""
        try:
            resp = session.head(url, timeout=timeout, allow_redirects=True)
        except Exception:
            return None
        if not resp.ok:
            return None
        cl = resp.headers.get("Content-Length")
        try:
            return int(cl) if cl is not None else None
        except (TypeError, ValueError):
            return None

    # download a single target
    def _download_one(segments: Sequence[str]) -> tuple[Sequence[str], str]:
        # local path
        rel = os.path.join(*segments)
        outpath = output_root / rel
        outpath.parent.mkdir(parents=True, exist_ok=True)
        tmp = outpath.with_suffix(outpath.suffix + ".part")

        # CDSE URL for this target
        url = scene_node_url(scene_id, *segments, list_children=False)

        # skip download: if local file exists and matches remote size
        remote_size = _get_remote_size(url)
        if outpath.exists() and remote_size is not None:
            local_size = outpath.stat().st_size
            if local_size == remote_size:
                # local file appears complete; treat as cached success
                return segments, "ok"

        # download/redownload URL
        try:
            resp = session.get(url, stream=True, timeout=timeout)
        except Exception as ex:
            return segments, f"error: request failed ({ex})"

        if resp.status_code != 200:
            return segments, f"error: HTTP {resp.status_code}"

        # prevent impartial downloads: determine expected size
        content_length = resp.headers.get("Content-Length")
        if content_length is not None:
            try:
                expected_bytes = int(content_length)
            except (TypeError, ValueError):
                expected_bytes = remote_size
        else:
            expected_bytes = remote_size

        # create fresh temp file; remove any stale .part from prior runs
        tmp.unlink(missing_ok=True)
        bytes_written = 0
        try:
            # stream download to temp file
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written += len(chunk)

            # if server told us how big the file should be, verify we got it all
            if expected_bytes is not None and bytes_written != expected_bytes:
                tmp.unlink(missing_ok=True)
                return (
                    segments,
                    f"error: incomplete download (expected {expected_bytes} bytes, "
                    f"got {bytes_written})",
                )

            # atomically move into place
            os.replace(tmp, outpath)
            return segments, "ok"

        except Exception as ex:
            # clean up partial temp file
            tmp.unlink(missing_ok=True)
            return segments, f"write error: {ex}"

    # run downloads concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_download_one, segs) for segs in targets]
        for fut in as_completed(futs):
            segments, status = fut.result()
            if status != "ok":
                failures.append({"segments": tuple(segments), "status": status})

    return failures