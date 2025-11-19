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
) -> list[dict]:
    if logger is None:
        logger = logging.getLogger(__name__)

    output_root = Path(output_root)
    failures: list[dict] = []

    def _get_remote_size(url: str) -> int | None:
        try:
            resp = session.head(url, timeout=timeout, allow_redirects=True)
        except Exception as ex:
            logger.warning("HEAD request failed for %s: %s", url, ex)
            return None

        if not resp.ok:
            logger.warning(
                "HEAD request for %s returned HTTP %s", url, resp.status_code
            )
            return None

        cl = resp.headers.get("Content-Length")
        try:
            return int(cl) if cl is not None else None
        except (TypeError, ValueError):
            logger.warning("Invalid Content-Length %r for %s", cl, url)
            return None

    def _download_one(segments: Sequence[str]) -> tuple[Sequence[str], str]:
        rel = os.path.join(*segments)
        outpath = output_root / rel
        outpath.parent.mkdir(parents=True, exist_ok=True)
        tmp = outpath.with_suffix(outpath.suffix + ".part")

        url = scene_node_url(scene_id, *segments, list_children=False)
        seg_str = "/".join(segments)

        logger.debug("Preparing download for %s (%s)", seg_str, url)

        remote_size = _get_remote_size(url)
        if outpath.exists() and remote_size is not None:
            local_size = outpath.stat().st_size
            if local_size == remote_size:
                logger.info("Cached OK: %s (size=%d)", outpath, local_size)
                return segments, "ok"
            else:
                logger.info(
                    "Re-downloading %s: local size %d != remote size %d",
                    outpath,
                    local_size,
                    remote_size,
                )

        try:
            resp = session.get(url, stream=True, timeout=timeout)
        except Exception as ex:
            logger.error("Request failed for %s: %s", url, ex)
            return segments, f"error: request failed ({ex})"

        if resp.status_code != 200:
            logger.error("HTTP %s for %s", resp.status_code, url)
            return segments, f"error: HTTP {resp.status_code}"

        content_length = resp.headers.get("Content-Length")
        if content_length is not None:
            try:
                expected_bytes = int(content_length)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid Content-Length %r for %s; falling back to HEAD result",
                    content_length,
                    url,
                )
                expected_bytes = remote_size
        else:
            expected_bytes = remote_size

        tmp.unlink(missing_ok=True)
        bytes_written = 0

        try:
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written += len(chunk)

            if expected_bytes is not None and bytes_written != expected_bytes:
                logger.error(
                    "Incomplete download for %s: expected %d bytes, got %d",
                    url,
                    expected_bytes,
                    bytes_written,
                )
                tmp.unlink(missing_ok=True)
                return (
                    segments,
                    f"error: incomplete download (expected {expected_bytes} bytes, "
                    f"got {bytes_written})",
                )

            os.replace(tmp, outpath)
            logger.info("Downloaded OK: %s (%d bytes)", outpath, bytes_written)
            return segments, "ok"

        except Exception as ex:
            tmp.unlink(missing_ok=True)
            logger.error("Write error for %s: %s", outpath, ex)
            return segments, f"write error: {ex}"

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_download_one, segs) for segs in targets]
        for fut in as_completed(futs):
            segments, status = fut.result()
            if status != "ok":
                seg_str = "/".join(segments)
                logger.error("Failure downloading %s: %s", seg_str, status)
                failures.append({"segments": tuple(segments), "status": status})

    if failures:
        logger.warning(
            "Completed downloads with %d failure(s) for scene %s",
            len(failures),
            scene_id,
        )
    else:
        logger.info(
            "All targets downloaded or cached successfully for scene %s",
            scene_id,
        )

    return failures
