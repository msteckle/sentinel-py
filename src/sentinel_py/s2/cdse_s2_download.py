from pathlib import Path
from typing import Iterable, Sequence
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from sentinel_py.s2.cdse_s2_nodes import scene_node_url


def download_s2_targets(
    session: requests.Session,
    scene_id: str,
    targets: Iterable[Sequence[str]],
    output_root: str | Path,
    *,
    chunk_mb: int = 4,
    timeout: int = 300,
    max_workers: int = 4,
) -> list[dict]:
    """
    Download a set of Sentinel-2 SAFE files concurrently.

    Parameters
    ----------
    session : requests.Session
        Authenticated CDSE session (AutoRefreshSession recommended).
    scene_id : str
        CDSE product/scene identifier.
    targets : iterable of sequences of str
        Each item is a tuple/list of SAFE path segments, e.g.
        ("S2B_MSIL2A_...SAFE", "GRANULE", "L2A_...", "IMG_DATA", "R10m", "Txx_B02_10m.jp2")
    output_root : str or Path
        Root directory where the SAFE tree will be written.
    chunk_mb : int, optional
        Download chunk size in MiB.
    timeout : int, optional
        Request timeout in seconds.
    max_workers : int, optional
        Maximum number of concurrent downloads.

    Returns
    -------
    failures : list of dict
        A list of failures, each like:
        { "segments": segments, "status": "error: ..." }.
        If empty, all downloads completed successfully.
    """

    output_root = Path(output_root)
    failures: list[dict] = []

    def _download_one(segments: Sequence[str]) -> tuple[Sequence[str], str]:
        # local path
        rel = os.path.join(*segments)
        outpath = output_root / rel
        outpath.parent.mkdir(parents=True, exist_ok=True)

        # CDSE URL for this SAFE file
        url = scene_node_url(scene_id, *segments, list_children=False)
        resp = session.get(url, stream=True, timeout=timeout)
        if resp.status_code != 200:
            return segments, f"error: HTTP {resp.status_code}"

        tmp = outpath.with_suffix(outpath.suffix + ".part")

        try:
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_mb * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp, outpath)
            return segments, "ok"
        except Exception as ex:
            if tmp.exists():
                tmp.unlink()
            return segments, f"write error: {ex}"

    # run downloads concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_download_one, segs) for segs in targets]
        for fut in as_completed(futs):
            segments, status = fut.result()
            if status != "ok":
                failures.append({"segments": tuple(segments), "status": status})

    return failures