"""
Helpers for navigating Sentinel SAFE directories through the Copernicus Data Space
Ecosystem (CDSE). Although CDSE exposes these as OData “Nodes”, we use standard 
remote-sensing terminology:

- Collection: A group of scenes (e.g., Sentinel-2 MSI Level-2A)
- Scene: A single acquisition, stored as a <scene_id>.SAFE directory
- Scene contents: Files and subfolders inside the SAFE structure

Example SAFE Directory Structure
--------------------------------
Collection: Sentinel-2 MSI Level-2A
Scene: S2B_MSIL2A_20230928T190039_N0509_R013_T10VFU_20230928T221830.SAFE

S2B_MSIL2A_20230928T190039_N0509_R013_T10VFU_20230928T221830.SAFE/
    ├── MTD_MSIL2A.xml
    ├── GRANULE/
    │    └── L2A_T10VFU_A035123_20230928/
    │         └── IMG_DATA/
    │              ├── R10m/
    │              │    ├── T10VFU_20230928T190039_B02_10m.jp2
    │              │    ├── T10VFU_20230928T190039_B03_10m.jp2
    │              │    └── ...
    │              └── R20m/, R60m/, etc.
    └── AUX_DATA/
"""

import os
from typing import Iterable, Sequence
from urllib.parse import quote
import requests
from shapely.geometry.base import BaseGeometry
from collections.abc import Iterable


CDSE_BASE = "https://download.dataspace.copernicus.eu"

def scene_node_url(
    scene_id: str,
    *path_segments: str,
    list_children: bool = False,
) -> str:
    """
    Build a CDSE OData Nodes URL for a single scene (product).

    Parameters
    ----------
    scene_id : str
        CDSE product/scene identifier used in /odata/v1/Products(<scene_id>).
    path_segments : str
        Path components inside the SAFE directory
        (e.g., "<SCENE>.SAFE", "GRANULE", "L2A_...", "IMG_DATA", "R10m", filename).
    list_children : bool, optional
        If True, return the URL to list child nodes (directories/files).
        If False, return the URL to download a single file ($value).
    """
    path = f"{CDSE_BASE}/odata/v1/Products({scene_id})"
    for seg in path_segments:
        path += f"/Nodes({quote(str(seg), safe='')})"
    path += "/Nodes" if list_children else "/$value"
    return path


def list_scene_children(
    session: requests.Session,
    scene_id: str,
    *path_segments: str,
) -> list[dict]:
    """
    List child nodes (files/directories) under a given path inside a scene's SAFE tree.

    Examples
    --------
    # Top-level children of the scene (usually one <SCENE>.SAFE entry)
    list_scene_children(sess, scene_id)

    # Children under the SAFE root (e.g., GRANULE, AUX_DATA, MTD_MSIL2A.xml, ...)
    list_scene_children(sess, scene_id, "<SCENE>.SAFE")

    # Children under the IMG_DATA/R10m directory
    list_scene_children(sess, scene_id, "<SCENE>.SAFE", ..., "IMG_DATA", "R10m")
    """
    url = scene_node_url(scene_id, *path_segments, list_children=True)
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json().get("result", [])


def find_scene_safe_directory(session: requests.Session, scene_id: str) -> str:
    """
    Find the name of the SAFE root directory for a scene.
    The SAFE root is the outermost directory named <scene_id>.SAFE.

    Parameters
    ----------
    session : requests.Session
        Authenticated CDSE session.
    scene_id : str
        CDSE product/scene identifier.

    Returns
    -------
    safe_name : str
        Name of the SAFE root directory (e.g., "S2B_MSIL2A_...SAFE").

    Raises
    ------
    RuntimeError
        If no child nodes exist under the product, or no SAFE directory is found.
    """
    children = list_scene_children(session, scene_id)
    if not children:
        raise RuntimeError(
            f"No child nodes found for scene {scene_id}. "
            "Is the scene ID correct, and is the collection fully available?"
        )

    # look for a child node ending with .SAFE
    for child in children:
        nm = child.get("Name", "")
        if nm.endswith(".SAFE"):
            return nm

    # fallback: return the first child name
    return children[0].get("Name", "")


def extract_tile_from_name(scene_name: str) -> str | None:
    """
    Extract the Sentinel-2 MGRS tile ID (e.g., T10VFU) from a scene name, if present.
    """
    for part in str(scene_name).split("_"):
        if part.startswith("T") and len(part) == 6:
            return part
    return None


def choose_best_resolution(
    target_res: int,
    available_resolutions: Iterable[int],
) -> int | None:
    """
    Choose the best available resolution (in meters) given a target resolution.

    Rules:
    - If `target_res` is available, return it.
    - Otherwise, choose the largest resolution <= target_res (finer-but-not-coarser).
    - If no resolution <= target_res exists, fall back to the finest available
      (smallest number).
    """
    avail = sorted(set(int(r) for r in available_resolutions))
    if not avail:
        return None

    if target_res in avail:
        return target_res

    # resolutions finer or equal to target (numerically <=)
    finer_or_equal = [r for r in avail if r <= target_res]
    if finer_or_equal:
        return max(finer_or_equal)

    # all are coarser than requested; pick the finest available
    return min(avail)


def select_s2_targets(
    session: requests.Session,
    scene_id: str,
    scene_name: str,
    bands: Iterable[str],
    target_res_m: int,
    *,
    possible_resolutions: Iterable[int] = (10, 20, 60),
) -> tuple[list[tuple[str, ...]], str, str | None, dict[str, int | None]]:
    """
    Select band files for a Sentinel-2 scene at or above the requested resolution.

    Parameters
    ----------
    session : requests.Session
        Authenticated CDSE session.
    scene_id : str
        CDSE product/scene identifier (used in /Products(<scene_id>)).
    scene_name : str
        Human-readable scene name, used to parse the MGRS tile ID.
    bands : iterable of str
        Band IDs to fetch (e.g., ["B02", "B03", "B04"]).
    target_res_m : int
        Desired ground sampling distance in meters (e.g., 10, 20, 60).
    possible_resolutions : iterable of int, optional
        All resolutions that may exist in the SAFE archive (default (10,20,60)).

    Returns
    -------
    targets : list[tuple[str, ...]]
        List of node path segments for each selected file (SAFE root, GRANULE, IMG_DATA, Rxxm, filename).
    safe_root : str
        Name of the SAFE root directory.
    granule_dir : str | None
        Name of the selected GRANULE directory, if found.
    band_res_map : dict[str, int | None]
        Mapping of band ID -> chosen resolution (in meters), or None if not found.
    """
    # get the SAFE root directory name
    safe_root = find_scene_safe_directory(session, scene_id)

    # choose GRANULE directory within SAFE directory (by tile or fallback)
    tile = extract_tile_from_name(scene_name)
    granules = list_scene_children(session, scene_id, safe_root)
    granule_dir: str | None = None
    for g in granules:
        gname = g.get("Name", "")
        if (tile and tile in gname) or gname.startswith("L2A_"):
            granule_dir = gname
            break
    if not granule_dir and granules:
        granule_dir = granules[0].get("Name", "")

    # gather target file paths for download
    targets: list[tuple[str, ...]] = []
    band_res_map: dict[str, int | None] = {}
    if granule_dir:
        # get all possible band files organized by resolution
        nodes_by_res: dict[int, list[dict]] = {}
        for res in sorted(set(int(r) for r in possible_resolutions)):
            try:
                nodes_by_res[res] = list_scene_children(
                    session,
                    scene_id,
                    safe_root,
                    "GRANULE",
                    granule_dir,
                    "IMG_DATA",
                    f"R{res}m",
                )
            except requests.HTTPError:
                nodes_by_res[res] = []

        # for each user-desired band, select the best available resolution
        for band in bands:
            # determine which resolutions have this band available
            available_for_band: list[int] = []
            for res, nodes in nodes_by_res.items():
                suffix = f"_{band}_{res}m.jp2"
                if any(n.get("Name", "").endswith(suffix) for n in nodes):
                    available_for_band.append(res)
            if not available_for_band:
                band_res_map[band] = None
                continue

            # select the best resolution for this band
            chosen_res = choose_best_resolution(target_res_m, available_for_band)
            band_res_map[band] = chosen_res

            # get the corresponding file node
            nodes = nodes_by_res[chosen_res]
            suffix = f"_{band}_{chosen_res}m.jp2"
            hit = next(
                (n for n in nodes if n.get("Name", "").endswith(suffix)),
                None,
            )
            if hit:
                targets.append(
                    (
                        safe_root,
                        "GRANULE",
                        granule_dir,
                        "IMG_DATA",
                        f"R{chosen_res}m",
                        hit["Name"],
                    )
                )

        # always include band-level XML if we have a granule
        targets.append((safe_root, "GRANULE", granule_dir, "MTD_TL.xml"))

    # also always include scene-level XML
    targets.append((safe_root, "MTD_MSIL2A.xml"))
    return targets, safe_root, granule_dir, band_res_map