import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from shapely import wkt as shapely_wkt
from shapely.geometry import MultiPolygon, Polygon, shape

CDSE_ODATA = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
console = Console()


def _is_missing(gf) -> bool:
    if gf is None:
        return True
    if isinstance(gf, float) and pd.isna(gf):
        return True
    if isinstance(gf, str) and not gf.strip():
        return True
    if isinstance(gf, (np.ndarray, list, tuple)) and len(gf) == 0:
        return True
    return False


def _coords_to_polygon(coords):
    """Build a shapely Polygon/MultiPolygon from raw coordinate arrays.
    Uses pure-Python iteration to avoid numpy ragged-array errors when
    coordinates come from parquet as object-nested structures."""

    def _depth(x):
        d = 0
        cur = x
        while isinstance(cur, (list, tuple, np.ndarray)) and len(cur) > 0:
            d += 1
            cur = cur[0]
        return d

    def _ring(r):
        # Explicit element access avoids numpy trying to build a uniform
        # array from object-nested coordinate data.
        return [(float(p[0]), float(p[1])) for p in r]

    d = _depth(coords)
    if d == 3:  # Polygon: rings -> points -> [x, y]
        rings = [_ring(r) for r in coords]
        return Polygon(rings[0], holes=rings[1:] if len(rings) > 1 else None)
    if d == 4:  # MultiPolygon
        polys = []
        for poly in coords:
            rings = [_ring(r) for r in poly]
            polys.append(Polygon(rings[0], holes=rings[1:] if len(rings) > 1 else None))
        return MultiPolygon(polys)
    raise ValueError(f"Unrecognized coordinate nesting depth: {d}")


def to_wkt(gf) -> str | None:
    """Convert a GeoFootprint of any reasonable form into a WKT string."""
    if _is_missing(gf):
        return None

    if isinstance(gf, str):
        s = gf.strip()
        if s.startswith("{"):
            return shape(json.loads(s)).wkt
        return shapely_wkt.loads(s).wkt

    if isinstance(gf, dict):
        if "coordinates" not in gf:
            raise ValueError(f"Unrecognized dict structure: keys={list(gf.keys())}")
        geom = _coords_to_polygon(gf["coordinates"])
        if gf.get("type") == "MultiPolygon" and isinstance(geom, Polygon):
            geom = MultiPolygon([geom])
        return geom.wkt

    if isinstance(gf, (np.ndarray, list, tuple)):
        return _coords_to_polygon(gf).wkt

    raise TypeError(f"Cannot convert GeoFootprint of type {type(gf).__name__}")


def fetch_footprint_from_cdse(
    safedir: str, session: requests.Session, timeout: int = 30
):
    """Look up a product by Name in CDSE OData and return its GeoFootprint (dict) or None."""
    params = {
        "$filter": f"Name eq '{safedir}'",
        "$select": "Name,GeoFootprint",
        "$top": 1,
    }
    r = session.get(CDSE_ODATA, params=params, timeout=timeout)
    r.raise_for_status()
    values = r.json().get("value", [])
    return values[0].get("GeoFootprint") if values else None


def enrich_downloads_with_footprints(
    downloads_parquet: Path,
    scenes_parquet: Path,
    output_parquet: Path,
    sleep_s: float = 0.1,
):

    downloads = pd.read_parquet(downloads_parquet)
    scenes = pd.read_parquet(scenes_parquet)

    console.print(
        f"[bold]downloads[/bold]: {len(downloads):,} rows, "
        f"{downloads['safedir'].nunique():,} unique safedirs"
    )
    console.print(
        f"[bold]scenes[/bold]:    {len(scenes):,} rows, "
        f"{scenes['Name'].nunique():,} unique Names"
    )
    valid_in_scenes = scenes["GeoFootprint"].apply(lambda g: not _is_missing(g)).sum()
    console.print(
        f"[bold]scenes[/bold]: {valid_in_scenes:,} / {len(scenes):,} "
        f"have a non-empty GeoFootprint"
    )

    # How many downloaded safedirs are actually present in scenes?
    dl_safedirs = set(downloads["safedir"].unique())
    scene_names = set(scenes["Name"].unique())
    matched = dl_safedirs & scene_names
    console.print(
        f"[bold]overlap[/bold]: {len(matched):,} / {len(dl_safedirs):,} "
        f"download safedirs found in scenes.parquet"
    )

    # 1) Join footprints we already have
    fp_map = dict(zip(scenes["Name"], scenes["GeoFootprint"]))
    downloads["geofootprint"] = downloads["safedir"].map(fp_map)

    # 2) Find safedirs still missing a footprint (unique, to avoid redundant queries)
    missing = list(
        downloads.loc[downloads["geofootprint"].apply(_is_missing), "safedir"].unique()
    )
    console.print(
        f"[bold]{len(missing)}[/bold] unique safedirs missing a footprint; querying CDSE..."
    )

    # 3) Query CDSE one by one with a rich progress bar
    fetched: dict[str, object] = {}
    ok_count = not_found_count = error_count = 0

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("-"),
        TimeElapsedColumn(),
        TextColumn("-"),
        TimeRemainingColumn(),
        TextColumn("[green]OK {task.fields[ok]}[/green]"),
        TextColumn("[yellow]NOT FOUND {task.fields[not_found]}[/yellow]"),
        TextColumn("[red]ERROR {task.fields[err]}[/red]"),
        console=console,
    )

    with progress, requests.Session() as session:
        task = progress.add_task(
            "Fetching footprints",
            total=len(missing),
            ok=0,
            not_found=0,
            err=0,
        )
        for safedir in missing:
            try:
                gf = fetch_footprint_from_cdse(safedir, session)
                if gf is not None:
                    ok_count += 1
                else:
                    not_found_count += 1
            except Exception as e:
                gf = None
                error_count += 1
                console.print(f"  [red]ERROR[/red] {safedir}: {e}")
            fetched[safedir] = gf
            progress.update(
                task,
                advance=1,
                ok=ok_count,
                not_found=not_found_count,
                err=error_count,
            )
            time.sleep(sleep_s)

    # 4) Fill them in
    fill_mask = downloads["geofootprint"].apply(_is_missing)
    downloads.loc[fill_mask, "geofootprint"] = downloads.loc[fill_mask, "safedir"].map(
        fetched
    )

    # 5) Convert everything to WKT strings
    console.print("Converting footprints to WKT...")
    conversion_errors = 0
    first_error_logged = False

    def _safe_to_wkt(g):
        nonlocal conversion_errors, first_error_logged
        try:
            return to_wkt(g)
        except Exception as e:
            conversion_errors += 1
            if not first_error_logged:
                first_error_logged = True
                console.print(f"  [red]First WKT error[/red]: {e}")
                console.print(f"  type: {type(g).__name__}")
                if isinstance(g, dict):
                    console.print(f"  keys: {list(g.keys())}")
                    coords = g.get("coordinates")
                    console.print(f"  coords type: {type(coords).__name__}")
                    if hasattr(coords, "shape"):
                        console.print(
                            f"  coords shape: {coords.shape}, dtype: {coords.dtype}"  # type: ignore[attr-defined]
                        )
                    try:
                        c0 = coords[0]  # type: ignore[index]
                        console.print(f"  coords[0] type: {type(c0).__name__}")
                        c00 = c0[0]
                        console.print(
                            f"  coords[0][0]: {c00!r} (type={type(c00).__name__})"
                        )
                    except Exception as inner:
                        console.print(f"  could not introspect: {inner}")
            return None

    downloads["geofootprint"] = downloads["geofootprint"].apply(_safe_to_wkt)

    still_missing = downloads["geofootprint"].isna().sum()
    console.print(
        f"Rows without a WKT footprint: [bold]{still_missing}[/bold] / {len(downloads)}  "
        f"(conversion errors: {conversion_errors})"
    )

    # 6) Save -- allow overwrite
    output_parquet = Path(output_parquet)
    if output_parquet.exists():
        console.print(f"[yellow]Overwriting existing[/yellow] {output_parquet}")
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    downloads.to_parquet(output_parquet, index=False)
    console.print(f"[green]Saved -->[/green] {output_parquet}")
    return downloads


if __name__ == "__main__":
    basepth = Path("/mnt/poseidon/remotesensing/6ru/sentinel-py/examples/cache")
    enrich_downloads_with_footprints(
        downloads_parquet=basepth / "all_downloaded_images.parquet",
        scenes_parquet=basepth / "816e26c158c72b0090f39f5a14f5619a" / "scenes.parquet",
        output_parquet=basepth / "all_downloaded_images_w_footprints.parquet",
    )
