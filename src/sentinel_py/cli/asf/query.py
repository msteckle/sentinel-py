import calendar
import datetime as dt
import json
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import geopandas as gpd
import pandas as pd
import typer

from sentinel_py.cache import (
    DEFAULT_ASF_CACHE_DIR,
    cache_directory,
    deterministic_cache_key,
    mark_cache_used,
    write_json_atomic,
    write_parquet_atomic,
)
from sentinel_py.download.asf import get_predominant_flightdir, query_asf

app = typer.Typer()


class ASFOrbitDirection(str, Enum):
    predominant = "predominant"
    ascending = "ASCENDING"
    descending = "DESCENDING"


def _seasonal_date(year: int, period: dt.datetime) -> dt.date:
    """Build a seasonal date, clamping invalid month-end days like CDSE does."""
    day = min(period.day, calendar.monthrange(year, period.month)[1])
    return dt.date(year, period.month, day)


@app.command(help="Query ASF for Sentinel-1 products and cache a download manifest.")
def query(
    aoi: Annotated[
        Path,
        typer.Option(
            help="The AOI file used to filter the query (GeoJSON, shapefile, etc.).",
            exists=True,
            dir_okay=False,
        ),
    ],
    years: Annotated[
        str,
        typer.Option(help="Space- or comma-separated list of years."),
    ],
    speriod: Annotated[
        dt.datetime,
        typer.Option(
            help="Start month and day of seasonal query window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
        ),
    ] = dt.datetime(2000, 1, 1),
    eperiod: Annotated[
        dt.datetime,
        typer.Option(
            help="End month and day of seasonal query window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
        ),
    ] = dt.datetime(2000, 12, 31),
    product_levels: Annotated[
        str,
        typer.Option(
            help="Space- or comma-separated ASF product levels, such as SLC or GRD_HD."
        ),
    ] = "GRD_HD GRD_FD GRD_HS GRD_MD GRD_MS",
    beam_mode: Annotated[
        str,
        typer.Option(help="ASF beam mode. Sentinel-1 commonly uses IW."),
    ] = "IW",
    flight_direction: Annotated[
        ASFOrbitDirection,
        typer.Option(
            help=(
                "Orbit direction. One of predominant, ASCENDING, or DESCENDING. By "
                "default, query both directions and keep only the direction with the "
                "most returned products (predominant)."
            )
        ),
    ] = ASFOrbitDirection.predominant,
    polarization: Annotated[
        str,
        typer.Option(
            help="Restrict product polarization. Defaults to dual-polarized VV+VH."
        ),
    ] = "VV+VH",
    relative_orbit: Annotated[
        Optional[int],
        typer.Option(help="Optionally restrict Sentinel-1 relative orbit.", min=1),
    ] = None,
    max_results: Annotated[
        Optional[int],
        typer.Option(help="Optional maximum result count per yearly window.", min=1),
    ] = None,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "Query cache root. Defaults to the hidden .asf-cache directory in "
                "the current working directory."
            ),
            file_okay=False,
        ),
    ] = DEFAULT_ASF_CACHE_DIR,
    crs: Annotated[
        str,
        typer.Option(help="CRS to assume when the input AOI has no CRS metadata."),
    ] = "EPSG:4326",
):
    # Validate inputs
    try:
        parsed_years = sorted({int(year) for year in years.replace(",", " ").split()})
    except ValueError as error:
        raise typer.BadParameter(f"Could not parse years: {error}") from error
    if not parsed_years:
        raise typer.BadParameter("--years must contain at least one year")
    if any(year < 1 or year > 9999 for year in parsed_years):
        raise typer.BadParameter("--years values must be between 1 and 9999")
    if (eperiod.month, eperiod.day) < (speriod.month, speriod.day):
        raise typer.BadParameter(
            "--eperiod must be on or after --speriod within each year"
        )

    query_windows = [
        (
            _seasonal_date(year, speriod).isoformat(),
            _seasonal_date(year, eperiod).isoformat(),
        )
        for year in parsed_years
    ]

    aoi_gdf = gpd.read_file(aoi)
    if aoi_gdf.empty:
        raise typer.BadParameter(f"AOI contains no features: {aoi}")
    if aoi_gdf.crs is None:
        aoi_gdf = aoi_gdf.set_crs(crs)
    aoi_wkt = aoi_gdf.to_crs("EPSG:4326").geometry.union_all().wkt

    direction_value = (
        flight_direction.value
        if isinstance(flight_direction, ASFOrbitDirection)
        else str(flight_direction)
    )
    query_direction = (
        None
        if direction_value == ASFOrbitDirection.predominant.value
        else direction_value
    )
    levels = [
        level.upper()
        for level in product_levels.replace(",", " ").split()
        if level.strip()
    ]
    if not levels:
        raise typer.BadParameter("--product-levels must contain at least one value")

    # Build query payload and cache directory
    query_payload = {
        "provider": "ASF",
        "platform": "SENTINEL-1",
        "aoi_wkt": aoi_wkt,
        "years": parsed_years,
        "speriod": f"{speriod.month:02d}-{speriod.day:02d}",
        "eperiod": f"{eperiod.month:02d}-{eperiod.day:02d}",
        "windows": query_windows,
        "product_levels": sorted(set(levels)),
        "beam_mode": beam_mode.upper(),
        "flight_direction": direction_value,
        "polarization": polarization,
        "relative_orbit": relative_orbit,
        "max_results": max_results,
    }
    query_dir = cache_directory(
        cache_dir,
        deterministic_cache_key(query_payload),
    )
    cached_manifest = query_dir / "manifest.parquet"
    query_info_path = query_dir / "query_info.json"
    direction_counts: dict[str, int] = {}
    selected_direction: str | None = None

    # Load cached manifest if it exists
    if cached_manifest.exists():
        manifest = pd.read_parquet(cached_manifest)
        mark_cache_used(cached_manifest)
        if query_info_path.exists():
            info = json.loads(query_info_path.read_text())
            direction_counts = {
                str(direction): int(count)
                for direction, count in info.get("direction_counts", {}).items()
            }
            selected_direction = info.get("selected_direction")
        typer.echo(f"Loaded cached ASF query: {cached_manifest}")
    # Otherwise, run the query and cache the results
    else:
        try:
            yearly_manifests = [
                query_asf(
                    aoi_wkt=aoi_wkt,
                    date_start=start_date,
                    date_end=end_date,
                    product_levels=levels,
                    beam_mode=beam_mode,
                    flight_direction=query_direction,
                    polarization=polarization,
                    relative_orbit=relative_orbit,
                    max_results=max_results,
                )
                for start_date, end_date in query_windows
            ]
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        manifest = pd.concat(yearly_manifests, ignore_index=True)
        if not manifest.empty:
            manifest = (
                manifest.drop_duplicates(subset=["url"])
                .sort_values("granule")
                .reset_index(drop=True)
            )

        # When no explicit direction is requested, retain only the direction that
        # occurs most often. Ties deterministically select ASCENDING.
        if direction_value == ASFOrbitDirection.predominant.value:
            counts = (
                manifest["flightDirection"]
                .dropna()
                .astype(str)
                .str.upper()
                .value_counts()
            )
            direction_counts = {
                str(direction): int(count) for direction, count in counts.items()
            }
            selected_direction = get_predominant_flightdir(manifest)
            if selected_direction is not None:
                manifest = manifest[
                    manifest["flightDirection"].astype(str).str.upper()
                    == selected_direction
                ].reset_index(drop=True)

        # Cache the manifest and query info
        write_parquet_atomic(manifest, cached_manifest, index=False)
        write_json_atomic(
            query_info_path,
            {
                **query_payload,
                "aoi": str(aoi),
                "crs": crs,
                "created": dt.datetime.now().isoformat(),
                "num_products": len(manifest),
                "direction_counts": direction_counts,
                "selected_direction": selected_direction,
            },
        )
        typer.echo(f"Cached ASF query: {cached_manifest}")

    # Report flight direction counts if applicable
    if selected_direction is not None:
        counts = ", ".join(
            f"{direction}={count}"
            for direction, count in sorted(direction_counts.items())
        )
        typer.echo(f"Flight direction counts: {counts}; selected {selected_direction}.")

    # Report results
    typer.echo(f"Found {len(manifest)} unique ASF product(s).")
