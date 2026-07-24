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
from sentinel_py.download.asf import (
    get_predominant_flightdir,
    query_asf,
)
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()


class ASFOrbitDirection(str, Enum):
    both = "BOTH"
    predominant = "PREDOMINANT"
    ascending = "ASCENDING"
    descending = "DESCENDING"


class ASFProductLevel(str, Enum):
    grd_hd = "GRD_HD"
    grd_hs = "GRD_HS"
    grd_md = "GRD_MD"
    grd_ms = "GRD_MS"
    grd_fd = "GRD_FD"
    slc = "SLC"
    raw = "RAW"
    ocn = "OCN"


class ASFBeamMode(str, Enum):
    iw = "IW"
    ew = "EW"
    wv = "WV"
    s1 = "S1"
    s2 = "S2"
    s3 = "S3"
    s4 = "S4"
    s5 = "S5"
    s6 = "S6"


class ASFPolarization(str, Enum):
    vv_vh = "VV+VH"
    hh_hv = "HH+HV"
    vv = "VV"
    hh = "HH"
    dual_vv = "DUAL VV"
    dual_vh = "DUAL VH"
    dual_hh = "DUAL HH"
    dual_hv = "DUAL HV"


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
            rich_help_panel="Required Arguments",
        ),
    ],
    years: Annotated[
        str,
        typer.Option(
            help="Space- or comma-separated list of years.",
            rich_help_panel="Required Arguments",
        ),
    ],
    speriod: Annotated[
        dt.datetime,
        typer.Option(
            help="Start month and day of seasonal query window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
            rich_help_panel="Optional Query Configurations",
        ),
    ] = dt.datetime(2000, 1, 1),
    eperiod: Annotated[
        dt.datetime,
        typer.Option(
            help="End month and day of seasonal query window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
            rich_help_panel="Optional Query Configurations",
        ),
    ] = dt.datetime(2000, 12, 31),
    product_levels: Annotated[
        ASFProductLevel,
        typer.Option(
            help=(
                "Sentinel-1 ASF product level: GRD_HD, GRD_HS, GRD_MD, GRD_MS, "
                "GRD_FD, SLC, RAW, or OCN. Availability depends on acquisition mode "
                "and archive history. Source: ASF Search API keyword reference."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = ASFProductLevel.grd_hd,
    beam_mode: Annotated[
        ASFBeamMode,
        typer.Option(
            help=(
                "Sentinel-1 ASF beam mode: IW (primary land mode), EW "
                "(extra-wide swath), WV (wave), or S1-S6 (individual Stripmap beams). "
                "Source: ESA Sentinel-1 Mission."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = ASFBeamMode.iw,
    flight_direction: Annotated[
        ASFOrbitDirection,
        typer.Option(
            help=(
                "Orbit direction. One of: BOTH, PREDOMINANT, ASCENDING, or DESCENDING. "
                "The default retains both directions."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = ASFOrbitDirection.both,
    polarization: Annotated[
        ASFPolarization,
        typer.Option(
            help=(
                "Sentinel-1 polarization. VV+VH is commonly used over land, HH+HV is "
                "commonly used for polar/sea-ice observations. Availability depends on "
                "the acquisition. Sources: ASF Search API keyword reference and ESA "
                "Sentinel-1 Mission."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = ASFPolarization.vv_vh,
    relative_orbit: Annotated[
        Optional[int],
        typer.Option(
            help="Optionally restrict Sentinel-1 relative orbit.",
            min=1,
            max=175,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    max_results: Annotated[
        Optional[int],
        typer.Option(
            help="Optionally set maximum result count per yearly window.",
            min=1,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "Query cache root. Defaults to the hidden .asf-cache directory in "
                "the current working directory."
            ),
            file_okay=False,
            rich_help_panel="Utils",
        ),
    ] = DEFAULT_ASF_CACHE_DIR,
    crs: Annotated[
        str,
        typer.Option(
            help="CRS to assume when the input AOI has no CRS metadata.",
            rich_help_panel="Utils",
        ),
    ] = "EPSG:4326",
    log: Annotated[
        Optional[Path],
        typer.Option(
            help=(
                "Log file path for query execution logs. If omitted, logs are saved to "
                f"{DEFAULT_LOG_DIR}."
            ),
            rich_help_panel="Utils",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            help="Enable verbose logging to the console and log file.",
            rich_help_panel="Utils",
        ),
    ] = False,
):
    logger = get_logger(name="asf_query_logger", logpath=log, verbose=verbose)

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

    product_level_value = product_levels.value
    beam_mode_value = beam_mode.value
    direction_value = flight_direction.value
    polarization_value = polarization.value
    query_direction = (
        None
        if direction_value
        in {
            ASFOrbitDirection.both.value,
            ASFOrbitDirection.predominant.value,
        }
        else direction_value
    )
    levels = [product_level_value]

    # Build query payload and cache directory
    query_payload = {
        "provider": "ASF",
        "platform": "SENTINEL-1",
        "aoi_wkt": aoi_wkt,
        "years": parsed_years,
        "speriod": f"{speriod.month:02d}-{speriod.day:02d}",
        "eperiod": f"{eperiod.month:02d}-{eperiod.day:02d}",
        "windows": query_windows,
        "product_levels": levels,
        "beam_mode": beam_mode_value,
        "flight_direction": direction_value,
        "polarization": polarization_value,
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
    possibly_truncated_windows: list[dict[str, object]] = []
    logger.info(
        "ASF query configured: aoi=%s years=%s windows=%s cache=%s",
        aoi,
        parsed_years,
        query_windows,
        cached_manifest,
    )

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
            possibly_truncated_windows = info.get("possibly_truncated_windows", [])
        logger.info("Loaded cached ASF query from %s", cached_manifest)
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
                    beam_mode=beam_mode_value,
                    flight_direction=query_direction,
                    polarization=polarization_value,
                    relative_orbit=relative_orbit,
                    max_results=max_results,
                    logger=logger,
                )
                for start_date, end_date in query_windows
            ]
        except ValueError as error:
            logger.error("Invalid ASF query: %s", error)
            raise typer.BadParameter(str(error)) from error

        if max_results is not None:
            possibly_truncated_windows = [
                {
                    "start": start_date,
                    "end": end_date,
                    "returned": len(yearly_manifest),
                    "max_results": max_results,
                }
                for (start_date, end_date), yearly_manifest in zip(
                    query_windows, yearly_manifests
                )
                if len(yearly_manifest) >= max_results
            ]

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
                "possibly_truncated_windows": possibly_truncated_windows,
            },
        )
        logger.info("Cached %d ASF products to %s", len(manifest), cached_manifest)
        typer.echo(f"Cached ASF query: {cached_manifest}")

    # Report flight direction counts if applicable
    if selected_direction is not None:
        counts = ", ".join(
            f"{direction}={count}"
            for direction, count in sorted(direction_counts.items())
        )
        typer.echo(f"Flight direction counts: {counts}; selected {selected_direction}.")

    if possibly_truncated_windows:
        windows = ", ".join(
            f"{window['start']} to {window['end']}"
            for window in possibly_truncated_windows
        )
        logger.warning(
            "ASF returned the --max-results limit for %s; the cached manifest may be "
            "truncated.",
            windows,
        )
        typer.echo(
            "WARNING: ASF returned the --max-results limit for "
            f"{windows}; the cached manifest may be truncated.",
            err=True,
        )

    # Report results
    logger.info("ASF query complete: %d unique products", len(manifest))
    typer.echo(f"Found {len(manifest)} unique ASF product(s).")
