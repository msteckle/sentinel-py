"""CLI command for querying ASF Sentinel-1 products and caching a manifest."""

import calendar
import datetime as dt
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from shapely.geometry import MultiPoint
from shapely.geometry.polygon import orient

from sentinel_py.cache import DEFAULT_ASF_CACHE_DIR, QueryCache
from sentinel_py.cli.options import (
    PANEL_QUERY,
    PANEL_REQUIRED,
    EPeriodOpt,
    LogOpt,
    SPeriodOpt,
    VerboseOpt,
    YearsOpt,
    cache_dir_option,
    load_aoi_geometries,
    parse_years,
    validate_season,
)
from sentinel_py.download.asf import (
    get_predominant_flightdir,
    query_asf,
)
from sentinel_py.enums import (
    ASFBeamMode,
    ASFOrbitDirection,
    ASFPolarization,
    ASFProductLevel,
)
from sentinel_py.log import get_logger

app = typer.Typer()


def _seasonal_date(year: int, period: dt.datetime) -> dt.date:
    """Build a seasonal date, clamping invalid month-end days like CDSE does."""
    day = min(period.day, calendar.monthrange(year, period.month)[1])
    return dt.date(year, period.month, day)


def _season_day(period: dt.datetime) -> int:
    """Return ASF's 1-based, non-leap day-of-year for a month/day period."""
    if (period.month, period.day) == (2, 29):
        return 60
    return dt.date(2001, period.month, period.day).timetuple().tm_yday


def _season_windows(
    years: list[int], speriod: dt.datetime, eperiod: dt.datetime
) -> tuple[list[tuple[str, str]], tuple[str, str], list[int]]:
    """Build the per-year ISO windows, overall range, and ASF season-day pair.

    Returns
    -------
    tuple
        ``(windows, (first_start, last_end), [start_day, end_day])``.
    """
    windows = [
        (
            _seasonal_date(year, speriod).isoformat(),
            _seasonal_date(year, eperiod).isoformat(),
        )
        for year in years
    ]
    return (
        windows,
        (windows[0][0], windows[-1][1]),
        [_season_day(speriod), _season_day(eperiod)],
    )


def _spatial_queries(components: list) -> tuple[list[str], str]:
    """Turn atomic AOI geometries into independent ASF spatial query WKTs.

    Returns
    -------
    tuple[list[str], str]
        Query WKTs and the strategy name recorded in the cache key.

    Raises
    ------
    typer.BadParameter
        If the AOI mixes points and polygons or has other geometry types.
    """
    if all(geometry.geom_type == "Point" for geometry in components):
        # ASF's point OR query is compact and avoids creating an enormous
        # polygon envelope around widely separated sample points.
        return [MultiPoint(components).wkt], "cmr_point_or_v1"
    if any(geometry.geom_type != "Polygon" for geometry in components):
        raise typer.BadParameter("AOI must contain only points or only polygons")
    # Never union disconnected boxes: each is an independent ASF filter.
    return [orient(geometry, sign=1.0).wkt for geometry in components], (
        "independent_polygon_v1"
    )


def _keep_predominant_direction(
    manifest: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int], str | None]:
    """Keep only the most frequent flight direction (ties select ASCENDING).

    Returns
    -------
    tuple
        ``(filtered_manifest, direction_counts, selected_direction)``.
    """
    counts = manifest["flightDirection"].dropna().astype(str).str.upper().value_counts()
    direction_counts = {str(direction): int(count) for direction, count in counts.items()}
    selected = get_predominant_flightdir(manifest)
    if selected is not None:
        manifest = manifest[
            manifest["flightDirection"].astype(str).str.upper() == selected
        ].reset_index(drop=True)
    return manifest, direction_counts, selected


def _report_query(
    logger,
    manifest: pd.DataFrame,
    selected_direction: str | None,
    direction_counts: dict[str, int],
    truncated_windows: list[dict[str, object]],
) -> None:
    """Echo flight-direction counts, truncation warnings, and the result count."""
    if selected_direction is not None:
        counts = ", ".join(
            f"{direction}={count}"
            for direction, count in sorted(direction_counts.items())
        )
        typer.echo(f"Flight direction counts: {counts}; selected {selected_direction}.")

    if truncated_windows:
        windows = ", ".join(
            f"{window['start']} to {window['end']}" for window in truncated_windows
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

    logger.info("ASF query complete: %d unique products", len(manifest))
    typer.echo(f"Found {len(manifest)} unique ASF product(s).")


@app.command(help="Query ASF for Sentinel-1 products and cache a download manifest.")
def query(
    aoi: Annotated[
        Path,
        typer.Option(
            help="The AOI file used to filter the query (GeoJSON, shapefile, etc.).",
            exists=True,
            dir_okay=False,
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    years: YearsOpt,
    speriod: SPeriodOpt = dt.datetime(2000, 1, 1),
    eperiod: EPeriodOpt = dt.datetime(2000, 12, 31),
    product_levels: Annotated[
        ASFProductLevel,
        typer.Option(
            help=(
                "Sentinel-1 ASF product level: GRD_HD, GRD_HS, GRD_MD, GRD_MS, "
                "GRD_FD, SLC, RAW, or OCN. Availability depends on acquisition mode "
                "and archive history. Source: ASF Search API keyword reference."
            ),
            case_sensitive=False,
            rich_help_panel=PANEL_QUERY,
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
            rich_help_panel=PANEL_QUERY,
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
            rich_help_panel=PANEL_QUERY,
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
            rich_help_panel=PANEL_QUERY,
        ),
    ] = ASFPolarization.vv_vh,
    relative_orbit: Annotated[
        int | None,
        typer.Option(
            help="Optionally restrict Sentinel-1 relative orbit.",
            min=1,
            max=175,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    max_results: Annotated[
        int | None,
        typer.Option(
            help="Optionally set the maximum result count for the complete query.",
            min=1,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    cache_dir: cache_dir_option("ASF", DEFAULT_ASF_CACHE_DIR) = DEFAULT_ASF_CACHE_DIR,
    crs: Annotated[
        str,
        typer.Option(
            help="CRS to assume when the input AOI has no CRS metadata.",
            rich_help_panel="Utils",
        ),
    ] = "EPSG:4326",
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Query ASF for Sentinel-1 products and cache a download manifest."""
    logger = get_logger(name="asf_query_logger", logpath=log, verbose=verbose)

    parsed_years = parse_years(years)
    validate_season(speriod, eperiod)
    query_windows, query_range, season = _season_windows(parsed_years, speriod, eperiod)
    aoi_wkts, spatial_query_strategy = _spatial_queries(load_aoi_geometries(aoi, crs))

    direction_value = flight_direction.value
    query_direction = (
        None
        if direction_value
        in {ASFOrbitDirection.both.value, ASFOrbitDirection.predominant.value}
        else direction_value
    )
    levels = [product_levels.value]

    # Build query payload and cache directory
    query_payload = {
        "provider": "ASF",
        "platform": "SENTINEL-1",
        "aoi_wkts": aoi_wkts,
        "years": parsed_years,
        "speriod": f"{speriod.month:02d}-{speriod.day:02d}",
        "eperiod": f"{eperiod.month:02d}-{eperiod.day:02d}",
        "windows": query_windows,
        "query_range": query_range,
        "season": season,
        "temporal_query_strategy": "asf_season_v1",
        "product_levels": levels,
        "beam_mode": beam_mode.value,
        "flight_direction": direction_value,
        "polarization": polarization.value,
        "relative_orbit": relative_orbit,
        "max_results": max_results,
        "spatial_query_strategy": spatial_query_strategy,
    }
    query_cache = QueryCache.for_payload(cache_dir, query_payload, "manifest.parquet")
    cached_manifest = query_cache.table_path
    direction_counts: dict[str, int] = {}
    selected_direction: str | None = None
    truncated_windows: list[dict[str, object]] = []
    logger.info(
        "ASF query configured: aoi=%s years=%s range=%s season=%s windows=%s cache=%s",
        aoi,
        parsed_years,
        query_range,
        season,
        query_windows,
        cached_manifest,
    )

    if query_cache.exists():
        manifest = query_cache.load()
        info = query_cache.load_info()
        direction_counts = {
            str(direction): int(count)
            for direction, count in info.get("direction_counts", {}).items()
        }
        selected_direction = info.get("selected_direction")
        truncated_windows = info.get("possibly_truncated_windows", [])
        logger.info("Loaded cached ASF query from %s", cached_manifest)
        typer.echo(f"Loaded cached ASF query: {cached_manifest}")
    else:
        try:
            manifest = pd.concat(
                [
                    query_asf(
                        aoi_wkt=aoi_wkt,
                        date_start=query_range[0],
                        date_end=query_range[1],
                        product_levels=levels,
                        season=season,
                        beam_mode=beam_mode.value,
                        flight_direction=query_direction,
                        polarization=polarization.value,
                        relative_orbit=relative_orbit,
                        max_results=max_results,
                        logger=logger,
                    )
                    for aoi_wkt in aoi_wkts
                ],
                ignore_index=True,
            )
        except ValueError as error:
            logger.error("Invalid ASF query: %s", error)
            raise typer.BadParameter(str(error)) from error

        if max_results is not None and len(manifest) >= max_results:
            truncated_windows = [
                {
                    "start": query_range[0],
                    "end": query_range[1],
                    "returned": len(manifest),
                    "max_results": max_results,
                }
            ]
        if not manifest.empty:
            manifest = (
                manifest.drop_duplicates(subset=["url"])
                .sort_values("granule")
                .reset_index(drop=True)
            )
        if direction_value == ASFOrbitDirection.predominant.value:
            manifest, direction_counts, selected_direction = (
                _keep_predominant_direction(manifest)
            )

        query_cache.save(
            manifest,
            {
                **query_payload,
                "aoi": str(aoi),
                "crs": crs,
                "created": dt.datetime.now().isoformat(),
                "num_products": len(manifest),
                "direction_counts": direction_counts,
                "selected_direction": selected_direction,
                "possibly_truncated_windows": truncated_windows,
            },
        )
        logger.info("Cached %d ASF products to %s", len(manifest), cached_manifest)
        typer.echo(f"Cached ASF query: {cached_manifest}")

    _report_query(logger, manifest, selected_direction, direction_counts, truncated_windows)
