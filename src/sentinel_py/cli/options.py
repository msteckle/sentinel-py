"""Typer option aliases and validation helpers shared by the provider commands.

Every provider (ASF, CDSE, PGC) exposes the same ``query`` and ``download`` option
vocabulary: an AOI and CRS, a season, a cache root, logging flags, and a manifest to
download. Declaring them once keeps flags, defaults, and help panels consistent and
keeps the command modules focused on provider-specific behavior.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Annotated, Any

import geopandas as gpd
import pandas as pd
import typer
from shapely.geometry.base import BaseGeometry

from sentinel_py.aoi import atomic_aoi_geometries
from sentinel_py.cache import find_latest_cache_file
from sentinel_py.log import DEFAULT_LOG_DIR

########################################################################################
# Help panels and shared constants
########################################################################################

PANEL_REQUIRED = "Required Arguments"
PANEL_QUERY = "Optional Query Configurations"
PANEL_DOWNLOAD = "Optional Download Configurations"
PANEL_UTILS = "Utils"

SEASON_FORMATS = ["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"]


########################################################################################
# Option aliases
########################################################################################

YearsOpt = Annotated[
    str,
    typer.Option(
        help="Space- or comma-separated list of years.",
        rich_help_panel=PANEL_REQUIRED,
    ),
]
SPeriodOpt = Annotated[
    dt.datetime,
    typer.Option(
        help="Start month and day of seasonal query window.",
        formats=SEASON_FORMATS,
        rich_help_panel=PANEL_QUERY,
    ),
]
EPeriodOpt = Annotated[
    dt.datetime,
    typer.Option(
        help="End month and day of seasonal query window.",
        formats=SEASON_FORMATS,
        rich_help_panel=PANEL_QUERY,
    ),
]
LogOpt = Annotated[
    Path | None,
    typer.Option(
        help=(
            "Log file path for execution logs. If omitted, logs are saved to "
            f"{DEFAULT_LOG_DIR}."
        ),
        rich_help_panel=PANEL_UTILS,
    ),
]
VerboseOpt = Annotated[
    bool,
    typer.Option(
        help="Enable verbose logging to the console and log file.",
        rich_help_panel=PANEL_UTILS,
    ),
]
YesOpt = Annotated[
    bool,
    typer.Option(
        "--yes",
        "-y",
        help="Skip the interactive download confirmation.",
        rich_help_panel=PANEL_DOWNLOAD,
    ),
]


def cache_dir_option(provider: str, default: Path) -> Any:
    """Build the ``--cache-dir`` option annotation for one provider.

    Parameters
    ----------
    provider : str
        Provider label used in the help text, e.g. ``"ASF"``.
    default : pathlib.Path
        Provider default cache root, shown in the help text.

    Returns
    -------
    Any
        An ``Annotated[Path, typer.Option(...)]`` suitable as a parameter type.
    """
    return Annotated[
        Path,
        typer.Option(
            help=f"{provider} query cache root. Defaults to {default}.",
            file_okay=False,
            rich_help_panel=PANEL_UTILS,
        ),
    ]


########################################################################################
# Input validation helpers
########################################################################################


def parse_years(years: str) -> list[int]:
    """Parse a space- or comma-separated ``--years`` value.

    Parameters
    ----------
    years : str
        Years separated by spaces and/or commas.

    Returns
    -------
    list[int]
        Sorted, de-duplicated years.

    Raises
    ------
    typer.BadParameter
        If the value is not integers, is empty, or has years outside 1-9999.
    """
    try:
        parsed = sorted({int(year) for year in years.replace(",", " ").split()})
    except ValueError as error:
        raise typer.BadParameter(f"Could not parse years: {error}") from error
    if not parsed:
        raise typer.BadParameter("--years must contain at least one year")
    if any(year < 1 or year > 9999 for year in parsed):
        raise typer.BadParameter("--years values must be between 1 and 9999")
    return parsed


def validate_season(speriod: dt.datetime, eperiod: dt.datetime) -> None:
    """Require the season end to fall on or after its start within a year.

    Raises
    ------
    typer.BadParameter
        If ``eperiod`` precedes ``speriod``.
    """
    if (eperiod.month, eperiod.day) < (speriod.month, speriod.day):
        raise typer.BadParameter(
            "--eperiod must be on or after --speriod within each year"
        )


def load_aoi_geometries(aoi: Path, crs: str) -> list[BaseGeometry]:
    """Read an AOI file into atomic WGS84 geometries for provider searches.

    Parameters
    ----------
    aoi : pathlib.Path
        AOI vector file.
    crs : str
        CRS to assume when the file has no CRS metadata.

    Returns
    -------
    list[BaseGeometry]
        Non-empty, de-duplicated atomic geometries in WGS84.

    Raises
    ------
    typer.BadParameter
        If the file has no features or no usable geometries.
    """
    frame = gpd.read_file(aoi)
    if frame.empty:
        raise typer.BadParameter(f"AOI contains no features: {aoi}")
    if frame.crs is None:
        frame = frame.set_crs(crs)
    geometries = atomic_aoi_geometries(frame.to_crs("EPSG:4326").geometry)
    if not geometries:
        raise typer.BadParameter(f"AOI contains no usable geometries: {aoi}")
    return geometries


########################################################################################
# Download helpers
########################################################################################


def load_manifest(
    query: Path | None,
    cache_dir: Path,
    *,
    provider: str,
    query_command: str,
) -> tuple[Path, pd.DataFrame]:
    """Resolve and read a download manifest, de-duplicated by ``url``.

    Parameters
    ----------
    query : pathlib.Path, optional
        Explicit manifest path; the latest cached query manifest is used when omitted.
    cache_dir : pathlib.Path
        Provider query cache root searched for the latest manifest.
    provider : str
        Provider label used in messages, e.g. ``"ASF"``.
    query_command : str
        Command the user should run first, e.g. ``"sentinel-py asf query"``.

    Returns
    -------
    tuple[pathlib.Path, pandas.DataFrame]
        Manifest path and its rows with unique, non-null URLs.

    Raises
    ------
    typer.BadParameter
        If no manifest can be found or it lacks a ``url`` column.
    """
    manifest_path = query or find_latest_cache_file(cache_dir, "manifest.parquet")
    if manifest_path is None:
        raise typer.BadParameter(
            f"No cached {provider} query manifest found in {cache_dir}. "
            f"Run '{query_command}' before downloading."
        )
    manifest = pd.read_parquet(manifest_path)
    if "url" not in manifest.columns:
        raise typer.BadParameter(
            f"{provider} query manifest is missing the required 'url' column: "
            f"{manifest_path}"
        )
    manifest = (
        manifest.dropna(subset=["url"]).drop_duplicates("url").reset_index(drop=True)
    )
    return Path(manifest_path), manifest


def echo_summary(messages: Any) -> None:
    """Print a ``Summary:`` block with one indented line per message."""
    typer.echo("Summary:")
    for message in messages:
        typer.echo(f"  {message}")
