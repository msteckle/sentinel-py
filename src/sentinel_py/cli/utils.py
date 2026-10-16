"""CLI utilities for building AOI files: bounding boxes, CSV points, and grids."""

from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.enums import GridClipOpts

app = typer.Typer()


# sentinel-py bbox2geojson -------------------------------------------------------------
@app.command(
    "bbox2geojson",
    help=(
        "Create a bounding box GeoJSON given xmin, ymin, xmax, ymax. "
        "The output bbox will always be in EPSG:4326 (lat/lon)."
    ),
)
def bbox2geojson(
    bounds: Annotated[
        tuple[float, float, float, float],
        typer.Option(help="Bounding box bounds as xmin ymin xmax ymax."),
    ],
    output: Annotated[
        Path,
        typer.Option(
            help="Output file path for the bbox GeoJSON.",
            dir_okay=False,
        ),
    ] = Path("bbox2geojson.geojson"),
) -> None:
    """Write a EPSG:4326 bounding-box GeoJSON from ``xmin ymin xmax ymax``."""
    from sentinel_py.aoi import bbox_to_geojson

    xmin, ymin, xmax, ymax = bounds
    bbox_to_geojson(
        bbox=(xmin, ymin, xmax, ymax),
        crs="EPSG:4326",
        output=output,
    )


# sentinel-py aoi2bbox -----------------------------------------------------------------
@app.command(
    "aoi2bbox",
    help=(
        "Create a bounding box GeoJSON enclosing all features of an AOI file. "
        "The output bbox will always be in EPSG:4326 (lat/lon)."
    ),
)
def aoi2bbox(
    aoi: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="Path to area of interest legible by pyogrio.",
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            help="Output file path for the bbox GeoJSON.",
            dir_okay=False,
        ),
    ] = Path("aoi2bbox.geojson"),
) -> None:
    """Write a EPSG:4326 bounding-box GeoJSON enclosing every feature of an AOI."""
    from sentinel_py.aoi import aoi_to_bbox

    try:
        aoi_to_bbox(aoi, output=output)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--aoi") from exc


# sentinel-py csv2geojson --------------------------------------------------------------
@app.command(
    "csv2geojson",
    help=(
        "Create a GeoJSON from a CSV with latitude and longitude columns. "
        "The output GeoJSON will be in EPSG:4326 (lat/lon)."
    ),
)
def csv2geojson(
    csv: Annotated[
        Path,
        typer.Option(
            help="Path to input CSV file.",
            exists=True,
            dir_okay=False,
        ),
    ],
    lon: Annotated[str, typer.Option(help="Name of the longitude column in the CSV.")],
    lat: Annotated[str, typer.Option(help="Name of the latitude column in the CSV.")],
    crs: Annotated[
        str, typer.Option(help="CRS of the lat/lon coordinates in the CSV.")
    ] = "EPSG:4326",
    output: Annotated[
        Path,
        typer.Option(
            help="Output file path for the GeoJSON.",
            dir_okay=False,
        ),
    ] = Path("csv2geojson.geojson"),
) -> None:
    """Write a EPSG:4326 point GeoJSON from CSV longitude and latitude columns."""
    from sentinel_py.aoi import csv_to_geojson

    # call core function
    csv_to_geojson(
        csv=csv,
        lon=lon,
        lat=lat,
        crs=crs,
        output=output,
    )


# sentinel-py grid ---------------------------------------------------------------------
@app.command(
    "grid",
    help=(
        "Create a EPSG:4326 (lat/lon) grid overlaying an AOI file for a specified "
        "cell size in degrees. The grid can be used for future parallel processing."
    ),
)
def grid(
    aoi: Annotated[
        Path,
        typer.Option(exists=True, help="Path to area of interest legible by pyogrio."),
    ],
    px: Annotated[
        tuple[float, float],
        typer.Option(
            help="Grid cell size in decimal degrees as float or tuple of (dx, dy).",
        ),
    ],
    crs: Annotated[
        str,
        typer.Option(
            help=(
                "CRS of the input aoi file. Default is EPSG:4326 (lat/lon degrees). "
                "The output grid will always be in EPSG:4326."
            )
        ),
    ] = "EPSG:4326",
    fill_holes: Annotated[
        bool, typer.Option(help="Fill holes in aoi geometry.")
    ] = True,
    clip: Annotated[
        GridClipOpts,
        typer.Option(
            case_sensitive=False,
            help=(
                "How grid cells are subselected based on their spatial relationship to "
                "the aoi geometry. Options: 'intersect' (keep cells that intersect the "
                "aoi), 'within' (keep cells fully within the aoi), or 'all' (keep all "
                "cells within the bounding box of the aoi)."
            ),
        ),
    ] = GridClipOpts.intersect,
    output: Annotated[
        Path,
        typer.Option(
            help="Output .geojson file.",
        ),
    ] = Path("grid.geojson"),
) -> None:
    """Write a EPSG:4326 grid of ``--px`` degree cells overlaying an AOI."""
    from sentinel_py.aoi import overlay_latlon_grid

    if not all(0.0001 <= value <= 180.0 for value in px):
        raise typer.BadParameter("Cell size values must be between 0.0001 and 180.0")
    dx, dy = px

    # call core function
    overlay_latlon_grid(
        aoi=aoi,
        cell_size_deg=(dx, dy),
        crs=crs,
        fill_holes=fill_holes,
        clip=clip.value,
        output=output,
    )
