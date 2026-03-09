from pathlib import Path
import datetime as dt
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from enum import Enum

import typer
from typing import Annotated

from sentinel_py.common.logging import get_logger, DEFAULT_LOG_DIR


########################################################################################
# Application creation
########################################################################################

# set up main app and subcommands
app = typer.Typer(
    help="Sentinel data and workflow CLI.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)
s1 = typer.Typer(
    help="Sentinel-1 download, processing, and analysis tools.",
    pretty_exceptions_enable=False,
)
app.add_typer(s1, name="s1")
s2 = typer.Typer(
    help="Sentinel-2 download, processing, and analysis tools.",
    pretty_exceptions_enable=False,
)
app.add_typer(s2, name="s2")


########################################################################################
# General commands
########################################################################################

class GridClipOpts(str, Enum):
    intersect = "intersect"
    within = "within"
    all = "all"


@app.command(
    "bbox2geojson",
    help=(
        "Create a bounding box GeoJSON given xmin, ymin, xmax, ymax. "
        "The output bbox will always be in EPSG:4326 (lat/lon)."
    )
)
def bbox2geojson(
    bounds: Annotated[
        tuple[float, float, float, float],
        typer.Option(
            help="Bounding box bounds as xmin ymin xmax ymax."
        )
    ],
    output: Annotated[
        Path,
        typer.Option(
            help="Output file path for the bbox GeoJSON.",
            dir_okay=False,
        )
    ] = Path("bbox.geojson"),
):
    from sentinel_py.common.aoi import bbox_to_geojson

    # handle inputted bounds argument
    try:
        xmin, ymin, xmax, ymax = bounds
    except Exception as exc:
        raise typer.BadParameter(
            f"Expected 4 floats (xmin, ymin, xmax, ymax), got: {bounds=}"
        ) from exc

    # call core function
    bbox_to_geojson(
        bbox=(xmin, ymin, xmax, ymax),
        crs="EPSG:4326",
        output=output,
    )


@app.command(
    "csv2geojson",
    help=(
        "Create a GeoJSON from a CSV with latitude and longitude columns. "
        "The output GeoJSON will be in EPSG:4326 (lat/lon)."
    )
)
def csv2geojson(
    csv: Annotated[
        Path,
        typer.Option(
            help="Path to input CSV file.",
            exists=True,
            dir_okay=False,
        )
    ],
    lon: Annotated[
        str,
        typer.Option(
            help="Name of the longitude column in the CSV."
        )
    ],
    lat: Annotated[
        str,
        typer.Option(
            help="Name of the latitude column in the CSV."
        )
    ],
    crs: Annotated[
        str,
        typer.Option(
            help="CRS of the lat/lon coordinates in the CSV."
        )
    ] = "EPSG:4326",
    output: Annotated[
        Path,
        typer.Option(
            help="Output file path for the GeoJSON.",
            dir_okay=False,
        )
    ] = Path("points.geojson"),
):
    from sentinel_py.common.aoi import csv_to_geojson

    # call core function
    csv_to_geojson(
        csv=csv,
        lon=lon,
        lat=lat,
        crs=crs,
        output=output,
    )


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
        typer.Option(
            exists=True, 
            help="Path to area of interest legible by pyogrio."
        )
    ],
    px: Annotated[
        tuple[float, float], 
        typer.Option(
            help="Grid cell size in decimal degrees as float or tuple of (dx, dy).",
        )
    ],
    crs: Annotated[
        str,
        typer.Option(
            help=(
                "CRS of the input aoi file. Default is EPSG:4326 (lat/lon degrees). "
                "The output grid will always be in EPSG:4326."
            )
        )
    ] = "EPSG:4326",
    fill_holes: Annotated[
        bool, 
        typer.Option(
            help="Fill holes in aoi geometry."
        )
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
        )
    ] = GridClipOpts.intersect.value,
    output: Annotated[
        Path,
        typer.Option(
            help="Output .geojson file.",
        )
    ] = Path("grid.geojson"),
):
    from sentinel_py.common.aoi import overlay_latlon_grid

    # handle cell size input
    try:
        if not all(0.0001 <= v <= 180.0 for v in px):
            raise typer.BadParameter(
                "Cell size values must be between 0.0001 and 180.0"
            )
        dx, dy = px
    except Exception as exc:
        raise typer.BadParameter(
            f"Expected px as float or tuple of (dx, dy), got: {px=}"
        ) from exc

    # call core function
    overlay_latlon_grid(
        aoi=aoi,
        cell_size_deg=(dx, dy),
        crs=crs,
        fill_holes=fill_holes,
        clip=clip,
        output=output,
    )


########################################################################################
# Sentinel-2 commands
########################################################################################

class CDSECollections(str, Enum):
    """CDSE collection names that can be downloaded with this CLI."""
    sentinel2 = "SENTINEL-2"
    # sentinel1 = "SENTINEL-1"  # not implemented yet, but could be


class CDSESentinel2Products(str, Enum):
    """CDSE Sentinel-2 product types that can be downloaded with this CLI."""
    msi2a = "S2MSI2A"
    # msi1c = "S2MSI1C"  # not implemented yet, but could be


class CDSESentinel2Bands(str, Enum):
    """CDSE Sentinel-2 bands that can be downloaded with this CLI."""
    b02 = "B02"
    b03 = "B03"
    b04 = "B04"
    b05 = "B05"
    b06 = "B06"
    b07 = "B07"
    b08 = "B08"
    b8a = "B8A"
    b09 = "B09"
    b10 = "B10"
    b11 = "B11"
    b12 = "B12"

    @classmethod
    def default_bands(cls) -> list["CDSESentinel2Bands"]:
        """Default bands to download if not specified in `download` command."""
        return [
            cls.b02, cls.b03, cls.b04, cls.b05, cls.b06, 
            cls.b07, cls.b08, cls.b8a, cls.b11, cls.b12
        ]


class CDSESentinel2Res(str, Enum):
    r10m = "10"
    r20m = "20"
    r60m = "60"


@s2.command(
    "download",
    help=(
        "Download Sentinel-2 scenes using OData API query parameters. "
        "Data are downloaded from the Copernicus Data Space Ecosystem (CDSE)."
    )
)
def download(
    aoi: Annotated[
        Path, 
        typer.Option(
            help="The aoi file (GeoJSON, shapefile, etc.).",
            exists=True,
            dir_okay=False,
        )
    ],
    outdir: Annotated[
        Path, 
        typer.Option(
            help="Output directory for downloaded data.",
            file_okay=False,
        )
    ],
    years: Annotated[
        str,
        typer.Option(
            help="Space or comma-separated list of years."
        )
    ],
    crs: Annotated[
        str,
        typer.Option(
            help="CRS of the input aoi file. Default is EPSG:4326 (lat/lon degrees)."
        )
    ] = "EPSG:4326",
    speriod: Annotated[
        dt.datetime, 
        typer.Option(
            help="Start month and day of seasonal download window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
        )
    ] = dt.datetime.strptime("01-01", "%m-%d"),
    eperiod: Annotated[
        dt.datetime,
        typer.Option(
            help="End month and day of seasonal download window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
        )
    ] = dt.datetime.strptime("12-31", "%m-%d"),
    collection: Annotated[
        CDSECollections, 
        typer.Option(
            help="CDSE collection name."
        )
    ] = CDSECollections.sentinel2,
    product: Annotated[
        CDSESentinel2Products, 
        typer.Option(
            help="Product type within the collection."
        )
    ] = CDSESentinel2Products.msi2a,
    bands: Annotated[
        list[CDSESentinel2Bands], 
        typer.Option(
            help="List of bands to download."
        )
    ] = CDSESentinel2Bands.default_bands(),
    include_scl: Annotated[
        bool, 
        typer.Option(
            help="Whether to include the SCL band in the download."
        )
    ] = True,
    res: Annotated[
        CDSESentinel2Res, 
        typer.Option(
            help="Target resolution in meters: 10, 20, or 60."
        )
    ] = CDSESentinel2Res.r20m,
    max_workers: Annotated[
        int, 
        typer.Option(
            help="Maximum number of worker threads for file downloads."
        )
    ] = 2,
    log: Annotated[
        Path,
        typer.Option(
            help=(
                "Log file path. If omitted and --verbose is used, logs are written to "
                f"{DEFAULT_LOG_DIR}. Use --verbose for console output."
            )
        )
    ] = None,
    verbose: Annotated[
        bool, 
        typer.Option(
            "--verbose", "-v",
            help="Enable verbose logging to the console."
        )
    ] = False,
):
    from sentinel_py.s2.workflows.download_s2 import download_s2_scenes

    # set up logging if requested
    logger = get_logger(logpath=log, verbose=verbose) if log or verbose else None

    # parse years arg
    try:
        years = [int(y) for y in years.replace(",", " ").split()]
    except ValueError as e:
        raise typer.BadParameter(f"Could not parse years: {e}")

    # parse query args
    collection = collection.value if hasattr(collection, "value") else collection
    product = product.value if hasattr(product, "value") else product
    res = int(res.value if hasattr(res, "value") else res)
    bands = [b.value if hasattr(b, "value") else b for b in bands]

    download_s2_scenes(
        aoi=aoi,
        crs=crs,
        outdir=outdir,
        years=years,
        speriod=speriod,
        eperiod=eperiod,
        s2collection=collection,
        s2product=product,
        s2bands=bands,
        s2res=res,
        include_scl=include_scl,
        max_workers_files=max_workers,
        logger=logger,
    )


def _bandwise_create_pb_offset_vrt(
    band_path: str,
    dn_offset: int,
    out_dir: str,
    dst_nodata: int,
) -> str:
    """
    Worker used in ProcessPoolExecutor.
    Uses only simple, picklable arguments.
    """
    from sentinel_py.s2.s2_masking import create_pb_offset_vrt
    band_p = Path(band_path)
    out_d = Path(out_dir)
    vrt = create_pb_offset_vrt(
        band_jp2_path=band_p,
        dn_offset=dn_offset,
        out_vrt_dir=out_d,
        dst_nodata=dst_nodata,
        logger=None,
    )
    return str(vrt)


@s2.command(
    "dn-offset",
    help=(
        "Determine per-band DN offsets for Sentinel-2 Level-2A products "
        "so later temporal composites and mosaics are radiometrically consistent."
    )
)
def dn_offset(
    input_dir: Annotated[Path, typer.Option(
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Directory with Sentinel-2 L2A products.",
    )],
    output_dir: Annotated[Path, typer.Option(
        help="Output directory for DN offset VRT files.")],
    years: Annotated[str, typer.Option(
        help='Space-separated list of years in quotes. E.g., "2020 2021 2022".')],
    speriod: Annotated[str, typer.Option(
        help="Start of seasonal window as MM-DD. E.g. --speriod 06-01")],
    eperiod: Annotated[str, typer.Option(
        help="End of seasonal window as MM-DD. E.g. --eperiod 08-31")],
    bands: Annotated[list[str], typer.Option(
        help="List of bands to process.")] = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"],
    res: Annotated[int, typer.Option(
        help="Target resolution in meters: 10, 20, or 60.")] = 20,
    log_path: Annotated[Path, typer.Option(
        help="Optional log file path. If omitted, default logging config is used.")] = None,
    verbose: Annotated[bool, typer.Option(
        help="Enable verbose logging to the console.")] = False,
    n_workers: Annotated[int, typer.Option(
        help="Number of parallel workers.")] = 4,
    dst_nodata: Annotated[int, typer.Option(
        help="Nodata value to write into PB-offset VRTs.")] = 65535,
):
    """
    In parallel, compute per-band DN offsets for Sentinel-2 L2A products
    and write PB-offset VRTs.
    """
    from sentinel_py.common.utils import parse_years
    from sentinel_py.s2.s2_masking import (
        get_band_paths, 
        get_pb_offset_from_jp2
    )

    # Set up logging if requested
    if log_path is not None or verbose:
        actual_log_path = setup_logging(log_path, verbose)
        typer.echo(f"Logging to: {actual_log_path}")
    logger = logging.getLogger(__name__)

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse years and seasonal window
    years_set = parse_years(years)
    try:
        start_month, start_day = map(int, speriod.split("-"))
        end_month, end_day = map(int, eperiod.split("-"))
    except Exception as exc:
        raise typer.BadParameter(
            f"Could not parse period_start/period_end as MM-DD: {speriod}, {eperiod}"
        ) from exc
    start_md = (start_month, start_day)
    end_md = (end_month, end_day)

    # get all jp2 band paths
    band_paths_df = get_band_paths(
        input_dir,
        bands,
        res,
        years=years_set,
        period_start=start_md,
        period_end=end_md,
        logger=logger,
    )

    # Compute DN offsets for each band
    band_paths_df["dn_offset"] = band_paths_df["band_jp2_path"].apply(
        lambda p: get_pb_offset_from_jp2(Path(p), logger=logger)
    )

    # In parallel, create PB-offset VRTs
    tasks = [
        (str(Path(row["band_jp2_path"])), int(row["dn_offset"]))
        for _, row in band_paths_df.iterrows()
    ]
    if logger:
        logger.info(
            f"Starting PB-offset VRT creation for {len(tasks)} bands "
            f"using {n_workers} workers. Output dir: {output_dir}"
        )

    # Assign tasks to workers and collects failures
    failures: list[tuple[Path, Exception]] = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        future_to_band = {
            ex.submit(
                _bandwise_create_pb_offset_vrt,
                band_path,
                dn_off,
                output_dir,
                dst_nodata,
            ): Path(band_path)
            for band_path, dn_off in tasks
        }

        for fut in as_completed(future_to_band):
            band_path = future_to_band[fut]
            try:
                vrt_path = Path(fut.result())
                if logger:
                    logger.info(f"Created PB-offset VRT for {band_path} -> {vrt_path}")
            except Exception as exc:
                failures.append((band_path, exc))
                if logger:
                    logger.error(f"Failed PB-offset VRT for {band_path}: {exc!r}")

    if failures:
        typer.echo(
            f"Completed with {len(failures)} failures out of {len(tasks)} bands.",
            err=True,
        )
    else:
        typer.echo(f"Successfully processed {len(tasks)} bands.")


if __name__ == "__main__":
    app()
