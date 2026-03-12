import datetime as dt
from concurrent.futures import ProcessPoolExecutor, as_completed
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.common.enums import (
    CDSECollections,
    CDSEOrbitDirs,
    S1Swaths,
    validate_product,
    validate_sensor_mode,
    validate_serial_id,
)
from sentinel_py.common.logging import DEFAULT_LOG_DIR, get_logger

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
    ] = GridClipOpts.intersect.value,
    output: Annotated[
        Path,
        typer.Option(
            help="Output .geojson file.",
        ),
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


# sentinel-py query --------------------------------------------------------------------
@app.command(
    "query",
    help=(
        "Query CDSE scenes using OData API query parameters. "
        "Query results are retrieved from the Copernicus Data Space Ecosystem (CDSE)."
    ),
)
def query(
    aoi: Annotated[
        Path,
        typer.Option(
            help="The aoi file used to filter the query (GeoJSON, shapefile, etc.).",
            exists=True,
            dir_okay=False,
        ),
    ],
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=("Directory for caching query results. Must exist and be writable. "),
            file_okay=False,
        ),
    ],
    crs: Annotated[
        str,
        typer.Option(help="CRS of the input aoi file."),
    ],
    years: Annotated[
        str, typer.Option(help="Space- or comma-separated list of years.")
    ],
    speriod: Annotated[
        dt.datetime,
        typer.Option(
            help="Start month and day of seasonal query window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
        ),
    ] = dt.datetime.strptime("01-01", "%m-%d"),
    eperiod: Annotated[
        dt.datetime,
        typer.Option(
            help="End month and day of seasonal query window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
        ),
    ] = dt.datetime.strptime("12-31", "%m-%d"),
    collection: Annotated[
        CDSECollections, typer.Option(help="Filter by CDSE Collection.")
    ] = CDSECollections.sentinel2,
    product: Annotated[
        str, typer.Option(help="Filter by product of collection.")
    ] = None,
    orbit: Annotated[
        CDSEOrbitDirs,
        typer.Option(help=("Filter by 'ASCENDING' or 'DESCENDING'.")),
    ] = None,
    cloud_thresh: Annotated[
        float,
        typer.Option(
            help=("Filter by cloud cover percentage threshold (>0-100)."),
            min=0.0,
            max=100.0,
        ),
    ] = None,
    burst_mode: Annotated[
        bool,
        typer.Option(help=("True/False to filter by burst mode (Sentinel-1).")),
    ] = False,
    burst_id: Annotated[
        int,
        typer.Option(help=("Filter by burst ID (Sentinel-1).")),
    ] = None,
    swath_id: Annotated[
        S1Swaths,
        typer.Option(help=("Filter by swath identifier (Sentinel-1).")),
    ] = None,
    rel_orbit_num: Annotated[
        int,
        typer.Option(
            help=("Filter by relative orbit number. Max 143 for S2; Max 175 for S1."),
            min=1,
            max=175,
        ),
    ] = None,
    ops_mode: Annotated[
        str,
        typer.Option(help=("Filter by operation mode.")),
    ] = None,
    platform_serial_id: Annotated[
        str,
        typer.Option(help=("Filter by platform serial identifier.")),
    ] = None,
    top: Annotated[
        int,
        typer.Option(
            help=(
                "Number of results to return per page in the OData query. Default is "
                "1000."
            )
        ),
    ] = 1000,
    count: Annotated[
        bool,
        typer.Option(
            help=("Whether to continue the OData query when page top is reached.")
        ),
    ] = True,
    log: Annotated[
        Path,
        typer.Option(
            help=(
                "Log file path for query execution logs. If omitted, logs are saved to "
                f"{DEFAULT_LOG_DIR} if --verbose is used, otherwise no logs are saved."
            )
        ),
    ] = None,
    verbose: Annotated[
        bool, typer.Option(help="Enable verbose logging to the console and log file.")
    ] = False,
):

    from sentinel_py.common.download import query_cdse
    from sentinel_py.common.logging import get_logger

    # set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # parse years
    try:
        years = [int(y) for y in years.replace(",", " ").split()]
    except ValueError as e:
        raise typer.BadParameter(f"Could not parse years: {e}")

    # parse query single item args
    collection = collection.value if hasattr(collection, "value") else collection
    orbit = orbit.value if hasattr(orbit, "value") else orbit
    swath_id = swath_id.value if hasattr(swath_id, "value") else swath_id

    # parse query args that depend on collection choice
    valid_product = validate_product(CDSECollections(collection), product)
    valid_serial_id = validate_serial_id(
        CDSECollections(collection), platform_serial_id
    )
    valid_sensor_mode = validate_sensor_mode(CDSECollections(collection), ops_mode)

    # query
    query_cdse(
        collection=collection,
        product=valid_product,
        years=years,
        speriod=speriod.date(),
        eperiod=eperiod.date(),
        aoi=aoi,
        crs=crs,
        cache_dir=cache_dir,
        orbit=orbit,
        cloud_thresh=cloud_thresh,
        burst_mode=burst_mode,
        burst_id=burst_id,
        swath_id=swath_id,
        rel_orbit_num=rel_orbit_num,
        ops_mode=valid_sensor_mode,
        platform_serial_id=valid_serial_id,
        top=top,
        count=count,
        logger=logger,
    )


# sentinel-py download -----------------------------------------------------------------
@app.command(
    "download",
    help=("Download from S3 given a cache of CDSE query results. Not implemented yet."),
)
def download(
    mission: Annotated[
        str, typer.Option(help="Mission name to filter the query cache for download.")
    ],
    bands: Annotated[
        str, typer.Option(help="Space- or comma-separated list of bands to download.")
    ],
    outdir: Annotated[
        Path,
        typer.Option(
            help=("Output directory for downloaded files. Must exist and be writable."),
            file_okay=False,
        ),
    ],
    res: Annotated[
        int,
        typer.Option(
            help=(
                "Target resolution in meters for the bands to download. Only used "
                "for Sentinel-2. Options: 10, 20, or 60."
            ),
        ),
    ],
    config: Annotated[
        Path,
        typer.Option(
            help=("Path to .s5cfg file with AWS credentials for S3 download access.")
        ),
    ],
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "Directory for caching target file information. Must exist and be "
                "writable."
            ),
            file_okay=False,
        ),
    ],
    query: Annotated[
        Path,
        typer.Option(
            help=("Path to a cache of CDSE query results stored as a parquet.")
        ),
    ] = None,
    parallel_scenes: Annotated[
        int, typer.Option(help="Number of scenes to download in parallel.")
    ] = 2,
    parallel_bands: Annotated[
        int,
        typer.Option(help="Number of bands to download in parallel within each scene."),
    ] = 4,
    log: Annotated[
        Path,
        typer.Option(
            help=(
                "Log file path for download execution logs. If omitted, logs are saved "
                f"to {DEFAULT_LOG_DIR}."
            )
        ),
    ] = None,
    verbose: Annotated[
        bool, typer.Option(help="Enable verbose logging to the console and log file.")
    ] = False,
):

    from sentinel_py.common.download import (
        find_latest_scenes_cache,
        resolve_and_download,
    )

    # set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # load most recent query cache if not provided
    if query is None:
        query = find_latest_scenes_cache(Path(cache_dir))
        if not query:
            raise typer.BadParameter(f"No scenes.parquet found in {cache_dir}")
        logger.info(f"Using most recent query cache: {query}")

    resolve_and_download(
        scenes_cache=query,
        mission=mission,
        bands=[b.strip() for b in bands.replace(",", " ").split()],
        resolution=res,
        output_dir=outdir,
        config_file=config,
        parallel_scenes=parallel_scenes,
        parallel_bands=parallel_bands,
        logger=logger,
    )


########################################################################################
# Sentinel-2 commands
########################################################################################


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
    ),
)
def dn_offset(
    input_dir: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=False,
            dir_okay=True,
            help="Directory with Sentinel-2 L2A products.",
        ),
    ],
    output_dir: Annotated[
        Path, typer.Option(help="Output directory for DN offset VRT files.")
    ],
    years: Annotated[
        str,
        typer.Option(
            help='Space-separated list of years in quotes. E.g., "2020 2021 2022".'
        ),
    ],
    speriod: Annotated[
        str,
        typer.Option(help="Start of seasonal window as MM-DD. E.g. --speriod 06-01"),
    ],
    eperiod: Annotated[
        str, typer.Option(help="End of seasonal window as MM-DD. E.g. --eperiod 08-31")
    ],
    bands: Annotated[list[str], typer.Option(help="List of bands to process.")] = [
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B8A",
        "B11",
        "B12",
    ],
    res: Annotated[
        int, typer.Option(help="Target resolution in meters: 10, 20, or 60.")
    ] = 20,
    log: Annotated[
        Path,
        typer.Option(
            help="Optional log file path. If omitted, default logging config is used."
        ),
    ] = None,
    verbose: Annotated[
        bool, typer.Option(help="Enable verbose logging to the console.")
    ] = False,
    n_workers: Annotated[int, typer.Option(help="Number of parallel workers.")] = 4,
    dst_nodata: Annotated[
        int, typer.Option(help="Nodata value to write into PB-offset VRTs.")
    ] = 65535,
):
    """
    In parallel, compute per-band DN offsets for Sentinel-2 L2A products
    and write PB-offset VRTs.
    """
    from sentinel_py.common.utils import parse_years
    from sentinel_py.s2.s2_masking import get_band_paths, get_pb_offset_from_jp2

    # set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

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
