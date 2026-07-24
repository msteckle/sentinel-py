import datetime as dt
from pathlib import Path
from typing import Annotated, Optional

import typer

from sentinel_py.cache import DEFAULT_CDSE_CACHE_DIR
from sentinel_py.enums import (
    CDSECollections,
    CDSEOrbitDirs,
    S1Swaths,
    validate_product,
    validate_sensor_mode,
    validate_serial_id,
)
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()


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
    crs: Annotated[
        str,
        typer.Option(help="CRS of the input aoi file."),
    ],
    years: Annotated[
        str, typer.Option(help="Space- or comma-separated list of years.")
    ],
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "Query cache root. Defaults to the hidden .cdse-cache directory in "
                "the current working directory."
            ),
            file_okay=False,
        ),
    ] = DEFAULT_CDSE_CACHE_DIR,
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
    collection: Annotated[
        CDSECollections, typer.Option(help="Filter by CDSE Collection.")
    ] = CDSECollections.sentinel2,
    product: Annotated[
        Optional[str], typer.Option(help="Filter by product of collection.")
    ] = None,
    orbit: Annotated[
        Optional[CDSEOrbitDirs],
        typer.Option(help=("Filter by 'ASCENDING' or 'DESCENDING'.")),
    ] = None,
    cloud_thresh: Annotated[
        Optional[float],
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
        Optional[int],
        typer.Option(help=("Filter by burst ID (Sentinel-1).")),
    ] = None,
    swath_id: Annotated[
        Optional[S1Swaths],
        typer.Option(help=("Filter by swath identifier (Sentinel-1).")),
    ] = None,
    rel_orbit_num: Annotated[
        Optional[int],
        typer.Option(
            help=("Filter by relative orbit number. Max 143 for S2; Max 175 for S1."),
            min=1,
            max=175,
        ),
    ] = None,
    ops_mode: Annotated[
        Optional[str],
        typer.Option(help=("Filter by operation mode.")),
    ] = None,
    platform_serial_id: Annotated[
        Optional[str],
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
        Optional[Path],
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

    from sentinel_py.download.cdse import query_cdse

    # Set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # Parse years
    try:
        parsed_years = [int(y) for y in years.replace(",", " ").split()]
    except ValueError as e:
        raise typer.BadParameter(f"Could not parse years: {e}")

    # Parse query single item args
    collection = collection.value if hasattr(collection, "value") else collection  # type: ignore
    orbit = orbit.value if hasattr(orbit, "value") else orbit  # type: ignore
    swath_id = swath_id.value if hasattr(swath_id, "value") else swath_id  # type: ignore

    # Parse query args that depend on collection choice
    valid_product = (
        validate_product(CDSECollections(collection), product)
        if product is not None
        else None
    )
    valid_serial_id = (
        validate_serial_id(CDSECollections(collection), platform_serial_id)
        if platform_serial_id is not None
        else None
    )
    valid_sensor_mode = (
        validate_sensor_mode(CDSECollections(collection), ops_mode)
        if ops_mode is not None
        else None
    )

    # Query
    scenes = query_cdse(
        collection=collection,
        product=valid_product,
        years=parsed_years,
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
    # Report query results
    if scenes is not None:
        cache_path = scenes.attrs.get("cache_path")
        if cache_path:
            typer.echo(f"Cached query manifest: {cache_path}")
        typer.echo(f"Found {len(scenes)} unique scene(s).")
