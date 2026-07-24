import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer

from sentinel_py.cache import DEFAULT_CDSE_CACHE_DIR
from sentinel_py.enums import (
    CDSECollections,
    CDSEOrbitDirs,
    validate_product,
    validate_sensor_mode,
    validate_serial_id,
)
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()


class CDSES2Product(str, Enum):
    S2MSI2A = "S2MSI2A"
    S2MSI1C = "S2MSI1C"


class CDSES2OpsMode(str, Enum):
    INS_NOBS = "INS-NOBS"
    INS_RAW = "INS-RAW"
    INS_VIC = "INS-VIC"


class CDSES2SerialID(str, Enum):
    A = "A"
    B = "B"
    C = "C"


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
            rich_help_panel="Required Arguments",
        ),
    ],
    crs: Annotated[
        str,
        typer.Option(
            help="CRS of the input aoi file.",
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
    product: Annotated[
        Optional[CDSES2Product],
        typer.Option(
            help=(
                "Sentinel-2 product type: S2MSI1C (Level-1C top-of-atmosphere "
                "reflectance) or S2MSI2A (Level-2A surface reflectance with SCL; "
                "recommended for analysis). Omit to apply no product-type filter. "
                "Source: CDSE catalogue and ESA Sentinel-2 User Guides."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    orbit: Annotated[
        Optional[CDSEOrbitDirs],
        typer.Option(
            help=(
                "Orbit direction: ASCENDING or DESCENDING. Sentinel-2 nominal imaging "
                "products are acquired on descending passes, so this normally need not "
                "be set. Source: ESA Sentinel-2 mission documentation."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    cloud_thresh: Annotated[
        Optional[float],
        typer.Option(
            help=(
                "Keep products whose scene-level cloudCover metadata is strictly less "
                "than specified percentage. This does not replace a pixel-level "
                "cloud/SCL mask."
            ),
            min=0.0,
            max=100.0,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    rel_orbit_num: Annotated[
        Optional[int],
        typer.Option(
            help="Sentinel-2 relative orbit number (1-143).",
            min=1,
            max=143,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    ops_mode: Annotated[
        Optional[CDSES2OpsMode],
        typer.Option(
            help=(
                "Sentinel-2 operational mode: INS-NOBS (normal observation), "
                "INS-RAW, or INS-VIC (vicarious calibration). Source: CDSE catalogue."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    platform_serial_id: Annotated[
        Optional[CDSES2SerialID],
        typer.Option(
            help=(
                "Sentinel-2 satellite identifier: A, B, or C. Availability depends on "
                "date and mission status."
            ),
            case_sensitive=False,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    top: Annotated[
        int,
        typer.Option(
            help=("Number of results to return per page in the OData query."),
            min=1,
            rich_help_panel="Optional Query Configurations",
        ),
    ] = 1000,
    count: Annotated[
        bool,
        typer.Option(
            help=("Whether to continue the OData query when page top is reached."),
            rich_help_panel="Optional Query Configurations",
        ),
    ] = True,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=("Query cache root. Defaults to the current working directory."),
            file_okay=False,
            rich_help_panel="Utils",
        ),
    ] = DEFAULT_CDSE_CACHE_DIR,
    log: Annotated[
        Optional[Path],
        typer.Option(
            help=(
                "Log file path for query execution logs. If omitted, logs are saved to "
                f"{DEFAULT_LOG_DIR} if --verbose is used, otherwise no logs are saved."
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

    from sentinel_py.download.cdse import query_cdse

    # Set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # Parse years
    try:
        parsed_years = sorted({int(year) for year in years.replace(",", " ").split()})
    except ValueError as e:
        raise typer.BadParameter(f"Could not parse years: {e}") from e
    if not parsed_years:
        raise typer.BadParameter("--years must contain at least one year")
    if any(year < 1 or year > 9999 for year in parsed_years):
        raise typer.BadParameter("--years values must be between 1 and 9999")
    if (eperiod.month, eperiod.day) < (speriod.month, speriod.day):
        raise typer.BadParameter(
            "--eperiod must be on or after --speriod within each year"
        )
    speriod_value = dt.date(2000, speriod.month, speriod.day)
    eperiod_value = dt.date(2000, eperiod.month, eperiod.day)

    # Convert CLI enums to the canonical strings expected by the query layer.
    orbit_value = orbit.value if orbit is not None else None
    product_value = product.value if product is not None else None
    ops_mode_value = ops_mode.value if ops_mode is not None else None
    serial_id_value = (
        platform_serial_id.value if platform_serial_id is not None else None
    )
    collection_enum = CDSECollections.sentinel2
    collection = collection_enum.value

    # Parse query args that depend on collection choice
    valid_product = (
        validate_product(collection_enum, product_value)
        if product_value is not None
        else None
    )
    valid_serial_id = (
        validate_serial_id(collection_enum, serial_id_value)
        if serial_id_value is not None
        else None
    )
    valid_sensor_mode = (
        validate_sensor_mode(collection_enum, ops_mode_value)
        if ops_mode_value is not None
        else None
    )

    # Query
    scenes = query_cdse(
        collection=collection,
        product=valid_product,
        years=parsed_years,
        speriod=speriod_value,
        eperiod=eperiod_value,
        aoi=aoi,
        crs=crs,
        cache_dir=cache_dir,
        orbit=orbit_value,
        cloud_thresh=cloud_thresh,
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
