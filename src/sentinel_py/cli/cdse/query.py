"""CLI command for querying Sentinel-2 scenes from CDSE."""

import datetime as dt
from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.cache import DEFAULT_CDSE_CACHE_DIR
from sentinel_py.cli.options import (
    PANEL_QUERY,
    PANEL_REQUIRED,
    EPeriodOpt,
    LogOpt,
    SPeriodOpt,
    VerboseOpt,
    YearsOpt,
    cache_dir_option,
    parse_years,
    validate_season,
)
from sentinel_py.enums import (
    CDSECollections,
    CDSEOrbitDirs,
    S2Products,
    S2SensorModes,
    S2SerialIds,
    validate_product,
    validate_sensor_mode,
    validate_serial_id,
)
from sentinel_py.log import get_logger

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
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    crs: Annotated[
        str,
        typer.Option(
            help="CRS of the input aoi file.",
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    years: YearsOpt,
    speriod: SPeriodOpt = dt.datetime(2000, 1, 1),
    eperiod: EPeriodOpt = dt.datetime(2000, 12, 31),
    product: Annotated[
        S2Products | None,
        typer.Option(
            help=(
                "Sentinel-2 product type: S2MSI1C (Level-1C top-of-atmosphere "
                "reflectance) or S2MSI2A (Level-2A surface reflectance with SCL; "
                "recommended for analysis). Omit to apply no product-type filter. "
                "Source: CDSE catalogue and ESA Sentinel-2 User Guides."
            ),
            case_sensitive=False,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    orbit: Annotated[
        CDSEOrbitDirs | None,
        typer.Option(
            help=(
                "Orbit direction: ASCENDING or DESCENDING. Sentinel-2 nominal imaging "
                "products are acquired on descending passes, so this normally need not "
                "be set. Source: ESA Sentinel-2 mission documentation."
            ),
            case_sensitive=False,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    cloud_thresh: Annotated[
        float | None,
        typer.Option(
            help=(
                "Keep products whose scene-level cloudCover metadata is strictly less "
                "than specified percentage. This does not replace a pixel-level "
                "cloud/SCL mask."
            ),
            min=0.0,
            max=100.0,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    rel_orbit_num: Annotated[
        int | None,
        typer.Option(
            help="Sentinel-2 relative orbit number (1-143).",
            min=1,
            max=143,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    ops_mode: Annotated[
        S2SensorModes | None,
        typer.Option(
            help=(
                "Sentinel-2 operational mode: INS-NOBS (normal observation), "
                "INS-RAW, or INS-VIC (vicarious calibration). Source: CDSE catalogue."
            ),
            case_sensitive=False,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    platform_serial_id: Annotated[
        S2SerialIds | None,
        typer.Option(
            help=(
                "Sentinel-2 satellite identifier: A, B, or C. Availability depends on "
                "date and mission status."
            ),
            case_sensitive=False,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = None,
    top: Annotated[
        int,
        typer.Option(
            help=("Number of results to return per page in the OData query."),
            min=1,
            rich_help_panel=PANEL_QUERY,
        ),
    ] = 1000,
    count: Annotated[
        bool,
        typer.Option(
            help=("Whether to continue the OData query when page top is reached."),
            rich_help_panel=PANEL_QUERY,
        ),
    ] = True,
    cache_dir: cache_dir_option("CDSE", DEFAULT_CDSE_CACHE_DIR) = DEFAULT_CDSE_CACHE_DIR,
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Query CDSE for Sentinel-2 scenes and cache the scene manifest."""
    from sentinel_py.download.cdse import query_cdse

    # Set up logging
    logger = get_logger(name="cdse_query_logger", logpath=log, verbose=verbose)

    # Parse years and season
    parsed_years = parse_years(years)
    validate_season(speriod, eperiod)
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
