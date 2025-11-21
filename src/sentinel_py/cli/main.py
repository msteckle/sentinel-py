from pathlib import Path
from typing import Optional, List
import datetime as dt
import logging
import sys
from venv import logger
from concurrent.futures import ProcessPoolExecutor, as_completed

import typer
from typing_extensions import Annotated

# Set up main app and subcommands
app = typer.Typer(
    help="Sentinel data and workflow CLI.",
    no_args_is_help=True,
)
s1 = typer.Typer(help="Sentinel-1 download, processing, and analysis tools.")
app.add_typer(s1, name="s1")
s2 = typer.Typer(help="Sentinel-2 download, processing, and analysis tools.")
app.add_typer(s2, name="s2")


# Default directory where log files are stored if --log-file is not given
DEFAULT_LOG_DIR = Path.home() / ".sentinel-py" / "logs"

def setup_logging(log_path: Path | None = None, verbose: bool = False) -> Path:
    """
    Configure logging so that the *directory or prefix* is user-defined, but
    the log file name is automatically generated.

    Parameters
    ----------
    log_path : Path or None
        - If None: logs go in ~/.sentinel-py/logs/sentinel_py_<timestamp>.log
        - If a directory: the file is created inside it
        - If a file-like path: acts as a prefix; timestamp and .log are appended
    verbose : bool
        True -> console logs at DEBUG level.

    Returns
    -------
    Path
        Fully resolved path to the created log file.
    """
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Determine final log file path
    if log_path is None:
        # Default location
        log_dir = DEFAULT_LOG_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        logfile = log_dir / f"sentinel_py_{timestamp}.log"

    else:
        # Normalize
        log_path = Path(log_path)

        if log_path.exists() and log_path.is_dir():
            # User gave a directory → use it
            log_path.mkdir(parents=True, exist_ok=True)
            logfile = log_path / f"sentinel_py_{timestamp}.log"

        else:
            # User gave a prefix (e.g., logs/myrun)
            parent = log_path.parent
            if parent != Path('.'):
                parent.mkdir(parents=True, exist_ok=True)
            prefix = log_path.name
            logfile = parent / f"{prefix}_{timestamp}.log"

    # Configure handlers
    handlers: list[logging.Handler] = []

    # Console handler
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    handlers.append(console)

    # File handler
    file_handler = logging.FileHandler(logfile)
    file_handler.setLevel(logging.DEBUG)
    handlers.append(file_handler)

    logging.basicConfig(
        level=logging.DEBUG,
        handlers=handlers,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )
    return logfile


@app.command(
    "aoi",
    help=(
        "Create an Area Of Interest (AOI) GeoJSON from a bounding box tuple. "
        'The bounding box is given as "xmin, ymin, xmax, ymax".'
    ),
)
def aoi(
    bbox: str = typer.Option(
        ...,
        help=(
            "Bounding box in form xmin,ymin,xmax,ymax "
            "(e.g. '-150,68,-148,70'). Commas or spaces are accepted."
        ),
    ),
    crs: str = typer.Option("EPSG:4326", help="CRS for AOI."),
    output_file: Path = typer.Option("latlon_aoi.geojson", help="Output .geojson file."),
    log_path: Optional[Path] = typer.Option(
        None,
        help=(
            "Optional log file path. If omitted and --verbose is used, logs are "
            f"written to {DEFAULT_LOG_DIR}. Use --verbose for console output."
        ),
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging to the console.",
    ),
):
    """
    Create an AOI polygon from a bounding box and optionally write it to GeoJSON.
    """
    from sentinel_py.common.aoi import create_aoi_geojson

    # Optional logging: enabled only if user requests file or verbose output
    if log_path is not None or verbose:
        actual_log_path = setup_logging(log_path, verbose)
        typer.echo(f"Logging to: {actual_log_path}")

    # Parse user-provided bbox string: accept commas OR spaces
    try:
        raw = bbox.replace(",", " ").split()
        parts = [float(p) for p in raw]
        if len(parts) != 4:
            raise ValueError
        xmin, ymin, xmax, ymax = parts
    except Exception:
        typer.secho(
            (
                "Error: bbox must be 4 floats like 'xmin,ymin,xmax,ymax' "
                "(e.g. '-150,68,-148,70')."
            ),
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    # Validate coordinates
    if xmin >= xmax:
        typer.secho(
            "Error: xmin must be less than xmax.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    if ymin >= ymax:
        typer.secho(
            "Error: ymin must be less than ymax.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    # Call core function
    create_aoi_geojson(
        bbox=(xmin, ymin, xmax, ymax),
        crs=crs,
        out_file=output_file,
    )


@app.command(
    "grid",
    help=(
        "Create a lat/lon grid overlaying an AOI GeoJSON file for a specified "
        "cell size in degrees. The grid can be used for future parallel processing."
    ),
)
def grid(
    aoi_file: Path = typer.Option(..., exists=True, help="AOI .geojson file."),
    dx_deg: float = typer.Option(..., help="Grid cell size in degrees (longitude)."),
    dy_deg: float = typer.Option(..., help="Grid cell size in degrees (latitude)."),
    crs: str = typer.Option("EPSG:4326", help="CRS for AOI and grid."),
    clip_to_aoi: bool = typer.Option(True, help="Clip grid cells to AOI."),
    fill_aoi_holes: bool = typer.Option(True, help="Fill holes in AOI geometry."),
    fill_cell_holes: bool = typer.Option(True, help="Fill holes in grid cells."),
    out_file: Path = typer.Option("latlon_grid.geojson", help="Output .geojson file."),
    log_path: Optional[Path] = typer.Option(
        None,
        help=(
            "Optional log file path. If omitted and --verbose is used, logs are "
            f"written to {DEFAULT_LOG_DIR}. Use --verbose for console output."
        ),
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging to the console.",
    ),
):
    """
    Create a regular lat/lon grid over an AOI and optionally write to GeoJSON.
    """
    from sentinel_py.common.aoi import overlay_latlon_grid

    if dx_deg <= 0 or dy_deg <= 0:
        typer.secho(
            "Error: dx_deg and dy_deg must be positive.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    # Optional logging
    if log_path is not None or verbose:
        actual_log_path = setup_logging(log_path, verbose)
        typer.echo(f"Logging to: {actual_log_path}")

    overlay_latlon_grid(
        aoi=aoi_file,
        cell_size_deg=(dx_deg, dy_deg),
        crs=crs,
        clip_to_aoi=clip_to_aoi,
        fill_aoi_holes=fill_aoi_holes,
        fill_cell_holes=fill_cell_holes,
        out_file=out_file,
    )


@app.command(
    "translate",
    help=(
        "Wrapper around gdal.Translate to convert raster files to different formats. " \
        "E.g., convert VRT to GeoTIFF."
    ),
)
def translate(
    src_file: Annotated[Path, typer.Option(
        help="Source raster file path.")],
    dst_file: Annotated[Path, typer.Option(
        help="Destination raster file path.")],
    options: Annotated[List[str], typer.Option(
        help=(
            "List of gdal.Translate options as strings. "
            "E.g., --options '-of VRT' '-co COMPRESS=LZW'"
        ))] | None = None,
):
    """
    GDAL Translate with Python PixelFunction support enabled.
    """
    from sentinel_py.common.gdal import gdaltranslate

    gdaltranslate(
        src_file=src_file,
        dst_file=dst_file,
        options=options,
    )


@s2.command(
    "download",
    help=(
        "Download Sentinel-2 scenes using OData API query parameters. "
        "Data are downloaded from the Copernicus Data Space Ecosystem (CDSE)."
    ),
)
def download(
    input_aoi: Annotated[Path, typer.Option(
        help="AOI file (GeoJSON, shapefile, etc.).")],
    output_dir: Annotated[Path, typer.Option(
        help="Output directory for downloaded data.")],
    years: Annotated[str, typer.Option(
        help="""Space-separated list of years in quotes. E.g., "2020 2021 2022".""")],
    speriod: Annotated[str, typer.Option(
        help="Start of seasonal window as MM-DD. E.g. --period-start 06-01)")],
    eperiod: Annotated[str, typer.Option(
        help="End of seasonal window as MM-DD. E.g. --period-end 08-31)")],
    collection: Annotated[str, typer.Option(
        help="CDSE collection name.")] = "SENTINEL-2",
    product: Annotated[str, typer.Option(
        help="Product type within the collection.")] = "S2MSI2A",
    bands: Annotated[List[str], typer.Option(
        help="List of bands to download.")] = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"],
    include_scl: Annotated[bool, typer.Option(
        help="Include the SCL band in the download.")] = True,
    res: Annotated[int, typer.Option(
        help="Target resolution in meters: 10, 20, or 60.")] = 20,
    max_workers: Annotated[int, typer.Option(
        help="Maximum number of worker threads for file downloads.")] = 4,
    log_path: Annotated[Path, typer.Option(
        help=f"Optional log file path. If omitted, logs are written to {DEFAULT_LOG_DIR} automatically.")] = None,
    verbose: Annotated[bool, typer.Option(
        help="Enable verbose logging to the console.")] = False,
):
    """
    Download Sentinel-2 scenes for a seasonal window over an AOI.
    """
    from sentinel_py.s2.workflows.download_s2 import download_s2_scenes

    # Unpack years from space-separated string
    try:
        years = [int(y) for y in years.split()]
    except ValueError:
        typer.secho(
            """Error: --years must be space-separated integers, e.g. --years "2020 2021 2022".""",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)
    
    # Unpack month/day from period_start and period_end
    try:
        smonth, sday = [int(part) for part in speriod.split("-")]
        emonth, eday = [int(part) for part in eperiod.split("-")]
    except ValueError:
        typer.secho(
            """Error: --speriod and --eperiod must be in MM-DD format, e.g. --speriod 06-01.""",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    # Always log for downloads (they are longer and more complex)
    actual_log_path = setup_logging(log_path, verbose)
    typer.echo(f"Logging to: {actual_log_path}")
    
    logger = logging.getLogger("sentinel_py.s2.workflows.download_s2")
    download_s2_scenes(
        aoi_path=input_aoi,
        output_root=output_dir,
        years=years,
        period_start=(smonth, sday),
        period_end=(emonth, eday),
        collection_name=collection,
        product_type=product,
        bands=bands,
        target_res_m=res,
        include_scl=include_scl,
        max_workers_files=max_workers,
        logger=logger,
    )


def _process_one_band(
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
    bands: Annotated[List[str], typer.Option(
        help="List of bands to process.")] = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"],
    res: Annotated[int, typer.Option(
        help="Target resolution in meters: 10, 20, or 60.")] = 20,
    log_path: Annotated[Path | None, typer.Option(
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
        get_pb_offset_from_jp2, 
        create_pb_offset_vrt
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
                _process_one_band,
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
