# src/sentinel_py/cli/main.py

from pathlib import Path
from typing import Optional, List
from datetime import datetime
import logging
import sys

import typer

app = typer.Typer(
    help="Sentinel data and workflow CLI.",
    no_args_is_help=True,
)
s2 = typer.Typer(help="Sentinel-2 download, processing, and analysis tools.")
app.add_typer(s2, name="s2")

# Default directory where log files are stored if --log-file is not given
DEFAULT_LOG_DIR = Path.home() / ".sentinel-py" / "logs"


def setup_logging(log_file: Path | None = None, verbose: bool = False) -> Path:
    """
    Configure logging to a file and (optionally verbose) console.

    Parameters
    ----------
    log_file : Path or None
        If given, log file path to write logs to.
        If None, a timestamped log file in DEFAULT_LOG_DIR is created.
    verbose : bool
        If True, console logs at DEBUG. Otherwise, INFO.

    Returns
    -------
    Path
        The resolved log file path in use.
    """
    DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)

    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = DEFAULT_LOG_DIR / f"sentinel_py_{timestamp}.log"

    handlers: list[logging.Handler] = []

    # Console handler (stderr)
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    handlers.append(console)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # full detail to file
    handlers.append(file_handler)

    # force=True so multiple invocations (e.g. tests) reset handlers cleanly
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=handlers,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )

    return log_file


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
    out_file: Path = typer.Option("latlon_aoi.geojson", help="Output .geojson file."),
    log_file: Optional[Path] = typer.Option(
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
    if log_file is not None or verbose:
        actual_log_path = setup_logging(log_file, verbose)
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
        out_file=out_file,
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
    log_file: Optional[Path] = typer.Option(
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
    if log_file is not None or verbose:
        actual_log_path = setup_logging(log_file, verbose)
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


@s2.command(
    "download",
    help=(
        "Download Sentinel-2 scenes using OData API query parameters. "
        "Data are downloaded from the Copernicus Data Space Ecosystem (CDSE)."
    ),
)
def download(
    aoi: Path = typer.Option(..., exists=True, help="AOI file (GeoJSON, shapefile, etc.)."),
    output: Path = typer.Option(..., help="Output directory for downloaded data."),
    years: List[int] = typer.Option(
        ...,
        help="List of year(s) to download (e.g. --years 2020 2021 2022).",
    ),
    period_start: tuple[int, int] = typer.Option(
        ...,
        help="(month, day) for the start of the seasonal window (e.g. (6, 1) for June 1).",
    ),
    period_end: tuple[int, int] = typer.Option(
        ...,
        help="(month, day) for the end of the seasonal window (e.g. (8, 31) for Aug 31).",
    ),
    collection_name: str = "Sentinel-2 MSI Level-2A",
    product_type: str = "S2MSI2A",
    bands: List[str] = typer.Option(
        ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"],
        help="Bands to download.",
    ),
    target_res_m: List[int] = typer.Option(
        [10, 20, 60],
        help="Target resolution in meters for downloaded bands.",
    ),
    credentials: Optional[str] = None,
    max_workers_files: int = 4,
    log_path: Optional[Path] = typer.Option(
        None,
        help=(
            "Optional log file path. If omitted, logs are written to "
            f"{DEFAULT_LOG_DIR} automatically."
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
    Download Sentinel-2 scenes for a seasonal window over an AOI.
    """
    from sentinel_py.s2.workflows.download_s2 import download_s2_seasonal_scenes

    # Always log for downloads (they are longer and more complex)
    actual_log_path = setup_logging(log_path, verbose)
    typer.echo(f"Logging to: {actual_log_path}")
    
    logger = logging.getLogger("sentinel_py.s2.workflows.download_s2")
    download_s2_seasonal_scenes(
        aoi=aoi,
        output_root=output,
        years=years,
        period_start=period_start,
        period_end=period_end,
        collection_name=collection_name,
        product_type=product_type,
        bands=bands,
        target_res_m=target_res_m,
        credentials=credentials,
        max_workers_files=max_workers_files,
        logger=logger,
    )


if __name__ == "__main__":
    app()
