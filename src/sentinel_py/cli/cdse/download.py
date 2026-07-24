import configparser
import time
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer
import pandas as pd

from sentinel_py.cache import DEFAULT_CDSE_CACHE_DIR
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()

CDSE_S3_CREDENTIALS_URL = (
    "https://eodata-s3keysmanager.dataspace.copernicus.eu/panel/s3-credentials"
)
S5_CONFIG_EXAMPLE = """[default]
aws_access_key_id = <YOUR_CDSE_S3_ACCESS_KEY>
aws_secret_access_key = <YOUR_CDSE_S3_SECRET_KEY>
aws_region = default
host_base = eodata.dataspace.copernicus.eu
use_https = true"""
REQUIRED_S5_CONFIG_KEYS = (
    "aws_access_key_id",
    "aws_secret_access_key",
    "host_base",
)


class CDSES2Bands(str, Enum):
    """Enum of valid Sentinel-2 bands for CDSE download."""

    B01 = "B01"
    B02 = "B02"
    B03 = "B03"
    B04 = "B04"
    B05 = "B05"
    B06 = "B06"
    B07 = "B07"
    B08 = "B08"
    B8A = "B8A"
    B09 = "B09"
    B10 = "B10"
    B11 = "B11"
    B12 = "B12"
    SCL = "SCL"
    TCI = "TCI"
    AOT = "AOT"
    WVP = "WVP"


class CDSES2Resolutions(int, Enum):
    """Enum of valid Sentinel-2 resolutions for CDSE download."""

    R10M = 10
    R20M = 20
    R60M = 60


def _s5_config_error(config: Path, reason: str) -> typer.BadParameter:
    return typer.BadParameter(
        f"Invalid CDSE S3 configuration: {reason}\n\n"
        "CDSE catalogue login credentials are not S3 credentials. Generate an "
        f"S3 access key and secret key at:\n{CDSE_S3_CREDENTIALS_URL}\n\n"
        f"Then create {config} with this shape:\n\n"
        f"{S5_CONFIG_EXAMPLE}\n\n"
        f"Protect the file with:\nchmod 600 {config}\n\n"
        "Do not commit this file or share its contents."
    )


def _validate_s5_config(config: Path) -> None:
    """Validate the s5cmd configuration before starting download workers."""
    if not config.is_file():
        raise _s5_config_error(config, f"file {config} was not found.")

    parser = configparser.ConfigParser()
    try:
        with config.open() as config_stream:
            parser.read_file(config_stream)
    except (OSError, configparser.Error) as error:
        raise _s5_config_error(config, f"could not read {config}: {error}") from error

    if "default" not in parser:
        raise _s5_config_error(config, "missing the [default] section.")

    missing = [
        key
        for key in REQUIRED_S5_CONFIG_KEYS
        if not parser["default"].get(key, "").strip()
    ]
    if missing:
        raise _s5_config_error(
            config,
            "missing or empty required setting(s): " + ", ".join(missing) + ".",
        )


@app.command(
    help="Download selected assets from S3 using a cached CDSE query manifest.",
)
def download(
    mission: Annotated[
        str,
        typer.Option(
            help="Mission name. Currently only Sentinel-2 (S2) is fully supported and "
            "tested.",
            rich_help_panel="Required Arguments",
        ),
    ],
    bands: Annotated[
        str,
        typer.Option(
            help=(
                "Space- or comma-separated Sentinel-2 assets: B01-B12, B8A, SCL, "
                "TCI, AOT, or WVP."
            ),
            rich_help_panel="Required Arguments",
        ),
    ],
    outdir: Annotated[
        Path,
        typer.Option(
            help=("Output directory for downloaded files. Must exist and be writable."),
            file_okay=False,
            rich_help_panel="Required Arguments",
        ),
    ],
    res: Annotated[
        CDSES2Resolutions,
        typer.Option(
            help=(
                "Target resolution in meters for the bands to download. Only used "
                "for Sentinel-2. Options: 10, 20, or 60."
            ),
            rich_help_panel="Required Arguments",
        ),
    ],
    config: Annotated[
        Path,
        typer.Option(
            help=(
                "Path to an INI file containing CDSE S3 credentials. If the file is "
                "missing or malformed, setup instructions are displayed."
            ),
            rich_help_panel="Required Arguments",
        ),
    ],
    query: Annotated[
        Optional[Path],
        typer.Option(
            help=("Path to a cache of CDSE query results stored as a parquet."),
            exists=True,
            dir_okay=False,
            rich_help_panel="Optional Download Configurations",
        ),
    ] = None,
    parallel_scenes: Annotated[
        int,
        typer.Option(
            help="Number of scenes to download in parallel.",
            min=1,
            rich_help_panel="Optional Download Configurations",
        ),
    ] = 2,
    parallel_bands: Annotated[
        int,
        typer.Option(
            help="Number of bands to download in parallel within each scene.",
            min=1,
            rich_help_panel="Optional Download Configurations",
        ),
    ] = 4,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "Query and image cache root. Defaults to the hidden .cdse-cache "
                "directory in the current working directory."
            ),
            file_okay=False,
            rich_help_panel="Utils",
        ),
    ] = DEFAULT_CDSE_CACHE_DIR,
    log: Annotated[
        Optional[Path],
        typer.Option(
            help=(
                "Log file path for download execution logs. If omitted, logs are saved "
                f"to {DEFAULT_LOG_DIR}."
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

    from sentinel_py.download.cdse import (
        find_latest_scenes_cache,
        resolve_and_download,
    )

    _validate_s5_config(config)

    mission_value = mission.upper()
    if mission_value != "S2":
        raise typer.BadParameter(
            "CDSE download currently supports only Sentinel-2; use --mission S2"
        )
    requested_bands = [
        value.upper() for value in bands.replace(",", " ").split() if value.strip()
    ]
    if not requested_bands:
        raise typer.BadParameter("--bands must contain at least one band")
    try:
        band_values = [CDSES2Bands(value).value for value in requested_bands]
    except ValueError as error:
        supported = ", ".join(band.value for band in CDSES2Bands)
        raise typer.BadParameter(
            f"Unsupported Sentinel-2 band in --bands. Supported values: {supported}"
        ) from error
    resolution_value = res.value

    # Set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # Validate inputs
    if query is None:
        # If no query cache is provided, find the latest query cache in the cache_dir
        query = find_latest_scenes_cache(Path(cache_dir))
        if not query:
            raise typer.BadParameter(f"No scenes.parquet found in {cache_dir}")
        logger.info(f"Using most recent query cache: {query}")

    scenes = pd.read_parquet(query, columns=["Name"])
    scene_count = len(scenes)
    requested_images_per_scene = len(set(band_values))
    image_label = "image" if requested_images_per_scene == 1 else "images"
    typer.echo(f"Cached query: {query}")
    typer.echo(
        f"Found {scene_count} scenes: "
        f"{requested_images_per_scene} requested {image_label} per scene"
    )

    started = time.time()
    started_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started))
    results = resolve_and_download(
        scenes_cache=query,
        mission=mission_value,
        bands=band_values,
        resolution=resolution_value,
        output_dir=outdir,
        config_file=str(config),
        parallel_scenes=parallel_scenes,
        parallel_bands=parallel_bands,
        logger=logger,
    )
    ended = time.time()
    ended_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ended))
    elapsed = ended - started
    downloaded = sum(len(result.succeeded) for result in (results or []))
    skipped = sum(len(result.skipped) for result in (results or []))
    failed = sum(len(result.failed) for result in (results or []))
    status_file = outdir / ".sentinel-py" / "cdse_downloads.parquet"

    typer.echo("Summary:")
    typer.echo(f"  Download started: {started_text}")
    typer.echo(f"  Download ended:   {ended_text}")
    typer.echo(f"  Elapsed time:     {elapsed:.1f} seconds for {scene_count} scenes")
    typer.echo(
        f"  Results:          {downloaded} downloaded, {skipped} skipped, "
        f"{failed} failed"
    )
    typer.echo(f"  Download status:  {status_file}")

    # Report any failed downloads
    failures = [
        (result.scene_name, failure)
        for result in (results or [])
        for failure in result.failed
    ]
    if failures:
        typer.echo(
            f"Download completed with {len(failures)} failed required asset(s).",
            err=True,
        )
        raise typer.Exit(code=1)
