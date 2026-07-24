import configparser
from pathlib import Path
from typing import Annotated, Optional

import typer

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
            help=(
                "Path to an INI file containing CDSE S3 credentials. If the file is "
                "missing or malformed, setup instructions are displayed."
            )
        ),
    ],
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "Query and image cache root. Defaults to the hidden .cdse-cache "
                "directory in the current working directory."
            ),
            file_okay=False,
        ),
    ] = DEFAULT_CDSE_CACHE_DIR,
    query: Annotated[
        Optional[Path],
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
        Optional[Path],
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

    from sentinel_py.download.cdse import (
        find_latest_scenes_cache,
        resolve_and_download,
    )

    _validate_s5_config(config)

    # Set up logging
    logger = get_logger(name="download_logger", logpath=log, verbose=verbose)

    # Validate inputs
    if query is None:
        # If no query cache is provided, find the latest query cache in the cache_dir
        query = find_latest_scenes_cache(Path(cache_dir))
        if not query:
            raise typer.BadParameter(f"No scenes.parquet found in {cache_dir}")
        logger.info(f"Using most recent query cache: {query}")

    if parallel_scenes < 1:
        raise typer.BadParameter("--parallel-scenes must be at least 1")
    if parallel_bands < 1:
        raise typer.BadParameter("--parallel-bands must be at least 1")
    if mission.upper() == "S2" and res not in {10, 20, 60}:
        raise typer.BadParameter("--res must be 10, 20, or 60 for Sentinel-2")

    results = resolve_and_download(
        scenes_cache=query,
        mission=mission,
        bands=[b.strip() for b in bands.replace(",", " ").split()],
        resolution=res,
        output_dir=outdir,
        config_file=str(config),
        parallel_scenes=parallel_scenes,
        parallel_bands=parallel_bands,
        logger=logger,
    )
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
