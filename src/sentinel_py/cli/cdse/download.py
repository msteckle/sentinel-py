from pathlib import Path
from typing import Annotated, Optional

import typer

from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()


@app.command(
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
        config_file=str(config),
        parallel_scenes=parallel_scenes,
        parallel_bands=parallel_bands,
        logger=logger,
    )
