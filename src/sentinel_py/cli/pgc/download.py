"""CLI command for downloading cached ArcticDEM manifests."""

from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.cache import DEFAULT_PGC_CACHE_DIR
from sentinel_py.cli.options import (
    PANEL_DOWNLOAD,
    PANEL_REQUIRED,
    LogOpt,
    VerboseOpt,
    YesOpt,
    cache_dir_option,
    echo_summary,
    load_manifest,
)
from sentinel_py.download.pgc import download_arcticdem, expected_size, product_filename
from sentinel_py.download.preflight import (
    confirm_download,
    echo_storage_summary,
    manifest_storage_summary,
)
from sentinel_py.log import get_logger

app = typer.Typer()


@app.command(help="Download ArcticDEM tiles from an explicit or cached PGC query.")
def download(
    outdir: Annotated[
        Path,
        typer.Option(
            help="Directory in which DEM GeoTIFFs will be stored.",
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    query: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="Explicit PGC manifest.parquet instead of the latest cached query.",
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = None,
    processes: Annotated[
        int,
        typer.Option(
            min=1,
            max=16,
            help="Number of parallel downloads.",
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = 4,
    retries: Annotated[
        int,
        typer.Option(
            min=0,
            help="Retry attempts after transient failures.",
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = 3,
    yes: YesOpt = False,
    cache_dir: cache_dir_option("PGC", DEFAULT_PGC_CACHE_DIR) = DEFAULT_PGC_CACHE_DIR,
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Download DEM assets from a cached PGC query manifest."""
    manifest_path, manifest = load_manifest(
        query, cache_dir, provider="PGC", query_command="sentinel-py pgc query"
    )
    if manifest.empty:
        typer.echo("Found 0 ArcticDEM tiles.")
        raise typer.Exit()
    logger = get_logger(name="pgc_download_logger", logpath=log, verbose=verbose)
    typer.echo(f"Cached query: {manifest_path}")
    typer.echo(f"Found {len(manifest)} ArcticDEM tile(s).")
    storage = manifest_storage_summary(
        manifest, outdir, filename_of=product_filename, expected_size_of=expected_size
    )
    echo_storage_summary(storage)
    confirm_download(assume_yes=yes, storage=storage)
    summary = download_arcticdem(
        manifest,
        outdir,
        processes=processes,
        retries=retries,
        logger=logger,
    )
    echo_summary(summary)
    if summary.failed:
        raise typer.Exit(code=1)
