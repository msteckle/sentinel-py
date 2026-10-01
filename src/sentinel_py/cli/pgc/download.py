"""CLI command for downloading cached ArcticDEM manifests."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from sentinel_py.cache import DEFAULT_PGC_CACHE_DIR, find_latest_cache_file
from sentinel_py.download.pgc import download_arcticdem
from sentinel_py.download.preflight import (
    DownloadStorageSummary,
    confirm_download,
    echo_storage_summary,
)
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()


def _storage_summary(products: pd.DataFrame, outdir: Path) -> DownloadStorageSummary:
    """Calculate known storage requirements for an ArcticDEM manifest."""
    known_total = 0
    known_additional = 0
    unknown = 0
    for product in products.to_dict("records"):
        expected = product.get("expected_size")
        if expected is None or pd.isna(expected):
            unknown += 1
            continue
        expected = int(expected)
        known_total += expected
        target = outdir / str(product.get("filename") or Path(str(product["url"])).name)
        if not (target.is_file() and target.stat().st_size == expected):
            known_additional += expected
    return DownloadStorageSummary(
        asset_count=len(products),
        known_total_bytes=known_total,
        known_additional_bytes=known_additional,
        unknown_size_assets=unknown,
    )


@app.command(help="Download ArcticDEM tiles from an explicit or cached PGC query.")
def download(
    outdir: Annotated[
        Path,
        typer.Option(
            help="Directory in which DEM GeoTIFFs will be stored.",
            rich_help_panel="Required Arguments",
        ),
    ],
    query: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="Explicit PGC manifest.parquet instead of the latest cached query.",
            rich_help_panel="Optional Download Configurations",
        ),
    ] = None,
    processes: Annotated[
        int,
        typer.Option(min=1, max=16, help="Number of parallel downloads."),
    ] = 4,
    retries: Annotated[
        int,
        typer.Option(min=0, help="Retry attempts after transient failures."),
    ] = 3,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip download confirmation."),
    ] = False,
    cache_dir: Annotated[
        Path,
        typer.Option(
            file_okay=False,
            help=f"PGC query cache root. Defaults to {DEFAULT_PGC_CACHE_DIR}.",
        ),
    ] = DEFAULT_PGC_CACHE_DIR,
    log: Annotated[
        Path | None,
        typer.Option(help=f"Log file path. Defaults to {DEFAULT_LOG_DIR}."),
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging.")] = False,
) -> None:
    """Download DEM assets from a cached PGC query manifest."""
    manifest_path = query or find_latest_cache_file(cache_dir, "manifest.parquet")
    if manifest_path is None:
        raise typer.BadParameter(
            f"No cached PGC query manifest found in {cache_dir}. Run 'sentinel-py pgc query'."
        )
    manifest = pd.read_parquet(manifest_path)
    if "url" not in manifest.columns:
        raise typer.BadParameter(
            f"PGC manifest is missing the required url column: {manifest_path}"
        )
    manifest = (
        manifest.dropna(subset=["url"]).drop_duplicates("url").reset_index(drop=True)
    )
    if manifest.empty:
        typer.echo("Found 0 ArcticDEM tiles.")
        raise typer.Exit()
    logger = get_logger(name="pgc_download_logger", logpath=log, verbose=verbose)
    typer.echo(f"Cached query: {manifest_path}")
    typer.echo(f"Found {len(manifest)} ArcticDEM tile(s).")
    storage = _storage_summary(manifest, outdir)
    echo_storage_summary(storage)
    confirm_download(assume_yes=yes, storage=storage)
    summary = download_arcticdem(
        manifest,
        outdir,
        processes=processes,
        retries=retries,
        logger=logger,
    )
    typer.echo("Summary:")
    for message in summary:
        typer.echo(f"  {message}")
    if summary.failed:
        raise typer.Exit(code=1)
