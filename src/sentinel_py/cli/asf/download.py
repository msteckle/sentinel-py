from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from sentinel_py.cache import DEFAULT_ASF_CACHE_DIR, find_latest_cache_file
from sentinel_py.download.asf import (
    _product_filename,
    download_asf,
    earthdata_netrc_credentials,
)
from sentinel_py.download.preflight import (
    DownloadStorageSummary,
    confirm_download,
    echo_storage_summary,
)
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

app = typer.Typer()

EARTHDATA_CONFIG_EXAMPLE = """machine urs.earthdata.nasa.gov
    login <YOUR_EARTHDATA_USERNAME>
    password <YOUR_EARTHDATA_PASSWORD>"""


def _earthdata_config_error(config: Path) -> typer.BadParameter:
    return typer.BadParameter(
        f"Invalid Earthdata credentials file: {config}\n\n"
        "Create a netrc-format file with:\n\n"
        f"{EARTHDATA_CONFIG_EXAMPLE}\n\n"
        f"Then protect it with:\nchmod 600 {config}\n\n"
        "Create an Earthdata account at https://urs.earthdata.nasa.gov/users/new"
    )


def _validate_earthdata_config(config: Path) -> None:
    if earthdata_netrc_credentials(config) is None:
        raise _earthdata_config_error(config)


def _storage_summary(products: pd.DataFrame, outdir: Path) -> DownloadStorageSummary:
    """Calculate known final and additional bytes for an ASF manifest."""
    known_total = 0
    known_additional = 0
    unknown = 0

    for product in products.to_dict("records"):
        expected = product.get("expected_size")
        if expected is None or pd.isna(expected):
            size_mb = product.get("sizeMB")
            expected = (
                round(float(size_mb) * 1024 * 1024)
                if size_mb is not None and not pd.isna(size_mb)
                else None
            )
        expected = int(expected) if expected is not None else None
        if expected is None or expected <= 0:
            unknown += 1
            continue

        known_total += expected
        local_path = outdir / _product_filename(product)
        try:
            already_complete = (
                local_path.is_file() and local_path.stat().st_size == expected
            )
        except OSError:
            already_complete = False
        if not already_complete:
            known_additional += expected

    return DownloadStorageSummary(
        asset_count=len(products),
        known_total_bytes=known_total,
        known_additional_bytes=known_additional,
        unknown_size_assets=unknown,
    )


@app.command(help="Download products from an explicit or cached ASF query manifest.")
def download(
    outdir: Annotated[
        Path,
        typer.Option(
            help="Directory in which downloaded products will be stored.",
            rich_help_panel="Required Arguments",
        ),
    ],
    config: Annotated[
        Path,
        typer.Option(
            help=(
                "Path to a netrc-format file containing Earthdata Login credentials."
            ),
            exists=True,
            dir_okay=False,
            rich_help_panel="Required Arguments",
        ),
    ],
    query: Annotated[
        Path | None,
        typer.Option(
            help="Explicit ASF manifest.parquet instead of the latest cached query.",
            exists=True,
            dir_okay=False,
            rich_help_panel="Optional Download Configurations",
        ),
    ] = None,
    processes: Annotated[
        int,
        typer.Option(
            help="Number of parallel ASF downloads.",
            min=1,
            max=8,
            rich_help_panel="Optional Download Configurations",
        ),
    ] = 4,
    retries: Annotated[
        int,
        typer.Option(
            help="Retry attempts per product after a transient download failure.",
            min=0,
            rich_help_panel="Optional Download Configurations",
        ),
    ] = 3,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip the interactive download confirmation.",
            rich_help_panel="Optional Download Configurations",
        ),
    ] = False,
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(f"ASF query cache root. Defaults to {DEFAULT_ASF_CACHE_DIR}."),
            file_okay=False,
            rich_help_panel="Utils",
        ),
    ] = DEFAULT_ASF_CACHE_DIR,
    log: Annotated[
        Path | None,
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
    # Validate inputs
    _validate_earthdata_config(config)
    logger = get_logger(name="asf_download_logger", logpath=log, verbose=verbose)

    manifest = query
    if manifest is None:
        manifest = find_latest_cache_file(cache_dir, "manifest.parquet")
        if manifest is None:
            raise typer.BadParameter(
                f"No cached ASF query manifest found in {cache_dir}. "
                "Run 'sentinel-py asf query' before downloading."
            )
        logger.info("Using most recent cached ASF query: %s", manifest)
    else:
        logger.info("Using explicit ASF query manifest: %s", manifest)
    typer.echo(f"Cached query: {manifest}")
    products = pd.read_parquet(manifest)
    if "url" not in products.columns:
        raise typer.BadParameter(
            f"ASF query manifest is missing the required 'url' column: {manifest}"
        )
    products = (
        products.dropna(subset=["url"])
        .drop_duplicates(subset=["url"])
        .reset_index(drop=True)
    )
    if products.empty:
        logger.info("Manifest contains no ASF URLs; nothing to download: %s", manifest)
        typer.echo("Found 0 scenes: 0 ZIP files")
        raise typer.Exit()

    zip_label = "ZIP file" if len(products) == 1 else "ZIP files"
    typer.echo(f"Found {len(products)} scenes: {len(products)} {zip_label}")
    storage = _storage_summary(products, outdir)
    echo_storage_summary(storage)
    logger.info(
        "ASF download preflight: manifest=%s assets=%d known_total_bytes=%d "
        "known_additional_bytes=%d unknown_size_assets=%d",
        manifest,
        storage.asset_count,
        storage.known_total_bytes,
        storage.known_additional_bytes,
        storage.unknown_size_assets,
    )
    confirm_download(assume_yes=yes)

    # Create output directory and download products
    outdir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Downloading %d unique ASF products to %s with %d processes and %d retries",
        len(products),
        outdir,
        processes,
        retries,
    )
    summary = download_asf(
        products=products,
        out_dir=outdir,
        config_file=config,
        processes=processes,
        retries=retries,
        logger=logger,
    )
    typer.echo("Summary:")
    for message in summary:
        typer.echo(f"  {message}")
    if summary.failed:
        typer.echo(
            f"Download completed with {summary.failed} failed ASF product(s).",
            err=True,
        )
        raise typer.Exit(code=1)
