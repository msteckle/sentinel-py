"""CLI command for downloading ASF Sentinel-1 products and staging orbit files."""

from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.cache import DEFAULT_ASF_CACHE_DIR
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
from sentinel_py.download.asf import (
    download_asf,
    earthdata_netrc_credentials,
    expected_product_size,
    product_filename,
)
from sentinel_py.download.preflight import (
    confirm_download,
    echo_storage_summary,
    manifest_storage_summary,
)
from sentinel_py.download.s1_orbits import stage_orbits
from sentinel_py.enums import S1OrbitPolicy
from sentinel_py.log import get_logger

app = typer.Typer()

EARTHDATA_CONFIG_EXAMPLE = """machine urs.earthdata.nasa.gov
    login <YOUR_EARTHDATA_USERNAME>
    password <YOUR_EARTHDATA_PASSWORD>"""


def _earthdata_config_error(config: Path) -> typer.BadParameter:
    """Build the setup guidance shown for a missing or invalid Earthdata file."""
    return typer.BadParameter(
        f"Invalid Earthdata credentials file: {config}\n\n"
        "Create a netrc-format file with:\n\n"
        f"{EARTHDATA_CONFIG_EXAMPLE}\n\n"
        f"Then protect it with:\nchmod 600 {config}\n\n"
        "Create an Earthdata account at https://urs.earthdata.nasa.gov/users/new"
    )


def _validate_earthdata_config(config: Path) -> None:
    """Raise ``BadParameter`` unless the netrc file holds Earthdata credentials."""
    if earthdata_netrc_credentials(config) is None:
        raise _earthdata_config_error(config)


@app.command(help="Download products from an explicit or cached ASF query manifest.")
def download(
    outdir: Annotated[
        Path,
        typer.Option(
            help="Directory in which downloaded products will be stored.",
            rich_help_panel=PANEL_REQUIRED,
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
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    query: Annotated[
        Path | None,
        typer.Option(
            help="Explicit ASF manifest.parquet instead of the latest cached query.",
            exists=True,
            dir_okay=False,
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = None,
    processes: Annotated[
        int,
        typer.Option(
            help="Number of parallel ASF downloads.",
            min=1,
            max=8,
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = 4,
    retries: Annotated[
        int,
        typer.Option(
            help="Retry attempts per product after a transient download failure.",
            min=0,
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = 3,
    orbits: Annotated[
        S1OrbitPolicy,
        typer.Option(
            help=(
                "Stage Sentinel-1 orbit files for GRD/SLC scenes in OUTDIR/orbits so "
                "SNAP can run offline: precise, restituted, precise-or-restituted "
                "(precise orbits are published weeks after acquisition, so recent "
                "scenes fall back to restituted and are upgraded on a later rerun), "
                "or none."
            ),
            case_sensitive=False,
            rich_help_panel=PANEL_DOWNLOAD,
        ),
    ] = S1OrbitPolicy.precise_or_restituted,
    yes: YesOpt = False,
    cache_dir: cache_dir_option("ASF", DEFAULT_ASF_CACHE_DIR) = DEFAULT_ASF_CACHE_DIR,
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Download an ASF manifest, then stage Sentinel-1 orbit files for SNAP."""
    # Validate inputs
    _validate_earthdata_config(config)
    logger = get_logger(name="asf_download_logger", logpath=log, verbose=verbose)

    manifest, products = load_manifest(
        query, cache_dir, provider="ASF", query_command="sentinel-py asf query"
    )
    logger.info(
        "Using %s ASF query manifest: %s",
        "explicit" if query is not None else "most recent cached",
        manifest,
    )
    typer.echo(f"Cached query: {manifest}")
    if products.empty:
        logger.info("Manifest contains no ASF URLs; nothing to download: %s", manifest)
        typer.echo("Found 0 scenes: 0 ZIP files")
        raise typer.Exit()

    zip_label = "ZIP file" if len(products) == 1 else "ZIP files"
    typer.echo(f"Found {len(products)} scenes: {len(products)} {zip_label}")
    storage = manifest_storage_summary(
        products,
        outdir,
        filename_of=product_filename,
        expected_size_of=expected_product_size,
    )
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
    confirm_download(assume_yes=yes, storage=storage)

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
    echo_summary(summary)

    # Orbit files are tiny and public, so stage them in the networked download
    # stage; compute nodes can then run SNAP without network access.
    orbit_failed = 0
    if orbits is not S1OrbitPolicy.none:
        logger.info("Staging Sentinel-1 orbit files with policy %s", orbits.value)
        try:
            orbit_summary = stage_orbits(
                products,
                outdir,
                orbits,
                processes=processes,
                retries=retries,
                logger=logger,
            )
        except Exception as error:  # noqa: BLE001 - reported as a command failure
            logger.exception("Orbit staging failed")
            typer.echo(f"Orbit staging failed: {error}", err=True)
            orbit_failed = 1
        else:
            for message in orbit_summary.messages:
                typer.echo(f"  {message}")
            orbit_failed = orbit_summary.failed
            if orbit_summary.missing:
                typer.echo(
                    f"{len(orbit_summary.missing)} scene(s) have no published "
                    f"{orbits.value} orbit yet; rerun this command later to stage them.",
                    err=True,
                )
    if orbit_failed:
        typer.echo("Orbit staging did not complete for some scenes.", err=True)
    if orbit_failed and not summary.failed:
        raise typer.Exit(code=1)
    if summary.failed:
        typer.echo(
            f"Download completed with {summary.failed} failed ASF product(s).",
            err=True,
        )
        raise typer.Exit(code=1)
