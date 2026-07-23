from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from sentinel_py.cache import DEFAULT_ASF_CACHE_DIR, find_latest_cache_file
from sentinel_py.download.asf import (
    download_asf,
    earthdata_netrc_credentials,
)

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


@app.command(help="Download products from the most recent cached ASF query.")
def download(
    outdir: Annotated[
        Path,
        typer.Option(help="Directory in which downloaded products will be stored."),
    ],
    config: Annotated[
        Path,
        typer.Option(
            help=(
                "Path to a netrc-format file containing Earthdata Login credentials."
            ),
            exists=True,
            dir_okay=False,
        ),
    ],
    cache_dir: Annotated[
        Path,
        typer.Option(
            help=(
                "ASF query cache root. Defaults to the hidden .asf-cache directory "
                "in the current working directory."
            ),
            file_okay=False,
        ),
    ] = DEFAULT_ASF_CACHE_DIR,
    processes: Annotated[
        int,
        typer.Option(help="Number of parallel ASF downloads.", min=1, max=8),
    ] = 4,
    retries: Annotated[
        int,
        typer.Option(
            help="Retry attempts per product after a transient download failure.",
            min=0,
        ),
    ] = 3,
):
    # Validate inputs
    _validate_earthdata_config(config)

    manifest = find_latest_cache_file(cache_dir, "manifest.parquet")
    if manifest is None:
        raise typer.BadParameter(
            f"No cached ASF query manifest found in {cache_dir}. "
            "Run 'sentinel-py asf query' before downloading."
        )
    typer.echo(f"Using most recent cached ASF query: {manifest}")
    products = pd.read_parquet(manifest)
    products = (
        products.dropna(subset=["url"])
        .drop_duplicates(subset=["url"])
        .reset_index(drop=True)
    )
    if products.empty:
        typer.echo("Manifest contains no ASF URLs; nothing to download.")
        raise typer.Exit()

    # Create output directory and download products
    outdir.mkdir(parents=True, exist_ok=True)
    typer.echo(
        f"Downloading {len(products)} unique ASF product(s) to {outdir} "
        f"with {processes} process(es) and up to {retries} retry attempt(s)."
    )
    messages = download_asf(
        products=products,
        out_dir=outdir,
        config_file=config,
        processes=processes,
        retries=retries,
    )
    for message in messages:
        typer.echo(message)
