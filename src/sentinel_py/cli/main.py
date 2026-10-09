"""Central CLI entry point of subcommands"""

from pathlib import Path
from typing import Annotated

import typer

from .asf import app as asf_app
from .cdse import app as cdse_app
from .pgc import app as pgc_app
from .run import run
from .utils import app as utils_app

app = typer.Typer(
    help="Sentinel 1 & 2 download and processing workflow CLI.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)

app.add_typer(utils_app, help="Utility commands for various tasks.")
app.add_typer(
    asf_app, name="asf", help="Commands for querying and downloading from ASF."
)
app.add_typer(
    cdse_app, name="cdse", help="Commands for querying and downloading from CDSE."
)
app.add_typer(
    pgc_app, name="pgc", help="Commands for querying and downloading ArcticDEM from PGC."
)
app.command(
    "run",
    help="Run a validated processing pipeline from a YAML file.",
)(run)


@app.command("create-aoi", hidden=True)
def create_aoi(
    xmin: Annotated[float, typer.Option()],
    xmax: Annotated[float, typer.Option()],
    ymin: Annotated[float, typer.Option()],
    ymax: Annotated[float, typer.Option()],
    out_file: Annotated[Path, typer.Option(dir_okay=False)],
) -> None:
    """Write an AOI bounding box using the historic command spelling."""
    if xmin >= xmax:
        typer.echo("xmin must be less than xmax", err=True)
        raise typer.Exit(code=1)
    if ymin >= ymax:
        typer.echo("ymin must be less than ymax", err=True)
        raise typer.Exit(code=1)
    from sentinel_py.aoi import bbox_to_geojson

    bbox_to_geojson((xmin, ymin, xmax, ymax), crs="EPSG:4326", output=out_file)


if __name__ == "__main__":
    app()
