"""Central CLI entry point of subcommands"""

import typer

from .asf import app as asf_app
from .cdse import app as cdse_app
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
app.command(
    "run",
    help="Run a validated processing pipeline from a YAML file.",
)(run)


if __name__ == "__main__":
    app()
