import typer

from .asf import app as asf_app
from .cdse import app as cdse_app
from .s2 import app as s2_app
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
app.add_typer(s2_app, name="s2", help="Sentinel-2 processing and analysis tools.")


if __name__ == "__main__":
    app()
