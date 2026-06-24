import typer

from .download import app as download_app
from .query import app as query_app

app = typer.Typer()

app.add_typer(download_app)
app.add_typer(query_app)
