import typer

from .offset import app as offset_app

app = typer.Typer()

app.add_typer(offset_app)
