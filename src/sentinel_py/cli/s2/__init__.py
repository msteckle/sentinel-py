import typer

from .preprocess import app as preprocess_app

app = typer.Typer()

app.add_typer(preprocess_app)
