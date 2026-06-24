import typer

app = typer.Typer()


@app.command()
def download():
    """Download ASF data."""
    typer.echo("Downloading ASF data...")
