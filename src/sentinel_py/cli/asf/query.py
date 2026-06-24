import typer

app = typer.Typer()


@app.command()
def query():
    """Query ASF data."""
    typer.echo("Querying ASF data...")
