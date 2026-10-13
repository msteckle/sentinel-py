"""``sentinel-py cache`` command group."""

import typer

from .compact import app as compact_app
from .listing import app as listing_app
from .prune import app as prune_app

app = typer.Typer(no_args_is_help=True)

app.add_typer(listing_app)
app.add_typer(compact_app)
app.add_typer(prune_app)
