"""``sentinel-py cache compact``: fold checkpoint parts into single store files."""

from __future__ import annotations

from typing import Annotated

import typer

from sentinel_py.cache import DEFAULT_CACHE_DIR, compact_stores, list_entries
from sentinel_py.cli.options import PANEL_UTILS, CacheRootOpt, DataDirOpt
from sentinel_py.log import get_logger

app = typer.Typer()


@app.command(
    "compact",
    help="Fold uncompacted checkpoint parts of cache stores into their base files.",
)
def compact(
    cache_dir: CacheRootOpt = DEFAULT_CACHE_DIR,
    data_dir: DataDirOpt = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be compacted without changing any file.",
            rich_help_panel=PANEL_UTILS,
        ),
    ] = False,
) -> None:
    """Compact every store that has parts; interrupted runs leave parts behind."""
    found = list_entries(cache_dir, data_dir or [])
    pending = [entry for entry in found if entry.parts]
    if not pending:
        typer.echo("Nothing to compact: no store has uncompacted parts.")
        return

    logger = get_logger(name="cache_compact_logger", logpath=None, verbose=False)
    try:
        results = compact_stores(pending, dry_run=dry_run, logger=logger)
    except OSError as error:
        typer.echo(f"Compaction failed: {error}", err=True)
        raise typer.Exit(code=1) from error

    verb = "Would compact" if dry_run else "Compacted"
    for result in results:
        typer.echo(f"{verb} {result.parts} part(s): {result.path}")
