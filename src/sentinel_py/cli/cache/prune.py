"""``sentinel-py cache prune``: delete old, regenerable cache entries."""

from __future__ import annotations

from typing import Annotated

import typer

from sentinel_py.cache import (
    DEFAULT_CACHE_DIR,
    delete_entries,
    list_entries,
    select_prunable,
)
from sentinel_py.cli.options import PANEL_REQUIRED, PANEL_UTILS, CacheRootOpt
from sentinel_py.download.preflight import format_storage_size
from sentinel_py.enums import CachePruneTarget
from sentinel_py.log import get_logger

app = typer.Typer()

TIME_FORMAT = "%Y-%m-%d %H:%M"


@app.command(
    "prune",
    help=(
        "Delete query caches and point samples not used for a number of days. "
        "Asset stores, catalogs, and state beside your data are never deleted."
    ),
)
def prune(
    older_than: Annotated[
        int,
        typer.Option(
            min=0,
            help="Delete entries last used more than this many days ago.",
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    provider: Annotated[
        list[CachePruneTarget] | None,
        typer.Option(
            help="Restrict pruning to this area. May be repeated; default is all.",
            rich_help_panel=PANEL_UTILS,
        ),
    ] = None,
    cache_dir: CacheRootOpt = DEFAULT_CACHE_DIR,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be deleted without deleting anything.",
            rich_help_panel=PANEL_UTILS,
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip the interactive prune confirmation.",
            rich_help_panel=PANEL_UTILS,
        ),
    ] = False,
) -> None:
    """Show the entries that would be pruned, confirm, then delete them."""
    selected = select_prunable(
        list_entries(cache_dir),
        older_than_days=older_than,
        providers=provider or [],
    )
    if not selected:
        typer.echo(f"Nothing to prune: no entries unused for {older_than} day(s).")
        return

    for entry in selected:
        typer.echo(
            f"{entry.namespace}  {format_storage_size(entry.size_bytes)}  "
            f"last used {entry.last_used.strftime(TIME_FORMAT)}  {entry.path}"
        )
    total = sum(entry.size_bytes for entry in selected)
    typer.echo(f"{len(selected)} entr(ies), {format_storage_size(total)} in total.")
    if dry_run:
        typer.echo("Dry run: nothing was deleted.")
        return
    if not yes and not typer.confirm("Continue with prune?", default=False):
        typer.echo("Prune cancelled.")
        return

    logger = get_logger(name="cache_prune_logger", logpath=None, verbose=False)
    result = delete_entries(selected, cache_dir, logger=logger)
    freed = sum(entry.size_bytes for entry in result.deleted)
    typer.echo(
        f"Deleted {len(result.deleted)} entr(ies), freeing {format_storage_size(freed)}."
    )
    for entry, reason in result.failed:
        typer.echo(f"Could not delete {entry.path}: {reason}", err=True)
    if result.failed:
        raise typer.Exit(code=1)
