"""``sentinel-py cache list``: show what is cached and how much space it uses."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.cache import DEFAULT_CACHE_DIR, CacheEntry, list_entries
from sentinel_py.cli.options import PANEL_UTILS, CacheRootOpt, DataDirOpt
from sentinel_py.download.preflight import format_storage_size

app = typer.Typer()

TIME_FORMAT = "%Y-%m-%d %H:%M"


def _echo_table(header: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    """Print left-aligned columns sized to their widest cell."""
    widths = [
        max(len(header[column]), *(len(row[column]) for row in rows))
        for column in range(len(header))
    ]
    for line in (header, *rows):
        typer.echo("  ".join(cell.ljust(width) for cell, width in zip(line, widths)))


def _rows_text(entry_rows: int | None) -> str:
    """Format a row count, showing ``?`` for unreadable tables."""
    return "?" if entry_rows is None else f"{entry_rows:,}"


@app.command("list", help="List cached queries, point samples, and stores.")
def list_caches(
    cache_dir: CacheRootOpt = DEFAULT_CACHE_DIR,
    data_dir: DataDirOpt = None,
    entries: Annotated[
        bool,
        typer.Option(
            "--entries",
            help="List every query directory, sample file, and store.",
            rich_help_panel=PANEL_UTILS,
        ),
    ] = False,
) -> None:
    """Print a per-namespace summary of the caches, or each entry with --entries."""
    found = list_entries(cache_dir, data_dir or [])
    if not found:
        typer.echo(f"No cache entries found in {cache_dir}.")
        return

    if entries:
        _echo_table(
            ("Namespace", "Size", "Rows", "Parts", "Last used", "Path", "Description"),
            [
                (
                    entry.namespace,
                    format_storage_size(entry.size_bytes),
                    _rows_text(entry.rows),
                    str(entry.parts),
                    entry.last_used.strftime(TIME_FORMAT),
                    str(entry.path),
                    entry.description,
                )
                for entry in found
            ],
        )
        return

    grouped: dict[str, list[CacheEntry]] = defaultdict(list)
    for entry in found:
        grouped[entry.namespace].append(entry)
    summary: list[tuple[str, ...]] = []
    for namespace, items in grouped.items():
        known_rows = [item.rows for item in items if item.rows is not None]
        summary.append(
            (
                namespace,
                items[0].kind.value,
                str(len(items)),
                format_storage_size(sum(item.size_bytes for item in items)),
                _rows_text(sum(known_rows)) if known_rows else "?",
                str(sum(item.parts for item in items)),
                max(item.last_used for item in items).strftime(TIME_FORMAT),
            )
        )
    _echo_table(
        ("Namespace", "Kind", "Entries", "Size", "Rows", "Parts", "Last used"), summary
    )
    typer.echo(f"Cache root: {Path(cache_dir)}")
