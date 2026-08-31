"""Shared display and confirmation helpers for download commands."""

from __future__ import annotations

import typer

from sentinel_py.download.preflight import DownloadStorageSummary


def format_storage_size(size: int) -> str:
    """Format bytes in decimal and binary units suitable for storage planning."""
    size = max(0, int(size))
    if size >= 1_000_000_000_000:
        return f"{size / 1e12:.2f} TB ({size / 2**40:.2f} TiB)"
    if size >= 1_000_000_000:
        return f"{size / 1e9:.2f} GB ({size / 2**30:.2f} GiB)"
    if size >= 1_000_000:
        return f"{size / 1e6:.2f} MB ({size / 2**20:.2f} MiB)"
    if size >= 1_000:
        return f"{size / 1e3:.2f} kB ({size / 2**10:.2f} KiB)"
    return f"{size} B"


def echo_storage_summary(summary: DownloadStorageSummary) -> None:
    """Print the storage implications of a download plan."""
    typer.echo(f"Resolved assets: {summary.asset_count:,}")
    typer.echo("Known dataset size: " + format_storage_size(summary.known_total_bytes))
    typer.echo(
        "Known additional storage needed: "
        + format_storage_size(summary.known_additional_bytes)
    )
    typer.echo(f"Assets without a reported size: {summary.unknown_size_assets:,}")


def confirm_download(*, assume_yes: bool) -> None:
    """Require standard interactive confirmation unless --yes was supplied."""
    if assume_yes:
        return
    if not typer.confirm("Continue with download?", default=False):
        typer.echo("Download cancelled.")
        raise typer.Exit()
