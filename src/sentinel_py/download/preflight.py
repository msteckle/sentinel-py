"""Storage information shared by provider download implementations."""

import math
from collections.abc import Sequence
from dataclasses import dataclass

import typer

# Two-sided 95% normal quantile used for estimate error bars.
_Z_95 = 1.96


@dataclass(frozen=True)
class DownloadStorageSummary:
    """Storage footprint for a download plan, exact or extrapolated from a sample.

    Attributes
    ----------
    asset_count : int
        Number of assets in the plan (extrapolated when ``is_estimate``).
    known_total_bytes : int
        Final dataset size (extrapolated when ``is_estimate``).
    known_additional_bytes : int
        Bytes still needed on disk (extrapolated when ``is_estimate``).
    unknown_size_assets : int
        Assets without a reported size.
    is_estimate : bool, default False
        Whether the totals were extrapolated from a sample of scenes.
    sampled_scenes, total_scenes : int, default 0
        Sample size and population size when ``is_estimate``.
    total_ci95_bytes, additional_ci95_bytes : int, default 0
        Half-width of the 95% confidence interval of the extrapolated totals.
    """

    asset_count: int
    known_total_bytes: int
    known_additional_bytes: int
    unknown_size_assets: int = 0
    is_estimate: bool = False
    sampled_scenes: int = 0
    total_scenes: int = 0
    total_ci95_bytes: int = 0
    additional_ci95_bytes: int = 0


def extrapolate_total(values: Sequence[float], population: int) -> tuple[float, float]:
    """Extrapolate a population total from a simple random sample.

    Parameters
    ----------
    values : Sequence[float]
        Per-unit values of the sampled units.
    population : int
        Number of units in the full population.

    Returns
    -------
    tuple[float, float]
        The estimated population total and the half-width of its 95% confidence
        interval, using a finite-population correction. The half-width is zero
        when the whole population was sampled.
    """
    sample = len(values)
    if sample == 0:
        return 0.0, 0.0
    mean = math.fsum(values) / sample
    total = mean * population
    if sample >= population or sample < 2:
        return total, 0.0
    variance = math.fsum((value - mean) ** 2 for value in values) / (sample - 1)
    standard_error = (
        population
        * math.sqrt(variance / sample)
        * math.sqrt(1.0 - sample / population)
    )
    return total, _Z_95 * standard_error


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
    if summary.is_estimate:
        typer.echo(
            f"Storage estimate from a random sample of {summary.sampled_scenes:,} of "
            f"{summary.total_scenes:,} scenes (95% confidence intervals):"
        )
        typer.echo(f"Estimated assets: ~{summary.asset_count:,}")
        typer.echo(
            "Estimated dataset size: ~"
            + format_storage_size(summary.known_total_bytes)
            + " ± "
            + format_storage_size(summary.total_ci95_bytes)
        )
        typer.echo(
            "Estimated additional storage needed: ~"
            + format_storage_size(summary.known_additional_bytes)
            + " ± "
            + format_storage_size(summary.additional_ci95_bytes)
        )
        typer.echo(f"Sampled assets without a reported size: {summary.unknown_size_assets:,}")
        return
    typer.echo(f"Resolved assets: {summary.asset_count:,}")
    typer.echo("Known dataset size: " + format_storage_size(summary.known_total_bytes))
    typer.echo(
        "Known additional storage needed: "
        + format_storage_size(summary.known_additional_bytes)
    )
    typer.echo(f"Assets without a reported size: {summary.unknown_size_assets:,}")


def confirm_download(
    *, assume_yes: bool, storage: DownloadStorageSummary | None = None
) -> None:
    """Require confirmation unless already-complete storage is known.

    An extrapolated estimate never proves a download is already complete, so it
    always prompts unless ``assume_yes`` is set.
    """
    if assume_yes:
        return
    if (
        storage is not None
        and not storage.is_estimate
        and storage.known_additional_bytes == 0
        and storage.unknown_size_assets == 0
    ):
        return
    if not typer.confirm("Continue with download?", default=False):
        typer.echo("Download cancelled.")
        raise typer.Exit()
