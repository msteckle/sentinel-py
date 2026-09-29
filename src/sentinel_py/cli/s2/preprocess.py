"""CLI for lazy Sentinel-2 Level-2A preprocessing."""

from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from click import ClickException
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from sentinel_py.enums import S2Bands, S2Res
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger
from sentinel_py.s2.base import S2Granule, S2PreprocessConfig, S2PreprocessResult

app = typer.Typer()

DEFAULT_PREPROCESS_BANDS = "B02 B03 B04 B05 B06 B07 B08 B8A B11 B12"
DEFAULT_PREPROCESS_MASK_CLASSES = "0 1 3 8 9 10 11"


def _parse_bands(value: str) -> tuple[str, ...]:
    """Parse and validate a comma- or space-separated spectral band list."""
    bands = tuple(
        dict.fromkeys(item.upper() for item in value.replace(",", " ").split())
    )
    if not bands:
        raise typer.BadParameter("--bands must contain at least one spectral band")
    supported = tuple(band.value for band in S2Bands)
    unsupported = [band for band in bands if band not in supported]
    if unsupported:
        raise typer.BadParameter(
            "Unsupported --bands value(s): "
            f"{', '.join(unsupported)}. Options: {', '.join(supported)}"
        )
    return bands


def _parse_mask_classes(value: str) -> tuple[int, ...]:
    """Parse and validate a comma- or space-separated SCL class list."""
    try:
        classes = tuple(
            dict.fromkeys(int(item) for item in value.replace(",", " ").split())
        )
    except ValueError as error:
        raise typer.BadParameter(
            "--mask-classes must contain integers from 0 through 11"
        ) from error
    unsupported = [number for number in classes if number < 0 or number > 11]
    if unsupported:
        raise typer.BadParameter(
            "Unsupported SCL class value(s): "
            f"{', '.join(map(str, unsupported))}. Options: 0 through 11"
        )
    return tuple(sorted(classes))


def _parse_years(value: str | None) -> tuple[int, ...] | None:
    """Parse an optional comma- or space-separated acquisition year list."""
    if value is None:
        return None
    try:
        years = tuple(sorted({int(item) for item in value.replace(",", " ").split()}))
    except ValueError as error:
        raise typer.BadParameter("--years must contain four-digit years") from error
    if not years:
        raise typer.BadParameter("--years must contain at least one year")
    if any(year < 2015 or year > 9999 for year in years):
        raise typer.BadParameter("--years values must be between 2015 and 9999")
    return years


def _mpi_context():
    """Return an MPI communicator only when the command has multiple ranks."""
    mpi_environment_size = max(
        (
            int(os.environ.get(name, "1"))
            for name in ("OMPI_COMM_WORLD_SIZE", "PMI_SIZE", "PMIX_SIZE")
            if os.environ.get(name, "1").isdigit()
        ),
        default=1,
    )
    if mpi_environment_size == 1:
        return None, 0, 1
    try:
        from mpi4py import MPI
    except ImportError as error:
        if mpi_environment_size > 1:
            raise RuntimeError(
                "This command was launched with MPI, but mpi4py is not installed. "
                "Install the sentinel-py hpc dependencies."
            ) from error
        return None, 0, 1
    communicator = MPI.COMM_WORLD
    size = communicator.Get_size()
    return (communicator if size > 1 else None), communicator.Get_rank(), size


def _run_local_tasks(
    granules: list[S2Granule],
    config: S2PreprocessConfig,
    output_dir: Path,
    cached_rows: dict[tuple[str, str], dict[str, object]],
    workers: int,
    show_progress: bool,
) -> list[S2PreprocessResult]:
    """Run one MPI rank's tasks serially or with a local process pool."""
    from sentinel_py.s2.preprocess import preprocess_s2_granule

    def cached_row(granule: S2Granule) -> dict[str, object] | None:
        return cached_rows.get((granule.product_id, granule.granule_id))

    # Keep the serial path explicit for debugging and scheduler-managed MPI ranks.
    results: list[S2PreprocessResult] = []
    progress = Progress(
        SpinnerColumn(),
        TextColumn("  Preprocessing Sentinel-2 granules"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        disable=not show_progress,
    )
    with progress:
        progress_task = progress.add_task("preprocess", total=len(granules))
        if workers == 1:
            for granule in granules:
                results.append(
                    preprocess_s2_granule(
                        granule,
                        config,
                        output_dir,
                        cached_row(granule),
                    )
                )
                progress.advance(progress_task)
            return results

        # Submit whole-granule jobs so GDAL work remains isolated and picklable.
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    preprocess_s2_granule,
                    granule,
                    config,
                    output_dir,
                    cached_row(granule),
                )
                for granule in granules
            ]
            for future in as_completed(futures):
                results.append(future.result())
                progress.advance(progress_task)
    return results


@app.command(
    "preprocess",
    help=(
        "Create lazy multiband VRTs that apply L2A BOA offsets and an SCL mask "
        "while preserving each granule's native UTM CRS."
    ),
)
def preprocess(
    indir: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=False,
            help=(
                "Local Sentinel-2 data cache containing downloaded Level-2A SAFE "
                "products."
            ),
            rich_help_panel="Required Arguments",
        ),
    ],
    outdir: Annotated[
        Path,
        typer.Option(
            file_okay=False,
            help="Directory in which preprocessed VRT recipes and state are stored.",
            rich_help_panel="Required Arguments",
        ),
    ],
    bands: Annotated[
        str,
        typer.Option(
            help=(
                "Space- or comma-separated spectral bands. Options: B01-B12 and B8A."
            ),
            rich_help_panel="Preprocessing Options",
        ),
    ] = DEFAULT_PREPROCESS_BANDS,
    aoi: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            help=(
                "Optional AOI file. Only cached granules whose tile footprints "
                "intersect it are processed. CRS metadata is read from the file."
            ),
            rich_help_panel="Selection Options",
        ),
    ] = None,
    years: Annotated[
        str | None,
        typer.Option(
            help=(
                "Optional space- or comma-separated acquisition years. Omit to "
                "consider all years in the data cache."
            ),
            rich_help_panel="Selection Options",
        ),
    ] = None,
    speriod: Annotated[
        datetime,
        typer.Option(
            help="Start month and day of the inclusive seasonal selection window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
            rich_help_panel="Selection Options",
        ),
    ] = datetime(2000, 1, 1, tzinfo=UTC),
    eperiod: Annotated[
        datetime,
        typer.Option(
            help="End month and day of the inclusive seasonal selection window.",
            formats=["%m-%d", "%m/%d", "%m %d", "%b-%d", "%b %d", "%B-%d", "%B %d"],
            rich_help_panel="Selection Options",
        ),
    ] = datetime(2000, 12, 31, tzinfo=UTC),
    res: Annotated[
        S2Res,
        typer.Option(
            help="Preprocessed native-UTM grid resolution in meters: 10, 20, or 60.",
            rich_help_panel="Preprocessing Options",
        ),
    ] = S2Res.r20m,
    mask_classes: Annotated[
        str,
        typer.Option(
            help=(
                "Space- or comma-separated SCL pixel classes to mask. Options: 0 no data, "
                "1 saturated/defective, 2 dark-area pixels, 3 cloud shadows, "
                "4 vegetation, 5 non-vegetated, 6 water, 7 unclassified, "
                "8 medium-probability cloud, 9 high-probability cloud, "
                "10 cirrus, 11 snow/ice."
            ),
            rich_help_panel="Preprocessing Options",
        ),
    ] = DEFAULT_PREPROCESS_MASK_CLASSES,
    workers: Annotated[
        int,
        typer.Option(
            min=1,
            help=(
                "Local worker processes per MPI rank. Use 1 for serial execution or "
                "when the scheduler assigns one CPU to each MPI rank."
            ),
            rich_help_panel="Execution Options",
        ),
    ] = 1,
    log: Annotated[
        Path | None,
        typer.Option(
            help=(f"Log file path. If omitted, logs are saved to {DEFAULT_LOG_DIR}."),
            rich_help_panel="Utils",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            help="Enable verbose logging to the console and log file.",
            rich_help_panel="Utils",
        ),
    ] = False,
):
    """Preprocess Sentinel-2 granules without materializing raster data."""
    from sentinel_py.s2.discover import discover_s2_granules
    from sentinel_py.s2.preprocess import (
        preprocess_recipe_id,
        read_preprocess_state,
        validate_gdal_for_preprocessing,
        write_preprocess_results,
    )

    # Parse and validate user-facing selectors before discovery or worker startup.
    band_values = _parse_bands(bands)
    mask_values = _parse_mask_classes(mask_classes)
    year_values = _parse_years(years)
    start_period = (speriod.month, speriod.day)
    end_period = (eperiod.month, eperiod.day)
    if end_period < start_period:
        raise typer.BadParameter(
            "--eperiod must be on or after --speriod within each year"
        )
    config = S2PreprocessConfig(
        bands=band_values,
        resolution_m=int(res.value),
        mask_classes=mask_values,
    )
    output_dir = Path(outdir).resolve()

    # Detect MPI before opening the rank-zero log shared by preprocessing startup.
    try:
        communicator, rank, mpi_size = _mpi_context()
    except RuntimeError as error:
        raise ClickException(str(error)) from error
    logger = (
        get_logger(name="s2_preprocess_logger", logpath=log, verbose=verbose)
        if rank == 0
        else None
    )

    # Validate GDAL after logging starts so native import failures reach the log file.
    try:
        gdal_version = validate_gdal_for_preprocessing()
    except RuntimeError as error:
        if logger:
            logger.exception("GDAL preprocessing validation failed")
        raise ClickException(str(error)) from error

    # Every rank discovers the same immutable tasks, then works a deterministic shard.
    try:
        granules = discover_s2_granules(
            indir,
            band_values,
            int(res.value),
            aoi=aoi,
            years=year_values,
            speriod=start_period,
            eperiod=end_period,
        )
    except (FileNotFoundError, TypeError, ValueError) as error:
        if logger:
            logger.exception("Sentinel-2 granule selection failed")
        raise ClickException(
            f"Could not select Sentinel-2 granules: {error}"
        ) from error
    if not granules:
        if rank == 0:
            if logger:
                logger.warning("Found 0 Sentinel-2 granules in %s", indir)
            typer.echo(f"Found 0 Sentinel-2 granules in {indir}")
        raise typer.Exit()
    state = read_preprocess_state(output_dir)
    recipe_id = preprocess_recipe_id(config)
    cached_rows: dict[tuple[str, str], dict[str, object]] = {}
    if not state.empty:
        matching = state[state["recipe_id"] == recipe_id]
        cached_rows = {
            (str(row["product_id"]), str(row["granule_id"])): {
                str(key): value for key, value in row.items()
            }
            for row in matching.to_dict("records")
        }
    local_granules = granules[rank::mpi_size]

    # Report the immutable recipe once and run local work on every MPI rank.
    start_clock = time.monotonic()
    started_at = datetime.now().astimezone()
    if rank == 0:
        typer.echo(f"Found {len(granules)} Sentinel-2 granule(s).")
        typer.echo(f"Preprocessing recipe: {recipe_id}")
        typer.echo(f"GDAL: {gdal_version} with native muparser VRT expressions")
        if mpi_size > 1:
            typer.echo(f"Execution: {mpi_size} MPI ranks × {workers} local worker(s)")
        else:
            typer.echo(f"Execution: {workers} local worker(s)")
        logger.info(
            "Preprocessing %d granules with recipe=%s mpi_ranks=%d workers_per_rank=%d",
            len(granules),
            recipe_id,
            mpi_size,
            workers,
        )
    local_results = _run_local_tasks(
        local_granules,
        config,
        output_dir,
        cached_rows,
        workers,
        show_progress=rank == 0,
    )

    # Gather compact result records; only rank zero mutates shared state or prints.
    if communicator is not None:
        gathered = communicator.gather(local_results, root=0)
        if rank != 0:
            return
        if gathered is None:
            raise ClickException("MPI did not return gathered results to rank zero")
        results = [result for rank_results in gathered for result in rank_results]
    else:
        results = local_results
    results.sort(key=lambda result: (result.product_id, result.granule_id))
    state_path = write_preprocess_results(output_dir, state, results)
    ended_at = datetime.now().astimezone()
    elapsed = time.monotonic() - start_clock
    preprocessed_count = sum(result.status == "preprocessed" for result in results)
    skipped_count = sum(result.status == "skipped" for result in results)
    failed = [result for result in results if result.status == "failed"]

    # Match the concise query/download command summaries and log each task failure.
    typer.echo("Summary:")
    typer.echo(f"  Preprocessing started: {started_at:%Y-%m-%d %H:%M:%S}")
    typer.echo(f"  Preprocessing ended:   {ended_at:%Y-%m-%d %H:%M:%S}")
    typer.echo(
        f"  Elapsed time:         {elapsed:.1f} seconds for {len(results)} granules"
    )
    typer.echo(
        f"  Results:              {preprocessed_count} preprocessed, "
        f"{skipped_count} skipped, {len(failed)} failed"
    )
    typer.echo(f"  Preprocessing state:   {state_path}")
    if logger:
        for result in failed:
            logger.error(
                "Failed product=%s granule=%s: %s",
                result.product_id,
                result.granule_id,
                result.error,
            )
    if failed:
        raise typer.Exit(code=1)
