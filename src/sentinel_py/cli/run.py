"""CLI entry point for declarative processing pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from click import ClickException
from rich.progress import Progress, TaskID

from sentinel_py.cli.options import LogOpt, VerboseOpt
from sentinel_py.download.base import make_progress
from sentinel_py.enums import WriteStatus
from sentinel_py.log import get_logger


def run(
    pipeline: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Versioned sentinel-py pipeline YAML file.",
        ),
    ],
    validate_only: Annotated[
        bool,
        typer.Option(
            "--validate-only",
            help="Validate and resolve the YAML without raster I/O or task execution.",
        ),
    ] = False,
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Run a validated processing pipeline from YAML."""
    from sentinel_py.pipeline import PipelineConfig, PipelineConfigError

    # Load and validate the pipeline configuration from the provided YAML file
    try:
        config = PipelineConfig.from_file(pipeline)
    except PipelineConfigError as error:
        raise ClickException(f"Invalid pipeline: {error}") from error

    # Display basic pipeline information
    typer.echo(f"Pipeline:      {config.path}")
    typer.echo(f"Execution:     {config.execution.method}")
    for source in config.sources.values():
        typer.echo(f"Source:        {source.source_id} ({source.data_dir})")
    if config.output_grid.chunks is None:
        typer.echo(f"Output grid:   native, {config.output_grid.resolution} m")
    else:
        typer.echo(
            f"Output grid:   {config.output_grid.crs}, {config.output_grid.resolution[0]} × "
            f"{config.output_grid.resolution[1]} units per AOI component"
        )
        if config.aoi_plan is not None:
            typer.echo(f"AOI units:     {len(config.aoi_plan.units)} ({config.aoi_plan.kind})")
    for node in config.ordered_nodes:
        dependencies = ", ".join(node.inputs.values()) or "none"
        typer.echo(
            f"Node:          {node.node_id} ({node.node_type}; inputs: {dependencies})"
        )
    for output in config.outputs.values():
        typer.echo(
            f"Output:        {output.output_id} <- {output.node}: "
            f"{output.path} ({output.format})"
        )
    if validate_only:
        typer.echo("Pipeline is valid. No tasks were executed.")
        return

    # Execute the pipeline with the configured logger
    logger = get_logger(name="pipeline_logger", logpath=log, verbose=verbose)
    progress: Progress | None = None
    progress_task_id: TaskID | None = None
    progress_counts = {"written": 0, "skipped": 0, "failed": 0}

    node_progress: Progress | None = None
    node_tasks: dict[str, TaskID] = {}

    def update_node_progress(label: str, completed: int, total: int) -> None:
        """Show eager node work, such as point sampling, before outputs are planned."""
        nonlocal node_progress
        if node_progress is None:
            node_progress = make_progress()
            node_progress.start()
        if label not in node_tasks:
            node_tasks[label] = node_progress.add_task(label, total=total)
        node_progress.update(node_tasks[label], total=total, completed=completed)

    def stop_node_progress() -> None:
        """Close the node bar; Rich allows only one live display at a time."""
        nonlocal node_progress
        if node_progress is not None:
            node_progress.stop()
            node_progress = None

    def start_progress(total: int) -> None:
        """Start the output progress bar once the total task count is known."""
        nonlocal progress, progress_task_id
        stop_node_progress()
        progress = make_progress(status=True)
        progress.start()
        progress_task_id = progress.add_task(
            "Processing outputs",
            total=total,
            status="written 0 · skipped 0 · failed 0",
        )

    def update_output_progress(result: object) -> None:
        """Count one finished output result in the written/skipped/failed status."""
        if progress is None or progress_task_id is None:
            return
        status = getattr(result, "status", "complete")
        if status in progress_counts:
            progress_counts[status] += 1
        progress.update(
            progress_task_id,
            status=(
                f"written {progress_counts['written']} · "
                f"skipped {progress_counts['skipped']} · "
                f"failed {progress_counts['failed']}"
            ),
        )

    def update_task_progress(completed: int, total: int) -> None:
        """Mirror Dask task completion onto the progress bar."""
        if progress is None or progress_task_id is None:
            return
        progress.update(progress_task_id, total=total, completed=completed)

    try:
        from sentinel_py.pipeline.run import run_pipeline

        result = run_pipeline(
            config,
            logger,
            progress_callback=update_output_progress,
            progress_start_callback=start_progress,
            task_progress_callback=update_task_progress,
            node_progress_callback=update_node_progress,
        )
    except (FileNotFoundError, RuntimeError, TypeError, ValueError) as error:
        logger.exception("Pipeline execution failed")
        raise ClickException(f"Pipeline execution failed: {error}") from error
    finally:
        stop_node_progress()
        if progress is not None:
            progress.stop()

    # Display a summary of the pipeline execution results
    typer.echo("Summary:")
    typer.echo(f"  Started:     {result.started_at:%Y-%m-%d %H:%M:%S}")
    typer.echo(f"  Ended:       {result.ended_at:%Y-%m-%d %H:%M:%S}")
    typer.echo(f"  Elapsed:     {result.elapsed_seconds:.1f} seconds")
    worker_results = [
        worker_result
        for output_result in result.output_results.values()
        for worker_result in output_result.results
    ]
    if worker_results:
        written = sum(item.status == WriteStatus.written for item in worker_results)
        skipped = sum(item.status == WriteStatus.skipped for item in worker_results)
        failed = sum(item.status == WriteStatus.failed for item in worker_results)
        typer.echo(
            f"  COG tiles:   {written} written, {skipped} skipped, {failed} failed"
        )
        if failed:
            raise typer.Exit(code=1)
        return
    lazy_datasets = [
        artifact
        for artifact in result.outputs.values()
        if hasattr(artifact, "data_vars") and hasattr(artifact, "chunks")
    ]
    if lazy_datasets:
        dataset = lazy_datasets[0]
        typer.echo(
            f"  Lazy data:   {dataset.sizes.get('time', 0)} scene(s), "
            f"{dataset.sizes.get('band', 0)} band(s), "
            f"{dataset.sizes.get('x', 0)} × {dataset.sizes.get('y', 0)} pixels"
        )
        typer.echo("  Raster I/O:  deferred (no output writer requested computation)")
        return
