"""CLI entry point for declarative processing pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from click import ClickException

from sentinel_py.log import DEFAULT_LOG_DIR, get_logger


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
            help="Validate and resolve the YAML without importing GDAL or executing tasks.",
        ),
    ] = False,
    log: Annotated[
        Path | None,
        typer.Option(help=f"Log file path. Defaults to {DEFAULT_LOG_DIR}."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(help="Enable verbose console and file logging."),
    ] = False,
) -> None:
    """Run a validated processing pipeline from YAML."""
    from sentinel_py.pipeline import PipelineConfigError, load_pipeline

    try:
        config = load_pipeline(pipeline)
    except PipelineConfigError as error:
        raise ClickException(f"Invalid pipeline: {error}") from error

    typer.echo(f"Pipeline:      {config.path}")
    typer.echo(f"Execution:     {config.execution.method}")
    for source in config.sources.values():
        typer.echo(f"Source:        {source.source_id} ({source.data_dir})")
    if config.output_grid.grid is None:
        typer.echo(
            f"Output grid:   native, {config.output_grid.resolution_m} m "
            "(legacy compatibility mode)"
        )
    else:
        grid = config.output_grid.grid
        typer.echo(
            f"Output grid:   {grid.crs}, {grid.resolution[0]} × "
            f"{grid.resolution[1]} units, {grid.width} × {grid.height} pixels"
        )
        typer.echo(
            f"Chunks:        {grid.chunks.x} × {grid.chunks.y} pixels "
            f"({grid.chunk_grid_shape[1]} × {grid.chunk_grid_shape[0]} chunks)"
        )
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

    logger = get_logger(name="pipeline_logger", logpath=log, verbose=verbose)
    try:
        from sentinel_py.pipeline.run import run_pipeline

        result = run_pipeline(config, logger)
    except (FileNotFoundError, RuntimeError, TypeError, ValueError) as error:
        logger.exception("Pipeline execution failed")
        raise ClickException(f"Pipeline execution failed: {error}") from error

    preprocessed = sum(item.status == "preprocessed" for item in result.results)
    skipped = sum(item.status == "skipped" for item in result.results)
    failed = sum(item.status == "failed" for item in result.results)
    typer.echo("Summary:")
    typer.echo(f"  Started:     {result.started_at:%Y-%m-%d %H:%M:%S}")
    typer.echo(f"  Ended:       {result.ended_at:%Y-%m-%d %H:%M:%S}")
    typer.echo(f"  Elapsed:     {result.elapsed_seconds:.1f} seconds")
    typer.echo(f"  Recipe:      {result.recipe_id}")
    typer.echo(
        f"  Results:     {preprocessed} preprocessed, {skipped} skipped, {failed} failed"
    )
    typer.echo(f"  State:       {result.state_path}")
    if failed:
        raise typer.Exit(code=1)
