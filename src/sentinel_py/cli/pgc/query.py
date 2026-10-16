"""CLI command for querying ArcticDEM through the PGC STAC API."""

import datetime as dt
from pathlib import Path
from typing import Annotated

import typer

from sentinel_py.cache import DEFAULT_PGC_CACHE_DIR, QueryCache
from sentinel_py.cli.options import (
    LogOpt,
    VerboseOpt,
    cache_dir_option,
    load_aoi_geometries,
)
from sentinel_py.download.base import make_progress
from sentinel_py.download.pgc import arcticdem_query_payload, query_arcticdem
from sentinel_py.log import get_logger

app = typer.Typer()


@app.command(help="Query PGC for ArcticDEM v4.1 10m mosaic tiles.")
def query(
    aoi: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="AOI file used to select intersecting ArcticDEM tiles.",
            rich_help_panel="Required Arguments",
        ),
    ],
    max_results: Annotated[
        int | None,
        typer.Option(
            min=1,
            help="Optional maximum number of tiles returned by the STAC query.",
            rich_help_panel="Optional Query Configurations",
        ),
    ] = None,
    cache_dir: cache_dir_option("PGC", DEFAULT_PGC_CACHE_DIR) = DEFAULT_PGC_CACHE_DIR,
    crs: Annotated[
        str,
        typer.Option(
            help="CRS to assume when the AOI has no CRS metadata.",
            rich_help_panel="Utils",
        ),
    ] = "EPSG:4326",
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Query and cache ArcticDEM tile metadata for an AOI."""
    logger = get_logger(name="pgc_query_logger", logpath=log, verbose=verbose)
    geometries = load_aoi_geometries(aoi, crs)
    geometry_wkts = [geometry.wkt for geometry in geometries]
    payload = arcticdem_query_payload(geometry_wkts, max_results)
    query_cache = QueryCache.for_payload(cache_dir, payload, "manifest.parquet")
    manifest_path = query_cache.table_path
    if query_cache.exists():
        manifest = query_cache.load()
        typer.echo(f"Loaded cached PGC query: {manifest_path}")
    else:
        with make_progress(eta=False) as progress:
            task_id = progress.add_task(
                "Querying PGC STAC components", total=len(geometries)
            )
            try:
                manifest = query_arcticdem(
                    geometries,
                    max_results=max_results,
                    logger=logger,
                    progress_callback=lambda completed, total: progress.update(
                        task_id, completed=completed
                    ),
                )
            except (RuntimeError, ValueError) as error:
                raise typer.BadParameter(str(error)) from error
        query_cache.save(
            manifest,
            {
                **payload,
                "aoi": str(aoi),
                "crs": crs,
                "created": dt.datetime.now().isoformat(),
                "num_tiles": len(manifest),
            },
        )
        typer.echo(f"Cached PGC query: {manifest_path}")
    logger.info("PGC query complete: %d unique tiles", len(manifest))
    typer.echo(f"Found {len(manifest)} unique ArcticDEM tile(s).")
