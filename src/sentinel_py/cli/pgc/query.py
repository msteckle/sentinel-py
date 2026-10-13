"""CLI command for querying ArcticDEM through the PGC STAC API."""

import datetime as dt
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from sentinel_py.cache import (
    DEFAULT_PGC_CACHE_DIR,
    cache_directory,
    deterministic_cache_key,
    mark_cache_used,
    write_json_atomic,
    write_parquet_atomic,
)
from sentinel_py.cli.options import (
    LogOpt,
    VerboseOpt,
    cache_dir_option,
    load_aoi_geometries,
)
from sentinel_py.download.base import make_progress
from sentinel_py.download.pgc import (
    PGC_CATALOG_URL,
    PGC_COLLECTION,
    query_arcticdem,
)
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
    payload = {
        "provider": "PGC",
        "catalog": PGC_CATALOG_URL,
        "collection": PGC_COLLECTION,
        "geometries_wkt": geometry_wkts,
        "spatial_query_strategy": "component_bbox_exact_v2",
        "max_results": max_results,
        "cache_version": 1,
    }
    query_dir = cache_directory(cache_dir, deterministic_cache_key(payload))
    manifest_path = query_dir / "manifest.parquet"
    info_path = query_dir / "query_info.json"
    if manifest_path.exists():
        manifest = pd.read_parquet(manifest_path)
        mark_cache_used(manifest_path)
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
        write_parquet_atomic(manifest, manifest_path, index=False)
        write_json_atomic(
            info_path,
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
