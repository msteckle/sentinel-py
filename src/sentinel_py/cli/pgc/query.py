"""CLI command for querying ArcticDEM through the PGC STAC API."""

import datetime as dt
import json
from pathlib import Path
from typing import Annotated

import geopandas as gpd
import pandas as pd
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

from sentinel_py.cache import (
    DEFAULT_PGC_CACHE_DIR,
    cache_directory,
    deterministic_cache_key,
    mark_cache_used,
    write_json_atomic,
    write_parquet_atomic,
)
from sentinel_py.download.pgc import (
    PGC_CATALOG_URL,
    PGC_COLLECTION,
    atomic_aoi_geometries,
    query_arcticdem,
)
from sentinel_py.log import DEFAULT_LOG_DIR, get_logger

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
    cache_dir: Annotated[
        Path,
        typer.Option(
            file_okay=False,
            help=f"PGC query cache root. Defaults to {DEFAULT_PGC_CACHE_DIR}.",
            rich_help_panel="Utils",
        ),
    ] = DEFAULT_PGC_CACHE_DIR,
    crs: Annotated[
        str,
        typer.Option(
            help="CRS to assume when the AOI has no CRS metadata.",
            rich_help_panel="Utils",
        ),
    ] = "EPSG:4326",
    log: Annotated[
        Path | None,
        typer.Option(
            help=f"Log file path. Defaults to {DEFAULT_LOG_DIR}.",
            rich_help_panel="Utils",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(help="Enable verbose logging.", rich_help_panel="Utils"),
    ] = False,
) -> None:
    """Query and cache ArcticDEM tile metadata for an AOI."""
    logger = get_logger(name="pgc_query_logger", logpath=log, verbose=verbose)
    aoi_gdf = gpd.read_file(aoi)
    if aoi_gdf.empty:
        raise typer.BadParameter(f"AOI contains no features: {aoi}")
    if aoi_gdf.crs is None:
        aoi_gdf = aoi_gdf.set_crs(crs)
    geometries = atomic_aoi_geometries(aoi_gdf.to_crs("EPSG:4326").geometry)
    if not geometries:
        raise typer.BadParameter(f"AOI contains no non-empty geometries: {aoi}")
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
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Querying PGC STAC components"),
        ) as progress:
            task_id = progress.add_task("query", total=len(geometries))
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
