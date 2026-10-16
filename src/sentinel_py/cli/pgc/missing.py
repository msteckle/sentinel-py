"""CLI command listing the ArcticDEM areas a pipeline still needs."""

from pathlib import Path
from typing import Annotated

import geopandas as gpd
import typer
from shapely.ops import unary_union

from sentinel_py.cli.options import PANEL_REQUIRED, PANEL_UTILS, LogOpt, VerboseOpt
from sentinel_py.log import get_logger
from sentinel_py.pipeline import PipelineConfig
from sentinel_py.pipeline.config import PipelineConfigError
from sentinel_py.pipeline.dem_coverage import NODE_TYPE, find_dem_gaps

app = typer.Typer()

# Equal-area CRS used only to report gap sizes in square kilometres.
AREA_CRS = "EPSG:6933"
SHOWN_SCENES = 10


def _area_km2(geometry) -> float:
    """Return the area of an EPSG:4326 geometry in square kilometres."""
    series = gpd.GeoSeries([geometry], crs="EPSG:4326").to_crs(AREA_CRS)
    return float(series.area.iloc[0]) / 1e6


@app.command(
    help=(
        "List the ArcticDEM areas a pipeline's snap_gpt nodes need but the local "
        "tiles do not cover, and write them as an AOI for 'pgc query'."
    )
)
def missing(
    pipeline: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="Pipeline YAML whose snap_gpt nodes are checked.",
            rich_help_panel=PANEL_REQUIRED,
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            dir_okay=False,
            help=(
                "GeoJSON written with the missing areas (EPSG:4326). Defaults to "
                "<pipeline name>_missing_dem.geojson beside the pipeline."
            ),
            rich_help_panel=PANEL_UTILS,
        ),
    ] = None,
    margin: Annotated[
        bool,
        typer.Option(
            "--margin/--no-margin",
            help=(
                "Also include areas needed only for the DEM margin around each "
                "processing region. Without it only areas that block processing "
                "are reported."
            ),
            rich_help_panel=PANEL_UTILS,
        ),
    ] = True,
    log: LogOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Report DEM coverage gaps and write them as an AOI; exit 1 if any exist."""
    logger = get_logger(name="pgc_missing_logger", logpath=log, verbose=verbose)
    try:
        config = PipelineConfig.from_file(pipeline)
    except PipelineConfigError as error:
        raise typer.BadParameter(f"Invalid pipeline {pipeline}: {error}") from error
    nodes = [node.node_id for node in config.ordered_nodes if node.node_type == NODE_TYPE]
    if not nodes:
        typer.echo(f"{pipeline} has no {NODE_TYPE} nodes; there is no DEM to check.")
        return

    gaps = find_dem_gaps(config, nodes, include_margin=margin)
    logger.info("DEM coverage check: nodes=%s gaps=%d margin=%s", nodes, len(gaps), margin)
    if not gaps:
        typer.echo("All processing regions are covered by the local DEM tiles.")
        return

    required = unary_union([gap.required for gap in gaps])
    margin_only = unary_union([gap.margin for gap in gaps]).difference(required)
    features = [
        {
            "kind": kind,
            "scenes": len({gap.scene for gap in gaps if not getattr(gap, kind).is_empty}),
            "area_km2": round(_area_km2(geometry), 3),
            "geometry": geometry,
        }
        for kind, geometry in (("required", required), ("margin", margin_only))
        if not geometry.is_empty
    ]
    destination = output or pipeline.with_name(f"{pipeline.stem}_missing_dem.geojson")
    destination.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(features, geometry="geometry", crs="EPSG:4326").to_file(
        destination, driver="GeoJSON"
    )

    typer.echo(f"DEM coverage gaps: {len(gaps)} scene/AOI combination(s)")
    for feature in features:
        label = (
            "blocks processing" if feature["kind"] == "required" else "DEM margin only"
        )
        typer.echo(
            f"  {feature['kind']:<8} {feature['area_km2']:>10.1f} km2 over "
            f"{feature['scenes']} scene(s) ({label})"
        )
    for gap in gaps[:SHOWN_SCENES]:
        kind = "required" if not gap.required.is_empty else "margin"
        typer.echo(f"  {gap.scene}  AOI {gap.aoi_id}  ({kind})")
    if len(gaps) > SHOWN_SCENES:
        typer.echo(f"  ... and {len(gaps) - SHOWN_SCENES} more")
    typer.echo(f"Missing areas written to {destination}")
    typer.echo("Fetch the tiles with:")
    typer.echo(f"  sentinel-py pgc query --aoi {destination}")
    typer.echo(f"  sentinel-py pgc download --outdir {gaps[0].dem_dir}")
    raise typer.Exit(code=1)
