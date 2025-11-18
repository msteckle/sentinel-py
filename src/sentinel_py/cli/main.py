import typer
from pathlib import Path
from typing import Optional, List
from sentinel_py.s2.workflows.download_s2 import download_s2_seasonal_scenes
from sentinel_py.common.aoi import create_aoi_geojson, overlay_latlon_grid

app = typer.Typer(help="Sentinel data and workflow CLI.")
s2 = typer.Typer(help="Sentinel-2 tools.")
app.add_typer(s2, name="s2")


@app.command("create-aoi")
def create_aoi(
    xmin: float = typer.Option(..., help="Minimum longitude"),
    xmax: float = typer.Option(..., help="Maximum longitude"),
    ymin: float = typer.Option(..., help="Minimum latitude"),
    ymax: float = typer.Option(..., help="Maximum latitude"),
    crs: str = typer.Option("EPSG:4326", help="CRS for AOI and grid"),
    out_file: Path = typer.Option("latlon_aoi.geojson", help="Output .geojson file"),
):
    create_aoi_geojson(
        xmin, 
        xmax, 
        ymin, 
        ymax, 
        crs, 
        out_file
    )


@app.command("create-latlon-grid")
def create_latlon_grid(
    aoi_file: Path = typer.Option(..., exists=True, help="AOI .geojson file"),
    dx_deg: float = typer.Option(..., help="Grid cell size in degrees (longitude)"),
    dy_deg: float = typer.Option(..., help="Grid cell size in degrees (latitude)"),
    crs: str = typer.Option("EPSG:4326", help="CRS for AOI and grid"),
    clip_to_aoi: bool = typer.Option(True, help="Clip grid cells to AOI"),
    fill_aoi_holes: bool = typer.Option(True, help="Fill holes in AOI geometry"),
    fill_cell_holes: bool = typer.Option(True, help="Fill holes in grid cells"),
    out_file: Path = typer.Option("latlon_grid.geojson", help="Output .geojson file"),
):
    overlay_latlon_grid(
        aoi=aoi_file,
        cell_size_deg=(dx_deg, dy_deg),
        crs=crs,
        clip_to_aoi=clip_to_aoi,
        fill_aoi_holes=fill_aoi_holes,
        fill_cell_holes=fill_cell_holes,
        out_file=out_file,
    )


@s2.command("download-seasonally")
def download_seasonally(
    aoi: Path = typer.Option(..., exists=True, help="AOI file"),
    output: Path = typer.Option(..., help="Output directory"),
    start_year: int = typer.Option(...),
    start_month: int = 6,
    start_day: int = 1,
    end_year: int = typer.Option(...),
    end_month: int = 8,
    end_day: int = 31,
    catalogue_odata: str = "https://cdse-catalogue.dataspace.copernicus.eu/odata/v1/Products",
    collection_name: str = "Sentinel-2 MSI Level-2A",
    product_type: str = "S2MSI2A",
    bands: List[str] = typer.Option(
        ["B02","B03","B04","B05","B06","B07","B08","B8A","B11","B12"],
        help="Bands to include.",
    ),
    target_res_m: int = 20,
    credentials: Optional[str] = None,
    max_scenes: Optional[int] = None,
    max_workers_files: int = 4,
    log_file: Optional[str] = None,
):
    download_s2_seasonal_scenes(
        aoi=aoi,
        output_root=output,
        start_year=start_year,
        start_month=start_month,
        start_day=start_day,
        end_year=end_year,
        end_month=end_month,
        end_day=end_day,
        catalogue_odata=catalogue_odata,
        collection_name=collection_name,
        product_type=product_type,
        bands=bands,
        target_res_m=target_res_m,
        credentials=credentials,
        max_scenes=max_scenes,
        max_workers_files=max_workers_files,
        log_file=log_file,
    )


if __name__ == "__main__":
    app()
