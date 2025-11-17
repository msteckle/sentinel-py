import typer
from pathlib import Path
from typing import Optional, List
from sentinel_py.s2.workflows.download_s2 import download_s2_seasonal_scenes

app = typer.Typer(help="Sentinel data and workflow CLI.")
s2 = typer.Typer(help="Sentinel-2 tools.")
app.add_typer(s2, name="s2")


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
