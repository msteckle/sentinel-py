import click
from sentinel_py.s2.workflows.download_s2 import download_s2_seasonal_scenes

@click.group()
def cli():
    """Sentinel data and workflow CLI."""
    pass

@cli.group()
def s2():
    """Sentinel-2 tools."""
    pass

@s2.command("download-seasonally")
@click.option("--aoi", type=click.Path(exists=True), required=True)
@click.option("--output", type=click.Path(), required=True)
@click.option("--start-year", type=int, required=True)
@click.option("--start-month", type=int, default=6)
@click.option("--start-day", type=int, default=1)
@click.option("--end-year", type=int, required=True)
@click.option("--end-month", type=int, default=8)
@click.option("--end-day", type=int, default=31)
@click.option("--catalogue-odata", type=str, default="https://cdse-catalogue.dataspace.copernicus.eu/odata/v1/Products")
@click.option("--collection-name", type=str, default="Sentinel-2 MSI Level-2A")
@click.option("--product-type", type=str, default="S2MSI2A")
@click.option("--bands", type=str, multiple=True, default=["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"])
@click.option("--target-res-m", type=int, default=20)
@click.option("--credentials", type=str, default=None)
@click.option("--max-scenes", type=int, default=None)
@click.option("--max-workers-files", type=int, default=4)
@click.option("--log-file", default=None)
def download_seasonally(
    aoi,
    output,
    start_year,
    start_month,
    start_day,
    end_year,
    end_month,
    end_day,
    catalogue_odata,
    collection_name,
    product_type,
    bands,
    target_res_m,
    credentials,
    max_scenes,
    max_workers_files,
    log_file
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
