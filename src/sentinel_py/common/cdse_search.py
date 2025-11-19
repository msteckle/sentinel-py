import requests
import pandas as pd
from shapely.geometry.base import BaseGeometry
import logging

def build_search_query(
    aoi: BaseGeometry,
    catalogue_odata: str,
    collection_name: str,
    product_type: str,
    start_iso: str,
    end_iso: str,
) -> str:
    """
    To query Copernicus Data Space Ecosystem (CDSE), build an 
    Open Data Protocol (OData) search query URL for the given parameters.
    """
    aoi_wkt = aoi.wkt
    filter_expr = (
        f"Collection/Name eq '{collection_name}' "
        f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
        f"and att/OData.CSC.StringAttribute/Value eq '{product_type}') "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt}') "
        f"and ContentDate/Start gt {start_iso} and ContentDate/Start lt {end_iso}"
    )
    return f"{catalogue_odata}/Products?$top=100&$filter={filter_expr}"


def fetch_all_products(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Follow CDSE OData pagination and collect all products into a DataFrame.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    sess = session or requests.Session()

    items: list[dict] = []

    while url:
        logger.debug("Fetching page: %s", url)
        r = sess.get(url, timeout=timeout)
        r.raise_for_status()
        j = r.json()

        items.extend(j.get("value", []))
        url = j.get("@odata.nextLink")

    if not items:
        return pd.DataFrame()

    return pd.DataFrame(items)