import requests
import pandas as pd
from shapely.geometry.base import BaseGeometry

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
    return f"{catalogue_odata}/Products?$filter={filter_expr}"


def fetch_all_products(
    base_query: str,
    top: int = 200,
    timeout: int = 60,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """
    Fetch all products from an OData endpoint, handling pagination.
    """
    sess = session or requests.Session()
    url = f"{base_query}&$count=true&$top={top}"
    items: list[dict] = []

    while url:
        r = sess.get(url, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        items.extend(j.get("value", []))
        url = j.get("@odata.nextLink")

    return pd.DataFrame(items)