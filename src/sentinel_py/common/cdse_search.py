import requests
import pandas as pd
from shapely.geometry.base import BaseGeometry
import logging
import geopandas as gpd

def batch_geometries(
    geoseries: gpd.GeoSeries, 
    max_url_len: int = 6000
) -> list[BaseGeometry]:
    """
    Split a GeoSeries into batches whose unioned WKT fits within max_url_len.
    Handles any geometry type (points, polygons, etc.).
    """

    from shapely.ops import unary_union

    batches = []
    current_batch = []
    for geom in geoseries:
        current_batch.append(geom)
        wkt = unary_union(current_batch).wkt
        if len(wkt) > max_url_len:
            if len(current_batch) > 1:
                batches.append(unary_union(current_batch[:-1]))
                current_batch = [geom]
            else:
                # single geometry already exceeds limit — add it anyway
                batches.append(geom)
                current_batch = []
    if current_batch:
        batches.append(unary_union(current_batch))
    return batches


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
    return f"{catalogue_odata}/Products?$top=1000&$filter={filter_expr}"


def all_query_results(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Follow CDSE OData pagination and collect all products into a DataFrame.
    """
    sess = session or requests.Session()

    items: list[dict] = []
    while url:
        logger.debug(f"Fetching page: {url}")
        r = sess.get(url, timeout=timeout)
        r.raise_for_status()
        j = r.json()

        items.extend(j.get("value", []))
        url = j.get("@odata.nextLink")

    if not items:
        return pd.DataFrame()

    return pd.DataFrame(items)