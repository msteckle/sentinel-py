import time
from pathlib import Path

import asf_search as asf
import numpy as np
import pandas as pd


def get_predominant_flightdir(manifest: pd.DataFrame) -> str | None:
    directions = manifest["flightDirection"].dropna().values
    if len(directions) == 0:
        return None
    unique_directions, counts = np.unique(np.asarray(directions), return_counts=True)
    return str(unique_directions[int(np.argmax(counts))])


def query_asf(
    aoi_wkt: str,
    date_start: str,
    date_end: str,
    product_levels: list[str],
    beam_mode: str = "IW",
    flight_direction: str | None = None,
) -> pd.DataFrame:
    """Return manifest pandas dataframe with unique URLs."""

    # use earthdata credentials

    asf.constants.INTERNAL.CMR_TIMEOUT = 60
    lvls = [getattr(asf.PRODUCT_TYPE, p) for p in product_levels]
    results = asf.search(
        platform=asf.PLATFORM.SENTINEL1,
        processingLevel=lvls,
        start=pd.to_datetime(date_start).date(),
        end=pd.to_datetime(date_end).date(),
        beamMode=beam_mode,
        intersectsWith=aoi_wkt,
        flightDirection=flight_direction if flight_direction else None,
    )
    geoj = results.geojson()
    rows = []
    for feat in geoj["features"]:
        prop = feat["properties"]
        rows.append(
            {
                "granule": prop.get("fileName"),
                "url": prop.get("url"),
                "beamMode": prop.get("beamMode"),
                "flightDirection": prop.get("flightDirection"),
                "startTime": prop.get("startTime"),
                "stopTime": prop.get("stopTime"),
            }
        )
    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).sort_values("granule")
    return df


def download_asf(
    urls: list[str],
    out_dir: Path,
    username: str | None = None,
    password: str | None = None,
    token: str | None = None,
    processes: int = 4,
):
    """
    Use ASF's official helpers. Auth via .netrc (preferred), creds, or token.
    Logs start/end timestamps and duration. Returns list of summary strings.
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    session = None
    if token:
        session = asf.ASFSession().auth_with_token(token)
    elif username and password:
        session = asf.ASFSession().auth_with_creds(username, password)

    start = time.time()
    start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))

    asf.download_urls(
        urls=urls, path=str(out_dir), session=session, processes=processes
    )

    end = time.time()
    end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    elapsed = end - start

    msgs = [
        f"Download started: {start_str}",
        f"Download ended:   {end_str}",
        f"Elapsed time:     {elapsed:.1f} seconds for {len(urls)} files",
    ]
    return msgs
