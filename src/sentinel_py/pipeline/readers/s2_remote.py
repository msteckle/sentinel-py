"""Remote Sentinel-2 L2A granules: Earth Search STAC items read as in-place COGs.

An ``s2.l2a.remote`` source resolves granules from the Earth Search catalog instead of
scanning a SAFE archive. The resulting ``S2Granule`` records carry COG URLs, the
footprint and the radiometry from the STAC item, so the lazy readers and processors
treat them exactly like local granules.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from shapely import wkt

from sentinel_py.cache import DEFAULT_PIPELINE_CACHE_DIR, QueryCache, deterministic_cache_key
from sentinel_py.enums import S2_SATURATED_DN
from sentinel_py.pipeline.grid import AOIPlan, AOIUnit
from sentinel_py.pipeline.readers.s2 import S2Granule, S2Radiometry, _acquisition_is_selected

# Cached Earth Search queries. They are derived memoization, so they live beside the
# pipeline's other derived caches and can be deleted at any time.
STAC_CACHE_DIR = DEFAULT_PIPELINE_CACHE_DIR / "s2-stac"
STAC_QUERY_CACHE_VERSION = 1
DEFAULT_QUANTIFICATION_VALUE = 10_000
DEFAULT_NODATA_DN = 0


########################################################################################
# Scene table
########################################################################################


def earth_search_scenes(
    plan: AOIPlan,
    *,
    collection: str,
    years: tuple[int, ...] | None,
    speriod: tuple[int, int],
    eperiod: tuple[int, int],
    cache_dir: Path = STAC_CACHE_DIR,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Return the Earth Search scenes covering every unit of an AOI plan.

    The query result is cached by its parameters, so repeated runs (and compute nodes
    sharing the cache directory) do not search again.
    """
    from sentinel_py.download.earth_search import EARTH_SEARCH_URL, query_earth_search

    logger = logger or logging.getLogger(__name__)
    geometries = [unit.geometry_wgs84 for unit in plan.units]
    payload = {
        "provider": "EarthSearch",
        "catalog": EARTH_SEARCH_URL,
        "collection": str(collection),
        "geometries_wkt": [geometry.wkt for geometry in geometries],
        "years": years,
        "speriod": speriod,
        "eperiod": eperiod,
        "cache_version": STAC_QUERY_CACHE_VERSION,
    }
    query_cache = QueryCache.for_payload(cache_dir, payload, "scenes.parquet")
    if query_cache.exists():
        scenes = query_cache.load()
        logger.info("Loaded cached Earth Search query: %s", query_cache.table_path)
        return scenes
    scenes = query_earth_search(
        geometries, collection=collection, years=years, speriod=speriod,
        eperiod=eperiod, logger=logger,
    )
    query_cache.save(
        scenes,
        {**payload, "created": dt.datetime.now().isoformat(), "num_scenes": len(scenes)},
    )
    logger.info("Cached Earth Search query: %s", query_cache.table_path)
    return scenes


########################################################################################
# Granule records
########################################################################################


def _dn_offset(record: dict[str, Any]) -> int:
    """Return the signed DN offset of an asset (STAC ``offset`` divided by ``scale``)."""
    scale, offset = record.get("scale"), record.get("offset")
    return round(offset / scale) if scale and offset else 0


def granule_from_row(row: Any, bands: tuple[str, ...], collection: str) -> S2Granule:
    """Build the granule record of one scene-table row for the requested bands."""
    assets = json.loads(row.assets)
    spectral = {band: record for band, record in assets.items() if band != "SCL"}
    scale = next((r["scale"] for r in spectral.values() if r.get("scale")), None)
    nodata = next(
        (r["nodata"] for r in spectral.values() if r.get("nodata") is not None),
        DEFAULT_NODATA_DN,
    )
    used = sorted(
        (band, record["href"])
        for band, record in assets.items()
        if band in bands or band == "SCL"
    )
    acquired = dt.datetime.fromisoformat(row.datetime).strftime("%Y%m%dT%H%M%S")
    return S2Granule(
        product_id=row.product_id,
        granule_id=row.item_id,
        tile_id=row.tile_id,
        acquisition_time=acquired,
        safe_dir=None,
        product_metadata=None,
        band_paths={band: spectral[band]["href"] for band in bands if band in spectral},
        scl_path=assets.get("SCL", {}).get("href"),
        footprint=wkt.loads(row.geometry_wkt),
        radiometry=S2Radiometry(
            offsets={band: _dn_offset(record) for band, record in spectral.items()},
            special_values=(int(nodata), S2_SATURATED_DN),
            quantification_value=round(1 / scale) if scale else DEFAULT_QUANTIFICATION_VALUE,
        ),
        fingerprint=hashlib.sha256(
            json.dumps(
                [str(collection), row.item_id, row.processing_baseline, used],
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
    )


########################################################################################
# Granule finder
########################################################################################


class RemoteGranuleFinder:
    """Lists the remote granules of one source for an AOI unit, querying only once.

    Instances are called like the local finder: with a unit they return the granules
    whose footprint intersects it, with ``None`` every granule of the plan.
    """

    def __init__(
        self,
        plan: AOIPlan,
        *,
        bands: tuple[str, ...],
        collection: str,
        years: tuple[int, ...] | None,
        speriod: tuple[int, int],
        eperiod: tuple[int, int],
        cache_dir: Path = STAC_CACHE_DIR,
        logger: logging.Logger | None = None,
    ) -> None:
        self._plan = plan
        self._bands = bands
        self._collection = collection
        self._years = years
        self._speriod = speriod
        self._eperiod = eperiod
        self._cache_dir = cache_dir
        self._logger = logger
        self._granules: list[S2Granule] | None = None

    def _load(self) -> list[S2Granule]:
        """Query (or load) the scenes once and keep those within the season."""
        if self._granules is None:
            scenes = earth_search_scenes(
                self._plan, collection=self._collection, years=self._years,
                speriod=self._speriod, eperiod=self._eperiod,
                cache_dir=self._cache_dir, logger=self._logger,
            )
            granules = [
                granule_from_row(row, self._bands, self._collection)
                for row in scenes.itertuples()
            ]
            self._granules = [
                granule
                for granule in granules
                if _acquisition_is_selected(
                    granule.acquisition_time, self._years, self._speriod, self._eperiod
                )
            ]
        return self._granules

    def __call__(self, unit: AOIUnit | None = None) -> list[S2Granule]:
        """Return the granules intersecting ``unit``, or all of them for ``None``."""
        granules = self._load()
        if unit is None:
            return list(granules)
        return [
            granule for granule in granules if granule.footprint.intersects(unit.geometry_wgs84)
        ]

    @property
    def fingerprint(self) -> str:
        """Hash of the identity of every granule, for point-sample cache keys."""
        return deterministic_cache_key(
            {"granules": [granule.fingerprint for granule in self._load()]}
        )
