"""Registry of named cache namespaces: where each cache lives and how it behaves."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sentinel_py.cache.base import (
    DEFAULT_ASF_CACHE_DIR,
    DEFAULT_CDSE_CACHE_DIR,
    DEFAULT_PGC_CACHE_DIR,
    DEFAULT_PIPELINE_CACHE_DIR,
)
from sentinel_py.cache.rowstore import RowStore
from sentinel_py.enums import CacheKind, CacheLocation

SIDECAR_DIR = ".sentinel-py"


@dataclass(frozen=True)
class CacheNamespace:
    """One registered cache: its location, invalidation kind, and row identity.

    Attributes
    ----------
    name : str
        Registry key.
    kind : CacheKind
        How entries become invalid; see :class:`sentinel_py.enums.CacheKind`.
    location : CacheLocation
        Whether the cache lives in the home cache, a data directory sidecar, or an
        output directory sidecar.
    relpath : str
        Path relative to the namespace root.
    description : str
        One-line purpose, used by documentation and future cache tooling.
    key_columns : tuple[str, ...]
        Row identity for keyed-row tables; empty for non-tabular caches.
    version : int
        Schema/semantics version of the stored rows.
    default_root : pathlib.Path | None
        Root used when the caller does not supply one (home caches only).
    """

    name: str
    kind: CacheKind
    location: CacheLocation
    relpath: str
    description: str
    key_columns: tuple[str, ...] = ()
    version: int = 1
    default_root: Path | None = None

    def path(self, root: Path | None = None) -> Path:
        """Return the file or directory for this namespace under ``root``."""
        base = Path(root) if root is not None else self.default_root
        if base is None:
            raise ValueError(f"Cache namespace {self.name!r} requires an explicit root")
        return base / self.relpath

    def store(
        self, root: Path | None = None, *, read_only: bool = False
    ) -> RowStore:
        """Open this keyed-row namespace as a :class:`RowStore`."""
        if not self.key_columns:
            raise ValueError(f"Cache namespace {self.name!r} is not a keyed-row table")
        return RowStore(
            self.path(root),
            list(self.key_columns),
            version=self.version,
            read_only=read_only,
        )


_ENTRIES = (
    CacheNamespace(
        "cdse_assets",
        CacheKind.immutable,
        CacheLocation.home,
        "assets.parquet",
        "Remote CDSE S3 asset listings per product, plus local download status.",
        key_columns=("safedir", "img_path_in_safedir"),
        default_root=DEFAULT_CDSE_CACHE_DIR,
    ),
    CacheNamespace(
        "cdse_scene_catalog",
        CacheKind.mutable,
        CacheLocation.home,
        "catalog.parquet",
        "Deduplicated spatial and temporal catalog of every queried CDSE scene.",
        key_columns=("Name",),
        default_root=DEFAULT_CDSE_CACHE_DIR,
    ),
    CacheNamespace(
        "cdse_downloads",
        CacheKind.mutable,
        CacheLocation.output_dir,
        f"{SIDECAR_DIR}/cdse_downloads.parquet",
        "Completed CDSE downloads recorded beside the output directory.",
        key_columns=("safedir", "img_path_in_safedir"),
    ),
    CacheNamespace(
        "asf_downloads",
        CacheKind.mutable,
        CacheLocation.output_dir,
        f"{SIDECAR_DIR}/asf_downloads.parquet",
        "ASF download state recorded beside the output directory.",
        key_columns=("url",),
    ),
    CacheNamespace(
        "pgc_downloads",
        CacheKind.mutable,
        CacheLocation.output_dir,
        f"{SIDECAR_DIR}/pgc_downloads.parquet",
        "PGC download state recorded beside the output directory.",
        key_columns=("url",),
    ),
    CacheNamespace(
        "s1_orbits",
        CacheKind.mutable,
        CacheLocation.data_dir,
        f"{SIDECAR_DIR}/s1_orbits.parquet",
        "Staged Sentinel-1 orbit-file assignments per scene.",
        key_columns=("scene",),
    ),
    CacheNamespace(
        "s1_catalog",
        CacheKind.stat_validated,
        CacheLocation.data_dir,
        f"{SIDECAR_DIR}/s1_catalog.parquet",
        "Manifest-derived catalog of local Sentinel-1 ZIP archives.",
        key_columns=("scene",),
    ),
    CacheNamespace(
        "cog_outputs",
        CacheKind.derived,
        CacheLocation.output_dir,
        f"{SIDECAR_DIR}/cog_outputs.parquet",
        "Resumable state of written COG outputs keyed by recipe fingerprint.",
        key_columns=("task_id",),
    ),
    CacheNamespace(
        "point_samples",
        CacheKind.derived,
        CacheLocation.home,
        "point-samples",
        "Cached per-reader point sample tables keyed by sampling recipe.",
        default_root=DEFAULT_PIPELINE_CACHE_DIR,
    ),
)

REGISTRY: dict[str, CacheNamespace] = {entry.name: entry for entry in _ENTRIES}

# Provider roots that hold keyed query directories rather than named files.
QUERY_ROOTS: dict[str, Path] = {
    "asf": DEFAULT_ASF_CACHE_DIR,
    "cdse": DEFAULT_CDSE_CACHE_DIR,
    "pgc": DEFAULT_PGC_CACHE_DIR,
}
