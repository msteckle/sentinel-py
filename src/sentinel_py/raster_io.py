"""Process-local synchronization for native raster library calls."""

from __future__ import annotations

from threading import RLock

# Some GDAL raster drivers use mutable process-global or dataset-adjacent state.
# Different Dask worker processes may run raster work concurrently, while threads
# within one worker serialize native reads, reprojection, and writes through this lock.
RASTERIO_LOCK = RLock()
