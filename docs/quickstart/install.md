# Installation

Querying and downloading Sentinel data only requires the base Python environment.
Sentinel-2 raster processing additionally requires the `processing` extra:

```bash
uv sync --extra processing
```

Rasterio supplies the production raster reader, reprojection operations, and COG
writer; no separate system-level raster build is required.
