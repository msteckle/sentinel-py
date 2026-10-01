# Processing pipeline architecture

## Scope of this foundation

The pipeline is a directed acyclic graph (DAG) of semantic processing nodes. YAML
describes intent and dependencies; registered processors validate their own settings
and execute nodes. Sentinel-2 preprocessing returns a lazy xarray Dataset backed by
Dask on the configured canonical grid.

## Configuration layers

Pipeline configuration has two layers:

1. Generic fields describe the graph: node `id`, processor `type`, named `inputs`, sources, execution settings, and named outputs.
2. Every other node field belongs to the registered processor and is validated by that processor's Pydantic model with unknown fields rejected.

An input mapping names the role an upstream artifact plays. The mapping itself is a graph edge:

```yaml
nodes:
  - id: source_node
    type: example.source
  - id: derived_node
    type: example.derived
    inputs:
      data: source_node

outputs:
  result:
    from: derived_node
    path: results
    format: example
```

Node declaration order has no execution meaning. Loading a pipeline rejects duplicate node IDs, missing input or output references, and dependency cycles, then records a stable topological order. Output names are user-facing materialization names. The legacy `node:` output reference remains accepted, while new YAML uses `from:`.

## Processor contract and registry

Each processor registers one unique type name and provides:

- a Pydantic configuration model;
- cross-reference validation for sources, inputs, outputs, and global grid settings;
- an `execute` method receiving named upstream artifacts and a processor context;
- one returned artifact that downstream input mappings can reference.

The registry rejects duplicate processor type names. Pipeline loading fails before execution when a YAML node names an unregistered processor.

The processor context carries the complete validated pipeline, the current node, the named outputs attached to that node, and the run logger. Execution services should be added to this context rather than imported into generic graph code as the architecture grows.

## Sentinel-2 processor boundary

`s2.preprocess` is the first built-in processor. Its Pydantic model owns `source`,
`bands`, `mask_classes`, and `nodata`. It:

- accepts no upstream node inputs;
- requires an AOI-derived grid in a projected metre CRS;
- reserves UInt16 value 65535 as output nodata;
- accepts named `cog` outputs for materialization and `xarray` outputs for an
  explicitly unmaterialized artifact;
- returns an `xarray.Dataset` immediately without reading raster pixels or writing an
  output path;
- leaves its Dask graph available to downstream processors and output writers.

## Canonical output grid

A canonical grid uses an explicit CRS and derives its extent from `selection.aoi`:

```yaml
selection:
  aoi: ../data/aois/toolik_025_aoi.geojson

output_grid:
  crs: EPSG:3338
  resolution: 20
  extent: aoi
  anchor: [0, 0]
  chunks:
    y: 1024
    x: 1024
```

`resolution` is expressed in the output CRS units. A scalar requests square pixels;
`[x, y]` requests distinct positive resolutions. `anchor` is the `[x, y]` origin of
the shared pixel lattice in those same units. It defaults to `[0, 0]`.

The planner transforms the AOI into the requested CRS, then snaps its minimum bounds
down and maximum bounds up relative to the anchor. The resulting `GridSpec` contains
the normalized CRS, positive x/y resolution, north-up affine transform, aligned
bounds, raster width and height, and Dask chunk shape. Its window iterator covers the
grid in deterministic row-major order and reduces edge windows without changing pixel
alignment.

Chunks replace the earlier idea of separately overlaid processing gridcells. They are
array execution partitions, not independent geospatial products. Two AOIs using the
same CRS, resolution, and anchor therefore share pixel boundaries even when their
extents and chunk edges differ.

`s2.preprocess` requires this canonical `GridSpec`; native-grid configuration is not
accepted by the processor.

## Lazy Sentinel-2 source adapter

The canonical adapter constructs one Dask task for every scene and spatial
`GridWindow`. A task opens all requested spectral assets and SCL together through
Rasterio, reads only the padded native windows needed by that output chunk, and
reprojects in memory. It never creates an intermediate raster.

Within each task, processing order is explicit:

1. Decode native-resolution source pixels without selecting a downsampled JP2
   overview.
2. Mark both XML-declared `NODATA` and `SATURATED` DN values invalid.
3. Add the signed per-band BOA offset and clamp valid negative results to zero.
4. Resample and align each band to the exact canonical window; use average for
   downsampling, nearest-neighbor at unchanged resolution, and bilinear for
   upsampling. Align SCL with nearest-neighbor.
5. Apply configured SCL classes, invalid SCL values, and radiometric nodata to every
   band.

The returned Dataset has dimensions `(time, band, y, x)`. `reflectance` contains
corrected DN as `uint16` with the configured nodata value; `scl` is `uint8`.
Coordinates include acquisition time, band names, pixel centers, product and granule
IDs, and the per-scene quantification value. CRS, affine transform, bounds, and nodata
are dataset attributes. SAFE discovery and XML metadata parsing happen while planning;
Rasterio import and all raster pixel work remain inside Dask tasks.

## Output writers and unified execution

Output writers have their own registry and contract; they are not processors. After
all processors have built their artifacts, the runner asks each named output's writer
for delayed tasks. It combines every requested output into one Dask graph and submits
that graph once. Identically keyed upstream scene/chunk reads are therefore shared
between variables and between multiple outputs.

Execution uses Dask for every configured backend:

- `local` starts a Dask `LocalCluster` with isolated worker processes;
- `dask` submits the combined graph to `scheduler_address`;
- `slurm` creates a `dask-jobqueue` cluster and submits the same graph.

There is no separate process-pool path. Workers do not open or append a shared log.
They return structured result objects; the runner logs those results and updates state
centrally after graph completion. Native raster calls are serialized between threads
inside each worker process because some JP2/OpenJPEG and libtiff paths are not safe
under concurrent access in one process. Parallel raster work occurs across worker
processes instead.

The `cog` writer materializes one bounded COG per scene, variable, and canonical
spatial chunk. Reflectance tiles are multiband UInt16 COGs and SCL tiles are Byte
COGs. Files use 256-pixel internal tiling, DEFLATE compression, a horizontal
predictor, and automatic overviews. This chunk-level layout keeps worker memory
bounded and allows failed work to resume independently. Paths follow:

```text
OUTPUT/PRODUCT_ID/GRANULE_ID/reflectance.rRRRR.cCCCC.tif
OUTPUT/PRODUCT_ID/GRANULE_ID/scl.rRRRR.cCCCC.tif
```

Each worker writes a unique temporary sibling with Rasterio and atomically replaces
the final COG only after it closes successfully. The central writer stores cache keys
and structured results in `OUTPUT/.sentinel-py/cog_outputs.parquet`. Cache keys
include the processor recipe, source file fingerprint, variable, and chunk identity.
A rerun skips valid files and recomputes missing or stale tiles.

## Intended next boundaries

Future work should preserve these separations:

- canonical output-grid planning is independent of raster reads;
- processors transform artifacts and do not choose cluster infrastructure;
- output writers are separate from processors;
- the YAML DAG remains small and semantic;
- Dask supplies the lower-level chunk execution graph;
- intermediate persistence is explicit rather than automatic;
- output writers are the components that request Dask computation.

Zarr and STAC are not required by this design. They can be added later as isolated
source or output integrations if an evidenced use case warrants them.
