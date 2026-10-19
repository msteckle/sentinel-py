# Add processors

Pipeline extensions use parallel contracts: readers acquire source data, processors apply semantic transformations, and writers materialize typed artifacts. A processor inherits from ``BaseProcessor``, declares ``type_name`` and a frozen Pydantic ``config_model``, and implements both ``execute_raster()`` and ``execute_points()``. The base class dispatches from the validated AOI mode, so processors never need geometry-mode ``isinstance`` branches. Raster hooks exchange ``RasterArtifact`` objects containing lazy xarray datasets; point hooks exchange compact ``PointSampleArtifact`` tables.

The best template is ``src/sentinel_py/pipeline/processors/s2_index.py``: a module docstring, a ``PROCESSOR_VERSION`` constant, banner-delimited sections (helpers, the node configuration, then the processor class), and NumPy-style docstrings on every function.

## Checklist for a new processor

1. Create ``src/sentinel_py/pipeline/processors/<name>.py`` with a module docstring, ``from __future__ import annotations``, and a ``PROCESSOR_VERSION`` integer. Bump the version whenever the produced values change, because it feeds the recipe ID and therefore output cache keys.
2. Define a frozen, ``strict=True`` Pydantic node configuration. Because strict mode rejects plain YAML strings for enum fields, use ``Literal[...]`` for string choices on node configurations.
3. Subclass ``BaseProcessor``; set ``type_name``, ``config_model``, and ``version = PROCESSOR_VERSION``. Set ``supported_output_formats`` only when the processor cannot write every format (the default is all of them).
4. Implement ``validate``. Call ``super().validate(...)`` first, which checks output formats; do not re-check formats yourself.
5. Use the shared helpers instead of re-implementing them: ``self.single_input(inputs)`` for exactly-one-upstream processors, ``make_recipe_id(type_name, version, **params)`` for the recipe ID (include ``source_recipe=`` when chained), ``mask_nodata`` for nodata masking, ``grid_for_unit`` for the per-unit grid, and ``RasterArtifact.map_datasets`` to apply a function to every AOI unit.
6. Register the processor in ``register_builtin_processors`` in ``processors/__init__.py``.
7. Add tests that cover the raster path, the point path, validation errors, and that the recipe ID changes with every result-affecting parameter. ``tests/test_repo_conventions.py`` fails if a processor is unregistered or has no ``PROCESSOR_VERSION``.

Below is an example of a custom processor. This processor, named ``AdditionProcessor``, adds a specified value to each pixel of the input data. We will require the data source (defined in the ``YAML`` under ``sources``) and the integer we want to add to each pixel. Thus, we will start by creating our pydantic BaseModel to define and validate the configuration parameters for the processor.

```python
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
    make_recipe_id,
)
from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Any, Mapping

PROCESSOR_VERSION = 1


class AdditionProcessorConfig(BaseModel):
    """Validated parameters belonging specifically to the ``addition`` node."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1)
    value: int = Field(..., description="The value to add per-pixel")

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, value: Any) -> int:
        """Require an integer value."""
        if not isinstance(value, int):
            raise ValueError("The value must be an integer")
        return value
```

Next, we can build the actual processor class. The ``validate`` method ensures that the parameters provided in the ``YAML`` configuration file meet the requirements designed above in ``AdditionProcessorConfig``.

```python
class AdditionProcessor(BaseProcessor):
    """Add a constant to every pixel of one upstream cube."""

    type_name = "addition"
    config_model = AdditionProcessorConfig
    version = PROCESSOR_VERSION

    def validate(
        self,
        config: ProcessorConfig,
        *,
        node_id: str,
        inputs: Mapping[str, str],
        sources: Mapping[str, Any],
        outputs: tuple[Any, ...],
        output_grid: Any,
    ) -> None:
        """Validate the configuration type and the single upstream input."""
        super().validate(
            config,
            node_id=node_id,
            inputs=inputs,
            sources=sources,
            outputs=outputs,
            output_grid=output_grid,
        )
        if not isinstance(config, AdditionProcessorConfig):
            raise TypeError(f"{node_id} has an invalid addition configuration")
        self.single_input(inputs)

    def execute_raster(self, config, inputs, context: ProcessorContext):
        """Return a RasterArtifact without materializing source chunks."""
        source = self.single_input(inputs)
        recipe_id = make_recipe_id(
            self.type_name,
            self.version,
            value=config.value,
            source_recipe=None,
        )
        ...

    def execute_points(self, config, inputs, context: ProcessorContext):
        """Return a PointSampleArtifact with normalized wide value columns."""
        ...
```

Raster nodes build lazy graphs, and their tasks are counted by the output progress bar. A node that does real work while the graph is being built, such as sampling points or running SNAP per scene in `execute_points`, should call `context.report_progress(label, completed, total)` so the `run` command can show a progress bar for that phase instead of staying silent until the outputs are planned. Calls with the same label update one bar, and the callback is a no-op when nothing is listening.

Point sampling in `s2.preprocess` and `dem` runs on the same execution backend as raster outputs (`execution.method` `local`, `dask`, or `slurm`), so point AOIs use every worker rather than one machine's process pool. `s2.preprocess` makes one task per granule and batch of at most `POINTS_PER_TASK` (500) points, so a scene's metadata is resolved once per batch; `dem` makes one task per point. Each call opens its own cluster, like each batch of raster outputs. `snap_gpt` still runs its scene jobs on the client, bounded by `max_concurrent`.

## Adding readers and writers

A reader subclasses ``BaseReader`` in ``pipeline/readers/``, sets ``source_type`` from the ``SourceType`` enum, implements ``parse_config`` (using the public validators ``only_keys``, ``required``, and ``resolve_path`` from ``pipeline/config.py``), and is registered in ``get_default_reader_registry``. A writer subclasses ``BaseOutputWriter`` in ``pipeline/writers/``, sets ``format_name`` from the ``OutputFormat`` enum and ``accepted_artifact_kinds``, implements ``build`` and ``finalize``, and is registered in ``get_default_writer_registry``. Writers that can resume implement ``checkpoint`` so completed work survives an interrupt. All three registries share the generic ``Registry`` in ``pipeline/base.py``.

## Seasonal composites

The built-in ``composite`` processor takes exactly one upstream processing cube and aggregates its ``reflectance`` variable along ``time``. The optional ``period`` uses a positive period such as ``15D``, ``2W``, ``1M``, or ``1Y``; bins are anchored at ``selection.speriod`` and use the same seasonal slots across all selected years. If ``period`` is omitted, it defaults to ``all`` and produces one composite across the complete selected time range. Supported aggregations are ``mean``, ``median``, and ``maximum``. COG products are named ``composite_<start year>-<end year>_<speriod>-<eperiod>_<aggregation>`` and each time bin is stored below a distinct ``bin_<index>_<seasonal start>`` directory.

```yaml
nodes:
  - id: preprocess
    type: s2.preprocess
    source: s2
  - id: summer_maximum
    type: composite
    inputs: {data: preprocess}
    period: 15D
    aggregation: maximum
```

Seasonal bins are anchored at `selection.speriod` and shared by every selected year, so `period: 15D` over 2019–2024 pools all years into the same 15-day slots (a climatology), and NaN or nodata values never enter a mean, median, or maximum; a pixel with no valid sample in a bin is nodata. By default bins with no observations at all are dropped (`empty_bins: drop`), which keeps the bin list identical to earlier releases but means different AOIs can end up with different bins. Set `empty_bins: fill` to emit every seasonal bin for every AOI, with empty ones written as nodata (NaN for floating-point data, the integer sentinel otherwise). That is the setting for time series feeding a model, where each site needs the same bin layout, and it also lets an AOI with no input scenes at all (a zero-length time axis) flow through to a nodata output instead of failing. Point AOIs use the same bins: every point observation is assigned to a seasonal slot by its month and day, and the result has one row per point and bin that has data, with `time` set to the bin start as `MM-DD` and `granule_id` set to `bin_NNNN_<date>` (`all` and a `source_fingerprint` of `composite` for `period: all`, as before). Rows outside the selected season are ignored, NaN values are skipped, and a bin whose samples are all missing is a NaN row. With `empty_bins: fill`, every planned point gets a row for every bin, NaN where it has no data, so each site has the same layout for a climatology or a model feature table.

Composite nodes aggregate `reflectance` when it is present and otherwise the input's only `(time, band, y, x)` variable, so Sentinel-1 backscatter needs no extra key. Set `variable` to choose explicitly when a cube carries several banded variables, for example `variable: backscatter`. The output keeps the source variable's name, dtype family, and nodata convention (NaN for floating-point backscatter), and all-missing pixels stay missing. Point composites aggregate polarization columns (`VV`, `VH`, `HH`, `HV`) and drop the per-scene attribute columns (`flight_direction`, `relative_orbit`, `orbit_type`).

```yaml
nodes:
  - id: sigma0
    type: snap_gpt
    source: s1
    dem: arcticdem
  - id: summer_backscatter
    type: composite
    inputs: {data: sigma0}
    period: 15D
    aggregation: median

outputs:
  s1_summer:
    from: summer_backscatter
    path: ../output/s1_summer
    format: cog
```

The COG writer tiles every `(time, band, y, x)` or `(time, y, x)` variable of the dataset as `<variable>.r<row>.c<column>.tif`. Integer variables use their integer nodata sentinel (Sentinel-2 reflectance DN uses 65535) and floating-point variables use NaN. Tile names, task IDs, and cache keys for Sentinel-2 outputs are unchanged, so existing resume state remains valid.

After the tiles of an output are written (or found already written on a rerun), the writer adds virtual mosaics (VRT files) that stitch them, so a GIS opens the chunks as one raster. A VRT is a small XML file that stores no pixels, lists its tiles by relative path (so the output directory can be moved), keeps band descriptions, nodata and any `encoding` scale and offset, and reads through each tile's own overviews when zoomed out. Each AOI unit gets `<variable>.vrt` beside its tiles (`OUTPUT/AOI/COMPONENT/PRODUCT_ID/GRANULE_ID/<variable>.vrt`), and a polygon output also gets one mosaic per time bin and variable at `OUTPUT/PRODUCT_ID/GRANULE_ID/<variable>.vrt` that lists the tiles of every AOI unit, so QGIS shows all sites as one layer without storing the empty space between them. A unit whose tiles failed or are missing gets no per-unit VRT, failed tiles are left out of the mosaic as gaps, and a unit whose CRS, pixel size, dtype, nodata, scale or bands differ from the others is left out of the mosaic with a warning. Where AOI units overlap, later units draw over earlier ones. rasterio has no mosaic-VRT builder, so `pipeline/writers/vrt.py` writes the XML with the standard library. VRTs are rewritten on every run and are not part of the resume state.

## Decibel conversion and integer packing

The built-in `to_db` processor converts a linear-power cube to decibels, `10 * log10(x)`, lazily and chunk by chunk. Zero, negative, and missing values become NaN rather than `-inf`. The input variable is chosen like `composite`: `reflectance` if present, otherwise the cube's only `(time, band, y, x)` variable, or the node's `variable`. The output replaces it with a float32 variable named `<variable>_db` (for example `backscatter_db`) carrying `units: dB` and NaN nodata, so COG files are self-describing and linear and dB data cannot be mixed up. Integer inputs such as reflectance DN, and data that is already in dB, are rejected. Point rows have their `VV`, `VH`, `HH`, and `HV` columns converted in place and a `units` column set to `dB`.

Convert after compositing: a mean must be taken in linear power, while a median gives the same result in either order. Texture statistics such as GLCM should be computed on dB, whose distribution is far more even than linear backscatter.

```yaml
nodes:
  - id: sigma0_db
    type: to_db
    inputs: {data: summer_backscatter}

outputs:
  s1_summer_db:
    from: sigma0_db
    path: ../output/s1_summer_db
    format: cog
    encoding: {dtype: int16, scale: 0.01}
```

A COG output may set `encoding` to store floating-point variables as `int16` or `uint16` with GDAL scale and offset tags, so readers such as QGIS and rasterio unpack the values automatically. A stored integer `q` means `q * scale + offset`; NaN is stored as the reserved code (`-32768` for `int16`, `65535` for `uint16`) and out-of-range values are clipped to the nearest valid code. A step of 0.01 dB (`int16` covers about ±327 dB) cut a measured Sentinel-1 COG to under half its float32 size with a worst-case error of 0.005 dB, far below speckle noise. Integer variables such as reflectance DN and SCL are never repacked. Quantize only on the final stored product, not before compositing or texture analysis. Floating-point tiles use the floating-point predictor (about 12% smaller than before); this changes only file bytes, not resume keys.

## Spectral indices

The built-in ``s2.index`` processor takes exactly one upstream processing cube and appends named Sentinel-2 spectral index bands to its ``reflectance`` variable. Supported indices are ``NDGI``, ``NDWI1``, ``NDWI2``, ``MSAVI``, ``VARI``, ``RVI``, ``OSAVI``, ``TGI``, ``GLI``, ``NGRDI``, ``CIG``, ``GNDVI``, ``CVI``, ``MTVI2``, ``TCARI``, ``TCI``, ``NARI``, ``TCAPB``, ``TCAPG``, and ``TCAPW``. Calculations are lazy, operate independently for every time step, preserve the upstream spatial chunks, and use Sentinel-2 quantification metadata when a formula depends on absolute reflectance values.

Each index is an auto-registered ``SpectralIndex`` subclass defining its uppercase ``name``, required ``bands``, and ``formula`` method. Formulas that divide must use the module's safe-division helper so zero denominators become missing values consistently for raster and point processing. Adding an index only requires adding a subclass; configuration validation and executor dispatch discover it from the shared registry.

```yaml
nodes:
  - id: preprocess
    type: s2.preprocess
    source: s2
  - id: summer_composites
    type: composite
    inputs: {data: preprocess}
    period: 15D
    aggregation: median
  - id: indices
    type: s2.index
    inputs: {data: summer_composites}
    indices: [NDGI, NDWI1]
```

## DEM terrain features

The built-in ``dem`` processor reads ArcticDEM GeoTIFFs and returns a single canonical-grid feature cube. It supports ``elevation``, ``aspect``, ``slope``, and ``hillshade`` bands. Slope, aspect, and hillshade use GDAL-compatible Horn semantics by default; ``ZevenbergenThorne`` is also available. Derivatives are calculated on the native projected DEM grid before reprojection to the pipeline output grid.

```yaml
sources:
  arcticdem:
    type: dem.local
    data_dir: ../data/arcticdem
    pattern: "*.tif"

nodes:
  - id: terrain
    type: dem
    source: arcticdem
    features: [elevation, aspect, slope, hillshade]
    algorithm: Horn
    hillshade_azimuth: 315
    hillshade_altitude: 45
```

A ``dem.local`` source reads tiles from ``data_dir``. A ``dem.remote`` source (``type: dem.remote``, no other keys) reads the public PGC ArcticDEM v4.1 10 m cloud-optimized GeoTIFFs directly over HTTPS, so nothing is downloaded and the compute nodes need internet access. The tiles covering all AOIs are found with one PGC STAC query over their combined bounds, cached like ``sentinel-py pgc query`` (the same cache entry is reused for identical geometries), and selected per AOI unit from their catalog footprints without opening them. Remote reads hold the same raster lock as local reads, so a worker process makes one network read at a time and the concurrent connections equal ``execution.workers``; GDAL retries throttled requests with a growing delay. No request limit is published for the PGC bucket. ``snap_gpt`` still requires a ``dem.local`` source because SNAP needs the tiles on disk.

New DEM-derived bands can be added by defining and registering a ``DEMFeature`` subclass in ``sentinel_py.pipeline.processors.dem``. The processor validates feature names from the shared registry and assembles the selected bands without changing YAML dispatch. DEM raster reads run under the process-wide raster lock, like every other native GDAL call. Changing a feature's output requires bumping ``PROCESSOR_VERSION`` in ``dem.py``.

## Joining nodes

The built-in ``join`` processor merges the outputs of several nodes into one table or cube, so a pipeline can write a single output holding, for example, spectral indices, terrain, and backscatter. It takes two or more ``inputs`` and no other keys. The first input is the base: its identity and provenance and every one of its rows or time steps are kept, so list the input that varies in time (the composite) first. An input that is ``static`` (such as ``dem`` terrain) is matched on the point or pixel only and repeated for every base time; any other input is matched on the time label too, so seasonal bins of different sources line up, and a missing bin gets missing values.

With point AOIs the rows are left-joined and the value columns of the other inputs are added. An input with several rows per join key (for example uncomposited scenes) and a value column name that already exists are errors, so aggregate with ``composite`` first. With polygon AOIs each AOI unit's cubes are joined into one float32 ``reflectance`` cube whose bands are the inputs' bands in order, written as one COG set (and VRTs, see the COG writer above) instead of one per node. Every input is matched to the base by grid position, so all inputs must be on the same output grid; each input's nodata (65535, -9999, NaN) becomes NaN, and band names must be unique across inputs. The cube's ``recipe_id`` and per-time ``source_fingerprint`` cover every input, so changing any input rewrites the tiles. A joined cube mixes units, so an output fed by a join cannot set ``encoding``; the band descriptions carry the names.

```yaml
nodes:
  - id: samples
    type: join
    inputs: {base: indices, terrain: terrain, radar: s1-db}
outputs:
  samples: {from: samples, path: ../data/samples, format: parquet}  # or cog for polygons
```

## Per-PFT cover models

The `pavc_gridded_rf` processor (package `processors/pavc_gridded_rf/`: `training.py` for the cover table and fitting, `modeling.py` for the node and prediction) trains one scikit-learn `RandomForestRegressor` per plant functional type (PFT) at field plots and maps percent cover onto any polygon AOI. It needs the optional `ml` extra (`pip install sentinel-py[ml]`). It works in two pipelines that share the saved models, so training can use locally staged data at the plots while prediction uses remote data over watersheds or boxes.

**Fit (point AOI).** The AOI holds one point per plot, with the plot identifier (`site_visit_id`) as `aoi_id`. The node takes one input, normally a `join` of spectral indices, terrain and backscatter, with one row per plot (use a single-window composite so each plot has one time step). Every numeric non-identity column of those rows is a feature, in column order (S1 attribute columns such as `relative_orbit` are skipped). A point AOI samples the single pixel containing the plot. The cover values and `qa_review_flag` come from `pft_table`, joined on `id_column`; plots in the AOI but not in the table are ignored, so the AOI file decides which plots train. Plots flagged `yes` are dropped when `drop_qa_flagged` is true, as are plots with no recorded value for any requested PFT. A cover of `-9999` means not recorded: `missing_cover: zero` trains on 0 (the behaviour of the earlier notebooks) and `drop` leaves the plot out of that PFT's model only. Plots with any non-finite feature are left out with a warning. There is no Cook's-distance filtering and no hold-out split.

One forest is fitted per PFT and saved as `model_dir/<recipe id>/<pft>.joblib` with `training_features.parquet` (the features of every plot), `oob_predictions.parquet` and a `manifest.json` (feature names, training size, out-of-bag R2 and top features per PFT) that is written last, so an interrupted fit is never reused. A rerun with the same recipe finds the manifest and skips fitting. The node returns one row per plot with the observed cover and the out-of-bag prediction (integer percent) of each PFT, which a `parquet` output writes; a pipeline needs at least one output.

**Predict (polygon AOI).** Set `models` to a `model_dir/<recipe id>` folder instead of the fit keys; `pfts` may select a subset of the saved models, and any fit key (`pft_table`, `model_dir`, hyperparameters, filtering options) is rejected. The input is a joined cube with one time step per AOI unit, with the same feature bands in the same order as at fit time, otherwise the node fails and names the first difference. Keep the S2, terrain and S1 nodes identical in both pipelines and use the same `output_grid`. Workers load the models from the folder, so it must be on a filesystem they share. The output is a lazy `cover` variable of `uint8` with one band per PFT: percent cover rounded to an integer, clipped to 0-100, with nodata 255 where any feature is missing. Write it with a `cog` output (no `encoding`).

The fit recipe ID covers the feature names and values, the content of the cover table, the PFT list, the hyperparameters, `seed`, the filtering options and the scikit-learn major.minor version, so changing any of them fits new models in a new folder. The prediction recipe ID covers the upstream recipe and scenes, the feature names and the content of the model files, so refitting rewrites the tiles. `PROCESSOR_VERSION` is 1. See `examples/pavc_gridded_rf_points_local_training.yaml` and `examples/pavc_gridded_rf_watersheds_remote_prediction.yaml`.

| Key | Default | Meaning |
|---|---|---|
| `pft_table`, `model_dir` | required to fit | Cover table and model folder, relative to the pipeline file |
| `pfts` | required to fit | PFT columns to model (`PFTClass` values) |
| `models` | none | Saved-model folder; selects predict mode |
| `id_column` | `site_visit_id` | Plot identifier column of the cover table |
| `n_estimators`, `max_depth`, `max_features` | 100, 10, 10 | Forest hyperparameters (`max_depth: null` is unlimited; an integer `max_features` may not exceed the number of bands) |
| `seed` | 42 | Random state |
| `drop_qa_flagged` | true | Drop plots with `qa_review_flag` equal to `yes` |
| `missing_cover` | `zero` | `zero` or `drop` for `-9999` cover values |
| `n_jobs` | 1 | Threads used while fitting (-1 for all cores) |

## Sentinel-1 preprocessing with SNAP

The built-in `snap_gpt` processor runs the external SNAP `gpt` tool once per Sentinel-1 GRD scene and AOI unit, then exposes the terrain-corrected scenes as a lazy `backscatter` variable with dimensions `(time, band, y, x)` on the canonical output grid. Install SNAP 14 first (see `docs/quickstart/install.md`), then stage orbit files and the EGM96 grid with `sentinel-py asf download` so processing never needs the network. The node sources are an `s1.grd.local` source (ASF ZIPs plus the staged orbits) and a `dem.local` source holding ArcticDEM tiles.

```yaml
sources:
  s1:
    type: s1.grd.local
    data_dir: ../data/s1/raw
    flight_direction: PREDOMINANT   # BOTH, ASCENDING, DESCENDING or PREDOMINANT
    orbit: precise-or-restituted    # precise, restituted or precise-or-restituted
  arcticdem:
    type: dem.local
    data_dir: ../data/arcticdem/10m

nodes:
  - id: sigma0
    type: snap_gpt
    source: s1
    dem: arcticdem
    polarizations: [VV, VH]
    subset: aoi                     # aoi (AOI plus margin) or scene
    dem_vertical_datum: ellipsoid   # ArcticDEM heights are ellipsoidal
```

The built-in graph `builtin:grd_sigma0` applies the orbit file, removes GRD border and thermal noise (without clipping negative values, so later temporal averaging is unbiased), calibrates to linear sigma0, subsets, and terrain-corrects onto the pipeline grid with the external DEM. Output stays in linear power; convert to dB after compositing. A custom graph is a SNAP graph XML file whose `${variables}` are either supplied by sentinel-py (`input`, `output`, `orbitType`, `polarisations`, `sourceBands`, `geoRegion`, `demFile`, `demNoData`, `demApplyEGM`, `mapProjection`, `pixelSpacing`, `gridOriginX`, `gridOriginY`) or listed under the node's `parameters`; unknown or unused names are rejected. SNAP's standard-grid origin is set to the grid anchor plus half a pixel so output pixels line up exactly with the canonical grid.

Node keys: `source`, `dem` (required); `on_no_scenes` (`warn` by default, or `error`); `graph` (default `builtin:grd_sigma0`), `polarizations` (default `[VV, VH]`), `dem_vertical_datum` (`ellipsoid` or `egm96`), `subset` (`aoi` or `scene`), `subset_margin_m` (2000), `dem_margin_m` (15000), `parameters`, `work_dir` (default `<s1 data_dir>/.sentinel-py/snap_outputs`), `gpt` (default `$SNAP_HOME/bin/gpt`; a bare `gpt` on `PATH` is never used), `memory_gb` (16), `threads` (4), `max_concurrent` (2), and `timeout_s`. Every `gpt` is a separate JVM, so `max_concurrent` caps how many run at once on one machine, across all Dask workers, threads, and pipelines (advisory lock files in the machine's temporary directory; on Slurm each node enforces its own cap). It is independent of `execution.workers`, which would otherwise start one `gpt` per worker. Size it so that `max_concurrent × memory_gb` fits in RAM and `max_concurrent × threads` fits in the CPU cores; the node logs a warning when either is exceeded. Each scene run is cached by a key covering the archive, orbit file, region, DEM tiles and datum, polarizations, output grid, graph hash, parameters, and SNAP version, and writes a JSON manifest with the exact `gpt` command. The DEM tiles must cover each scene's processing region: before any scene runs, the node checks every selected scene against the tile footprints and stops with the number of uncovered scenes. `sentinel-py pgc missing --pipeline <yaml>` reports the same gaps without SNAP or the network and writes them as a GeoJSON AOI for `pgc query` (see the README).

An AOI unit with no Sentinel-1 scenes in the selected years, season, and flight direction is not an error by default: the node logs one warning that names the units and the search that found nothing, and gives them a zero-length time axis so a downstream `composite` with `empty_bins: fill` writes nodata for them (set `on_no_scenes: error` to fail instead). Problems that can be fixed by staging data, such as missing orbit files or DEM tiles, still stop the run. Raster AOIs run each scene as a Dask task inside the shared graph. Point AOIs run the same cached scene jobs on the client with at most `min(execution.workers, max_concurrent)` concurrent `gpt` processes and sample one pixel per scene. The `composite` processor and the COG writer accept the `backscatter` variable directly, so the usual chain is `snap_gpt` then `composite` then a `cog` output (see below).

A scene whose real pixels do not reach an AOI unit's region is skipped for that unit instead of failing the run. The catalog footprint is the four manifest corners joined by straight lon/lat lines, which near a swath edge at high latitude can differ from SNAP's product boundary by kilometres, so a plot can pass scene selection and still fall outside the scene. When SNAP's `Subset` step reports no intersection, the point path records the pair in a `<key>.nooverlap` file next to the scene's outputs (so reruns do not launch `gpt` again), omits that scene's row for the plot, and logs one warning with the number of skipped pairs and the first few scene names. Any other `gpt` failure still stops the run. The raster path does not skip these scenes.
