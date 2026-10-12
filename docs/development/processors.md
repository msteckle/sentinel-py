# Add processors

Pipeline extensions use parallel contracts: readers acquire source data, processors apply semantic transformations, and writers materialize typed artifacts. A processor must inherit from ``BaseProcessor``, declare ``type_name`` and a frozen Pydantic ``config_model``, and implement both ``execute_raster()`` and ``execute_points()``. The base class dispatches from the validated AOI mode, so processors never need geometry-mode ``isinstance`` branches. Raster hooks exchange ``RasterArtifact`` objects containing lazy xarray datasets; point hooks exchange compact ``PointSampleArtifact`` tables.

Below is an example of how to create a custom processor in ``sentinel-py``. This processor, named ``AdditionProcessor``, adds a specified value to each pixel of the input data. We will require the data source (defined in the ``YAML`` under ``sources``) and the integer we want to add to each pixel. Thus, we will start by creating our pydantic BaseModel to define and validate the configuration parameters for the processor.

```python
from sentinel_py.pipeline.processors.base import BaseProcessor, ProcessorConfig, ProcessorContext
from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Any, Mapping

class AdditionProcessorConfig(BaseModel):

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    # Define your processor configuration parameters here
    source: str = Field(min_length=1)
    value: int = Field(..., description="The value to add per-pixel")

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, value: Any) -> int:
        if not isinstance(value, int):
            raise ValueError("The value must be an integer")
        return value
```

Next, we can build the actual processor class. First, we will instantiate the ``validate`` function, which ensures that the parameters provided in the ``YAML`` configuration file meet the requirements we designed above in the ``AdditionProcessorConfig``.

```python
class AdditionProcessor(BaseProcessor):
    type_name = "addition"
    config_model = AdditionProcessorConfig

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

        # Ensure the configuration is of the correct type
        if not isinstance(config, AdditionProcessorConfig):
            raise TypeError(f"{node_id} has an invalid addition configuration")

    def execute_raster(self, config, inputs, context: ProcessorContext):
        """Return a RasterArtifact without materializing source chunks."""
        ...

    def execute_points(self, config, inputs, context: ProcessorContext):
        """Return a PointSampleArtifact with normalized wide value columns."""
        ...
```

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

The built-in ``dem`` processor reads local ArcticDEM GeoTIFFs and returns a single canonical-grid feature cube. It supports ``elevation``, ``aspect``, ``slope``, and ``hillshade`` bands. Slope, aspect, and hillshade use GDAL-compatible Horn semantics by default; ``ZevenbergenThorne`` is also available. Derivatives are calculated on the native projected DEM grid before reprojection to the pipeline output grid.

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

New DEM-derived bands can be added by defining and registering a ``DEMFeature`` subclass in ``sentinel_py.pipeline.processors.dem``. The processor validates feature names from the shared registry and assembles the selected bands without changing YAML dispatch.
