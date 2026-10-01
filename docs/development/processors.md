# Add processors

To create a processor, you need to define a class that inherits from the base ``Processor`` class and implements the required methods for processing the data. You also need to define a corresponding configuration class that inherits from ``ProcessorConfig`` and specifies the parameters for the processor. Once the processor and its configuration are defined, you can reference the processor in the YAML configuration file and provide the necessary parameters for its execution.

The ``Processor`` base class is located in ``sentinel_py/pipeline/processors/base.py``. A ``Processor`` must assign the ``type_name`` attribute, which is the name assigned to the processor that can be called in the ``YAML``. It must also assign the ``config_model`` attribute, which is a pydantic ``BaseModel`` that defines what parameters the processor accepts, plus validation rules for those parameters. This ensures that the processor receives the correct input and behaves as expected during execution.

Below is an example of how to create a custom processor in ``sentinel-py``. This processor, named ``AdditionProcessor``, adds a specified value to each pixel of the input data. We will require the data source (defined in the ``YAML`` under ``sources``) and the integer we want to add to each pixel. Thus, we will start by creating our pydantic BaseModel to define and validate the configuration parameters for the processor.

```python
from sentinel_py.pipeline.processors.base import Processor, ProcessorConfig
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
class AdditionProcessor(Processor):
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

The built-in ``s2.index`` processor takes exactly one upstream processing cube and appends named Sentinel-2 spectral index bands to its ``reflectance`` variable. The initial supported indices are ``NDGI`` and ``NDWI1``. Calculations are lazy, operate independently for every time step, and preserve the upstream spatial chunks.

Each index is an auto-registered ``SpectralIndex`` subclass defining its uppercase ``name``, required ``bands``, ``formula`` method, and ``denominator`` method. Adding an index only requires adding a subclass; configuration validation and executor dispatch discover it from the shared registry.

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
