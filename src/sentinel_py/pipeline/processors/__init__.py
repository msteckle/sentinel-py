"""Built-in pipeline processors."""

from typing import cast

from sentinel_py.pipeline.processors.base import (
    Processor,
    ProcessorRegistry,
    get_default_registry,
)
from sentinel_py.pipeline.processors.s2_composite import S2CompositeProcessor
from sentinel_py.pipeline.processors.s2_index import (
    NDGI,
    NDWI1,
    S2IndexProcessor,
    SpectralIndex,
)
from sentinel_py.pipeline.processors.s2_preprocess import S2PreprocessProcessor


def register_builtin_processors(registry: ProcessorRegistry) -> None:
    """Register processors shipped with sentinel-py."""
    registry.register(cast(Processor, S2PreprocessProcessor()))
    registry.register(cast(Processor, S2CompositeProcessor()))
    registry.register(cast(Processor, S2IndexProcessor()))


__all__ = [
    "Processor",
    "ProcessorRegistry",
    "S2PreprocessProcessor",
    "S2CompositeProcessor",
    "S2IndexProcessor",
    "SpectralIndex",
    "NDGI",
    "NDWI1",
    "get_default_registry",
    "register_builtin_processors",
]
