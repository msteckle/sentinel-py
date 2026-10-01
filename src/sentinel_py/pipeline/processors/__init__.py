"""Built-in pipeline processors."""

from typing import cast

from sentinel_py.pipeline.processors.base import Processor, ProcessorRegistry
from sentinel_py.pipeline.processors.s2_preprocess import S2PreprocessProcessor


def register_builtin_processors(registry: ProcessorRegistry) -> None:
    """Register processors shipped with sentinel-py."""
    registry.register(cast(Processor, S2PreprocessProcessor()))


__all__ = ["S2PreprocessProcessor", "register_builtin_processors"]
