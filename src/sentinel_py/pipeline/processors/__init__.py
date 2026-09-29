"""Built-in pipeline processors."""

from sentinel_py.pipeline.processor import ProcessorRegistry
from sentinel_py.pipeline.processors.s2_preprocess import S2PreprocessProcessor


def register_builtin_processors(registry: ProcessorRegistry) -> None:
    """Register processors shipped with sentinel-py."""
    registry.register(S2PreprocessProcessor())


__all__ = ["S2PreprocessProcessor", "register_builtin_processors"]
