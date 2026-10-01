"""Declarative, processor-driven processing pipelines."""

from .config import PipelineConfig, PipelineConfigError
from .grid import ChunkShape, GridSpec, GridWindow

__all__ = [
    "ChunkShape",
    "GridSpec",
    "GridWindow",
    "PipelineConfig",
    "PipelineConfigError",
]
