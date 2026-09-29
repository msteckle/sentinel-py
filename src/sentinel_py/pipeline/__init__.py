"""Declarative, processor-driven processing pipelines."""

from .config import PipelineConfig, PipelineConfigError, PipelineNode, load_pipeline
from .grid import ChunkShape, GridSpec, GridWindow
from .processor import Processor, ProcessorConfig, ProcessorRegistry

__all__ = [
    "PipelineConfig",
    "PipelineConfigError",
    "PipelineNode",
    "ChunkShape",
    "GridSpec",
    "GridWindow",
    "Processor",
    "ProcessorConfig",
    "ProcessorRegistry",
    "load_pipeline",
]
