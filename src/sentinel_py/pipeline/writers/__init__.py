"""Built-in output writers."""

from sentinel_py.pipeline.writers.base import (
    ArtifactOutputWriter,
    OutputExecutionResult,
    OutputWritePlan,
    OutputWriter,
    OutputWriterRegistry,
    get_default_writer_registry,
)
from sentinel_py.pipeline.writers.cog import COGOutputWriter, COGWriteResult

__all__ = [
    "ArtifactOutputWriter",
    "COGOutputWriter",
    "COGWriteResult",
    "OutputExecutionResult",
    "OutputWritePlan",
    "OutputWriter",
    "OutputWriterRegistry",
    "get_default_writer_registry",
]
