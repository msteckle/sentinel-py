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
from sentinel_py.pipeline.writers.parquet import ParquetOutputWriter, ParquetWriteResult

__all__ = [
    "ArtifactOutputWriter",
    "COGOutputWriter",
    "COGWriteResult",
    "ParquetOutputWriter",
    "ParquetWriteResult",
    "OutputExecutionResult",
    "OutputWritePlan",
    "OutputWriter",
    "OutputWriterRegistry",
    "get_default_writer_registry",
]
