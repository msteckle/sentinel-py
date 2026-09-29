"""Execute validated pipeline nodes in dependency order."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from sentinel_py.pipeline.config import PipelineConfig
from sentinel_py.pipeline.processor import ProcessorContext
from sentinel_py.pipeline.processors.s2_preprocess import S2PreprocessArtifact
from sentinel_py.s2.base import S2PreprocessResult


@dataclass(frozen=True)
class PipelineRunResult:
    """Artifacts and timing from one dependency-ordered pipeline execution."""

    node_results: Mapping[str, Any]
    outputs: Mapping[str, Any]
    started_at: datetime
    ended_at: datetime
    elapsed_seconds: float

    def _single_s2_artifact(self) -> S2PreprocessArtifact:
        artifacts = {
            id(artifact): artifact
            for artifact in self.outputs.values()
            if isinstance(artifact, S2PreprocessArtifact)
        }
        if len(artifacts) != 1:
            raise AttributeError(
                "This pipeline result does not contain one S2 preprocessing artifact"
            )
        return next(iter(artifacts.values()))

    @property
    def recipe_id(self) -> str:
        """Expose the legacy preprocessing recipe identifier when applicable."""
        return self._single_s2_artifact().recipe_id

    @property
    def state_path(self) -> Path:
        """Expose the legacy preprocessing state path when applicable."""
        return self._single_s2_artifact().state_path

    @property
    def results(self) -> tuple[S2PreprocessResult, ...]:
        """Expose legacy per-granule results when applicable."""
        return self._single_s2_artifact().results


def run_pipeline(config: PipelineConfig, logger) -> PipelineRunResult:
    """Execute registered processors in the graph's topological order."""
    started_at = datetime.now().astimezone()
    clock = time.monotonic()
    node_results: dict[str, Any] = {}

    for node in config.ordered_nodes:
        processor = config.registry.get(node.node_type)
        inputs = {
            input_name: node_results[reference]
            for input_name, reference in node.inputs.items()
        }
        outputs = tuple(
            output for output in config.outputs.values() if output.node == node.node_id
        )
        context = ProcessorContext(config, node, outputs, logger)
        logger.info(
            "Executing node=%s type=%s inputs=%s",
            node.node_id,
            node.node_type,
            ",".join(node.inputs.values()) or "none",
        )
        node_results[node.node_id] = processor.execute(node.config, inputs, context)

    materialized_outputs = {
        output_id: node_results[output.node]
        for output_id, output in config.outputs.items()
    }
    ended_at = datetime.now().astimezone()
    return PipelineRunResult(
        node_results,
        materialized_outputs,
        started_at,
        ended_at,
        time.monotonic() - clock,
    )
