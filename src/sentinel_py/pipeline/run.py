"""Execute validated pipeline nodes in dependency order."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sentinel_py.pipeline.config import PipelineConfig
from sentinel_py.pipeline.execution import compute_dask_tasks
from sentinel_py.pipeline.processors.base import ProcessorContext, get_default_registry
from sentinel_py.pipeline.writers.base import (
    OutputExecutionResult,
    OutputWriterRegistry,
    get_default_writer_registry,
)


@dataclass(frozen=True)
class PipelineRunResult:
    """Artifacts and timing from one dependency-ordered pipeline execution."""

    node_results: Mapping[str, Any]
    outputs: Mapping[str, Any]
    output_results: Mapping[str, OutputExecutionResult]
    started_at: datetime
    ended_at: datetime
    elapsed_seconds: float


def run_pipeline(
    config: PipelineConfig,
    logger,
    *,
    writer_registry: OutputWriterRegistry | None = None,
) -> PipelineRunResult:
    """Build every processor and execute all requested outputs as one Dask graph."""

    # Record the start time of the pipeline run
    started_at = datetime.now().astimezone()
    clock = time.monotonic()
    node_results: dict[str, Any] = {}

    # Loop through each processor listed under the YAML ``nodes`` section
    for node in config.ordered_nodes:
        # Get the node's associated processor from the registry
        registry = get_default_registry()
        processor = registry.get(node.node_type)
        # Get the node's input artifacts from previously executed nodes
        inputs = {
            input_name: node_results[reference]
            for input_name, reference in node.inputs.items()
        }
        # Collect the node's outputs that are requested in the pipeline configuration
        outputs = tuple(
            output for output in config.outputs.values() if output.node == node.node_id
        )
        # Create the processor context for this node
        context = ProcessorContext(config, node, outputs, logger)
        logger.info(
            "Executing node=%s type=%s inputs=%s",
            node.node_id,
            node.node_type,
            ",".join(node.inputs.values()) or "none",
        )
        # Execute the processor and store its result for downstream nodes
        node_results[node.node_id] = processor.execute(node.config, inputs, context)

    # Build and execute output plans using the selected writers
    named_outputs = {
        output_id: node_results[output.node]
        for output_id, output in config.outputs.items()
    }
    selected_writers = writer_registry or get_default_writer_registry()
    plans = {}
    task_ranges = {}
    all_tasks = []

    # Iterate over each output and prepare its execution plan using appropriate writer
    for output_id, output in config.outputs.items():
        try:
            writer = selected_writers.get(output.format)
        except KeyError as error:
            raise ValueError(
                f"Output {output_id!r} uses unregistered format {output.format!r}"
            ) from error
        plan = writer.build(named_outputs[output_id], output, config)
        start = len(all_tasks)
        all_tasks.extend(plan.tasks)
        task_ranges[output_id] = start, len(all_tasks)
        plans[output_id] = writer, plan

    # Execute all tasks using Dask and collect the results
    logger.info(
        "Computing %d output task(s) together with Dask backend=%s",
        len(all_tasks),
        config.execution.method,
    )
    computed = compute_dask_tasks(tuple(all_tasks), config.execution)
    output_results = {}
    for output_id, output in config.outputs.items():
        writer, plan = plans[output_id]
        start, end = task_ranges[output_id]
        results = writer.finalize(plan, computed[start:end])
        output_results[output_id] = OutputExecutionResult(
            output_id,
            output.format,
            output.path,
            results,
        )
        for result in results:
            status = getattr(result, "status", "complete")
            log = logger.error if status == "failed" else logger.info
            log(
                "output=%s task=%s status=%s path=%s error=%s",
                output_id,
                getattr(result, "task_id", "none"),
                status,
                getattr(result, "output_path", output.path),
                getattr(result, "error", None),
            )
    ended_at = datetime.now().astimezone()

    # Return the final pipeline run result
    return PipelineRunResult(
        node_results,
        named_outputs,
        output_results,
        started_at,
        ended_at,
        time.monotonic() - clock,
    )
