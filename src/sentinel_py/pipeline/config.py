"""Strict schema, processor configuration, and DAG compilation."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

import yaml
from pydantic import BaseModel, ValidationError

from sentinel_py.pipeline.processor import ProcessorRegistry, get_default_registry
from sentinel_py.pipeline.grid import (
    ChunkShape,
    GridPlanningError,
    GridSpec,
    plan_aoi_grid,
)

_SEASON_PATTERN = re.compile(r"^(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


class PipelineConfigError(ValueError):
    """Raised when a pipeline document does not match the supported schema."""


@dataclass(frozen=True)
class ExecutionConfig:
    """Execution backend and resource settings."""

    method: Literal["local", "dask", "slurm"]
    workers: int = 1
    threads_per_worker: int = 1
    local_directory: Path | None = None
    scheduler_address: str | None = None
    jobs: int | None = None
    cores_per_job: int | None = None
    processes_per_job: int | None = None
    memory_per_job: str | None = None
    queue: str | None = None
    account: str | None = None
    walltime: str | None = None
    job_extra_directives: tuple[str, ...] = ()
    job_script_prologue: tuple[str, ...] = ()
    interface: str | None = None


@dataclass(frozen=True)
class SelectionConfig:
    """Spatial and temporal selection applied to source catalogs."""

    aoi: Path | None
    years: tuple[int, ...] | None
    start_period: tuple[int, int]
    end_period: tuple[int, int]


@dataclass(frozen=True)
class OutputGridConfig:
    """Validated output-grid request and its canonical plan when available."""

    crs: str
    resolution: tuple[float, float]
    extent: Literal["aoi"] | None = None
    anchor: tuple[float, float] = (0.0, 0.0)
    chunks: ChunkShape | None = None
    grid: GridSpec | None = None

    @property
    def resolution_m(self) -> int:
        """Return the square integral resolution expected by the legacy processor."""
        x_resolution, y_resolution = self.resolution
        if x_resolution != y_resolution or not x_resolution.is_integer():
            raise ValueError("The legacy processor requires a square metre resolution")
        return int(x_resolution)

    @property
    def is_canonical(self) -> bool:
        """Return whether this request has a concrete AOI-derived pixel grid."""
        return self.grid is not None


@dataclass(frozen=True)
class S2SourceConfig:
    """One local Sentinel-2 Level-2A source."""

    source_id: str
    data_dir: Path
    source_type: str = "s2.l2a.local"


@dataclass(frozen=True)
class PipelineNode:
    """One validated semantic node in the pipeline graph."""

    node_id: str
    node_type: str
    inputs: Mapping[str, str]
    config: BaseModel


@dataclass(frozen=True)
class OutputConfig:
    """Named materialization target associated with a pipeline node."""

    output_id: str
    node: str
    path: Path
    format: str


@dataclass(frozen=True)
class PipelineConfig:
    """A fully validated pipeline plus its dependency-ordered node graph."""

    path: Path
    version: int
    execution: ExecutionConfig
    selection: SelectionConfig
    output_grid: OutputGridConfig
    sources: Mapping[str, S2SourceConfig]
    nodes: tuple[PipelineNode, ...]
    ordered_nodes: tuple[PipelineNode, ...]
    outputs: Mapping[str, OutputConfig]
    registry: ProcessorRegistry = field(repr=False, compare=False)

    @property
    def source(self) -> S2SourceConfig:
        """Return the first source for compatibility with the version-one API."""
        return next(iter(self.sources.values()))

    @property
    def node(self) -> BaseModel:
        """Return the first node's processor configuration for compatibility."""
        return self.nodes[0].config

    @property
    def output(self) -> OutputConfig:
        """Return the first named output for compatibility with the version-one API."""
        return next(iter(self.outputs.values()))


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PipelineConfigError(f"{location} must be a mapping")
    return value


def _sequence(value: Any, location: str) -> list[Any]:
    if not isinstance(value, list):
        raise PipelineConfigError(f"{location} must be a list")
    return value


def _only_keys(value: dict[str, Any], allowed: set[str], location: str) -> None:
    unexpected = sorted(set(value) - allowed)
    if unexpected:
        raise PipelineConfigError(
            f"Unsupported key(s) in {location}: {', '.join(unexpected)}"
        )


def _required(value: dict[str, Any], key: str, location: str) -> Any:
    if key not in value:
        raise PipelineConfigError(f"Missing required value: {location}.{key}")
    return value[key]


def _identifier(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PipelineConfigError(f"{location} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PipelineConfigError(f"{location} must be a positive integer")
    return value


def _resolve_path(value: Any, base_dir: Path, location: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise PipelineConfigError(f"{location} must be a non-empty path string")
    path = Path(value).expanduser()
    return (base_dir / path).resolve() if not path.is_absolute() else path.resolve()


def _season(value: Any, location: str) -> tuple[int, int]:
    if not isinstance(value, str) or _SEASON_PATTERN.fullmatch(value) is None:
        raise PipelineConfigError(f"{location} must use MM-DD format")
    month, day = (int(part) for part in value.split("-"))
    try:
        from datetime import date

        date(2000, month, day)
    except ValueError as error:
        raise PipelineConfigError(f"{location} is not a valid month and day") from error
    return month, day


def _parse_execution(raw: Any, base_dir: Path) -> ExecutionConfig:
    value = _mapping(raw, "execution")
    allowed = {
        "method", "workers", "threads_per_worker", "local_directory",
        "scheduler_address", "jobs", "cores_per_job", "processes_per_job",
        "memory_per_job", "queue", "account", "walltime",
        "job_extra_directives", "job_script_prologue", "interface",
    }
    _only_keys(value, allowed, "execution")
    method = _required(value, "method", "execution")
    if method not in {"local", "dask", "slurm"}:
        raise PipelineConfigError("execution.method must be local, dask, or slurm")
    workers = _positive_int(value.get("workers", 1), "execution.workers")
    threads = _positive_int(
        value.get("threads_per_worker", 1), "execution.threads_per_worker"
    )
    local_directory = (
        _resolve_path(value["local_directory"], base_dir, "execution.local_directory")
        if value.get("local_directory") is not None
        else None
    )
    scheduler_address = value.get("scheduler_address")
    if scheduler_address is not None and (
        not isinstance(scheduler_address, str) or not scheduler_address.strip()
    ):
        raise PipelineConfigError("execution.scheduler_address must be a string")
    if method == "dask" and scheduler_address is None:
        raise PipelineConfigError(
            "execution.scheduler_address is required when method is dask"
        )
    if method == "local":
        unsupported = sorted(
            key
            for key in value
            if key not in {"method", "workers", "threads_per_worker"}
        )
        if unsupported:
            raise PipelineConfigError(
                "Unsupported local execution value(s): " + ", ".join(unsupported)
            )
        if threads != 1:
            raise PipelineConfigError(
                "execution.threads_per_worker must be 1 for the local process backend"
            )
    if method == "dask":
        unsupported = sorted(
            key for key in value if key not in {"method", "scheduler_address"}
        )
        if unsupported:
            raise PipelineConfigError(
                "Unsupported external Dask execution value(s): "
                + ", ".join(unsupported)
            )
    jobs = (
        _positive_int(value["jobs"], "execution.jobs")
        if value.get("jobs") is not None
        else None
    )
    cores = (
        _positive_int(value["cores_per_job"], "execution.cores_per_job")
        if value.get("cores_per_job") is not None
        else None
    )
    processes = (
        _positive_int(value["processes_per_job"], "execution.processes_per_job")
        if value.get("processes_per_job") is not None
        else None
    )
    if method == "slurm":
        slurm_keys = {
            "method", "local_directory", "jobs", "cores_per_job",
            "processes_per_job", "memory_per_job", "queue", "account",
            "walltime", "job_extra_directives", "job_script_prologue", "interface",
        }
        unsupported = sorted(key for key in value if key not in slurm_keys)
        if unsupported:
            raise PipelineConfigError(
                "Unsupported Slurm execution value(s): " + ", ".join(unsupported)
            )
        missing = [
            key
            for key, current in (
                ("jobs", jobs),
                ("cores_per_job", cores),
                ("processes_per_job", processes),
                ("memory_per_job", value.get("memory_per_job")),
                ("walltime", value.get("walltime")),
            )
            if current is None
        ]
        if missing:
            raise PipelineConfigError(
                "Missing required Slurm execution value(s): " + ", ".join(missing)
            )
        if processes is not None and cores is not None and processes > cores:
            raise PipelineConfigError(
                "execution.processes_per_job cannot exceed cores_per_job"
            )
    directives = value.get("job_extra_directives", [])
    if not isinstance(directives, list) or not all(
        isinstance(item, str) for item in directives
    ):
        raise PipelineConfigError(
            "execution.job_extra_directives must be a list of strings"
        )
    prologue = value.get("job_script_prologue", [])
    if not isinstance(prologue, list) or not all(
        isinstance(item, str) for item in prologue
    ):
        raise PipelineConfigError(
            "execution.job_script_prologue must be a list of strings"
        )
    string_fields = {
        key: value.get(key)
        for key in ("memory_per_job", "queue", "account", "walltime", "interface")
    }
    invalid_strings = [
        key
        for key, item in string_fields.items()
        if item is not None and (not isinstance(item, str) or not item.strip())
    ]
    if invalid_strings:
        raise PipelineConfigError(
            "Execution value(s) must be non-empty strings: "
            + ", ".join(invalid_strings)
        )
    return ExecutionConfig(
        method=method,
        workers=workers,
        threads_per_worker=threads,
        local_directory=local_directory,
        scheduler_address=scheduler_address,
        jobs=jobs,
        cores_per_job=cores,
        processes_per_job=processes,
        memory_per_job=value.get("memory_per_job"),
        queue=value.get("queue"),
        account=value.get("account"),
        walltime=value.get("walltime"),
        job_extra_directives=tuple(directives),
        job_script_prologue=tuple(prologue),
        interface=value.get("interface"),
    )


def _parse_selection(raw: Any, base_dir: Path) -> SelectionConfig:
    value = _mapping(raw, "selection")
    _only_keys(value, {"aoi", "years", "speriod", "eperiod"}, "selection")
    aoi = (
        _resolve_path(value["aoi"], base_dir, "selection.aoi")
        if value.get("aoi") is not None
        else None
    )
    if aoi is not None and not aoi.is_file():
        raise PipelineConfigError(f"selection.aoi does not exist: {aoi}")
    raw_years = value.get("years")
    years = None
    if raw_years is not None:
        items = _sequence(raw_years, "selection.years")
        if not items:
            raise PipelineConfigError("selection.years cannot be empty")
        if any(isinstance(year, bool) or not isinstance(year, int) for year in items):
            raise PipelineConfigError("selection.years must contain integers")
        years = tuple(sorted(set(items)))
        if any(year < 2015 or year > 9999 for year in years):
            raise PipelineConfigError("selection.years must be between 2015 and 9999")
    start = _season(value.get("speriod", "01-01"), "selection.speriod")
    end = _season(value.get("eperiod", "12-31"), "selection.eperiod")
    if end < start:
        raise PipelineConfigError(
            "selection.eperiod must be on or after selection.speriod"
        )
    return SelectionConfig(aoi, years, start, end)


def _finite_number(value: Any, location: str, *, positive: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PipelineConfigError(f"{location} must be a number")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0):
        qualifier = "a positive finite number" if positive else "a finite number"
        raise PipelineConfigError(f"{location} must be {qualifier}")
    return number


def _coordinate_pair(
    value: Any,
    location: str,
    *,
    positive: bool,
    allow_scalar: bool,
) -> tuple[float, float]:
    if allow_scalar and isinstance(value, (int, float)) and not isinstance(value, bool):
        number = _finite_number(value, location, positive=positive)
        return number, number
    if not isinstance(value, list) or len(value) != 2:
        expected = "a number or a two-item list" if allow_scalar else "a two-item list"
        raise PipelineConfigError(f"{location} must be {expected}")
    return (
        _finite_number(value[0], f"{location}[0]", positive=positive),
        _finite_number(value[1], f"{location}[1]", positive=positive),
    )


def _parse_chunks(raw: Any) -> ChunkShape:
    value = _mapping(raw, "output_grid.chunks")
    _only_keys(value, {"y", "x"}, "output_grid.chunks")
    return ChunkShape(
        y=_positive_int(
            _required(value, "y", "output_grid.chunks"), "output_grid.chunks.y"
        ),
        x=_positive_int(
            _required(value, "x", "output_grid.chunks"), "output_grid.chunks.x"
        ),
    )


def _parse_output_grid(raw: Any, selection: SelectionConfig) -> OutputGridConfig:
    value = _mapping(raw, "output_grid")
    crs = _identifier(_required(value, "crs", "output_grid"), "output_grid.crs")
    if crs.lower() == "native":
        _only_keys(value, {"crs", "resolution_m"}, "output_grid")
        resolution_m = _positive_int(
            _required(value, "resolution_m", "output_grid"),
            "output_grid.resolution_m",
        )
        resolution = float(resolution_m), float(resolution_m)
        return OutputGridConfig("native", resolution)

    _only_keys(
        value,
        {"crs", "resolution", "extent", "anchor", "chunks"},
        "output_grid",
    )
    if selection.aoi is None:
        raise PipelineConfigError(
            "selection.aoi is required for an AOI-derived canonical output grid"
        )
    resolution = _coordinate_pair(
        _required(value, "resolution", "output_grid"),
        "output_grid.resolution",
        positive=True,
        allow_scalar=True,
    )
    extent = value.get("extent")
    if extent != "aoi":
        raise PipelineConfigError("output_grid.extent must be 'aoi'")
    anchor = _coordinate_pair(
        value.get("anchor", [0, 0]),
        "output_grid.anchor",
        positive=False,
        allow_scalar=False,
    )
    chunks = _parse_chunks(_required(value, "chunks", "output_grid"))
    try:
        grid = plan_aoi_grid(
            selection.aoi,
            crs=crs,
            resolution=resolution,
            anchor=anchor,
            chunks=chunks,
        )
    except GridPlanningError as error:
        raise PipelineConfigError(f"Could not plan output grid: {error}") from error
    return OutputGridConfig(
        grid.crs,
        resolution,
        extent="aoi",
        anchor=anchor,
        chunks=chunks,
        grid=grid,
    )


def _parse_sources(raw: Any, base_dir: Path) -> dict[str, S2SourceConfig]:
    values = _mapping(raw, "sources")
    if not values:
        raise PipelineConfigError("sources must define at least one source")
    sources: dict[str, S2SourceConfig] = {}
    for raw_id, raw_source in values.items():
        source_id = _identifier(raw_id, "source identifiers")
        value = _mapping(raw_source, f"sources.{source_id}")
        _only_keys(value, {"type", "data_dir"}, f"sources.{source_id}")
        source_type = _required(value, "type", f"sources.{source_id}")
        if source_type != "s2.l2a.local":
            raise PipelineConfigError(
                f"sources.{source_id}.type must be s2.l2a.local"
            )
        data_dir = _resolve_path(
            _required(value, "data_dir", f"sources.{source_id}"),
            base_dir,
            f"sources.{source_id}.data_dir",
        )
        if not data_dir.is_dir():
            raise PipelineConfigError(f"S2 source directory does not exist: {data_dir}")
        sources[source_id] = S2SourceConfig(source_id, data_dir, source_type)
    return sources


def _parse_inputs(raw: Any, location: str) -> dict[str, str]:
    if raw is None:
        return {}
    values = _mapping(raw, location)
    inputs: dict[str, str] = {}
    for raw_name, raw_reference in values.items():
        name = _identifier(raw_name, f"{location} names")
        reference = _identifier(raw_reference, f"{location}.{name}")
        inputs[name] = reference
    return inputs


def _parse_nodes(
    raw: Any,
    registry: ProcessorRegistry,
) -> tuple[PipelineNode, ...]:
    values = _sequence(raw, "nodes")
    if not values:
        raise PipelineConfigError("nodes must contain at least one node")
    nodes: list[PipelineNode] = []
    seen: set[str] = set()
    for index, raw_node in enumerate(values):
        location = f"nodes[{index}]"
        value = dict(_mapping(raw_node, location))
        node_id = _identifier(_required(value, "id", location), f"{location}.id")
        if node_id in seen:
            raise PipelineConfigError(f"Duplicate node id: {node_id}")
        seen.add(node_id)
        node_type = _identifier(
            _required(value, "type", location), f"{location}.type"
        )
        try:
            processor = registry.get(node_type)
        except KeyError as error:
            supported = ", ".join(registry.type_names) or "none"
            raise PipelineConfigError(
                f"Unknown processor type {node_type!r}; registered types: {supported}"
            ) from error
        inputs = _parse_inputs(value.pop("inputs", None), f"{location}.inputs")
        value.pop("id")
        value.pop("type")
        try:
            processor_config = processor.config_model.model_validate(value)
        except ValidationError as error:
            raise PipelineConfigError(
                f"Invalid configuration for node {node_id!r}: {error}"
            ) from error
        nodes.append(PipelineNode(node_id, node_type, inputs, processor_config))
    return tuple(nodes)


def _topological_order(nodes: tuple[PipelineNode, ...]) -> tuple[PipelineNode, ...]:
    node_by_id = {node.node_id: node for node in nodes}
    for node in nodes:
        for input_name, reference in node.inputs.items():
            if reference not in node_by_id:
                raise PipelineConfigError(
                    f"Node {node.node_id!r} input {input_name!r} references "
                    f"missing node {reference!r}"
                )

    state: dict[str, int] = {}
    stack: list[str] = []
    ordered: list[PipelineNode] = []

    def visit(node: PipelineNode) -> None:
        current = state.get(node.node_id, 0)
        if current == 2:
            return
        if current == 1:
            cycle_start = stack.index(node.node_id)
            cycle = stack[cycle_start:] + [node.node_id]
            raise PipelineConfigError(
                "Pipeline node cycle detected: " + " -> ".join(cycle)
            )
        state[node.node_id] = 1
        stack.append(node.node_id)
        for reference in node.inputs.values():
            visit(node_by_id[reference])
        stack.pop()
        state[node.node_id] = 2
        ordered.append(node)

    for node in nodes:
        visit(node)
    return tuple(ordered)


def _parse_outputs(
    raw: Any,
    nodes: tuple[PipelineNode, ...],
    base_dir: Path,
) -> dict[str, OutputConfig]:
    values = _mapping(raw, "outputs")
    if not values:
        raise PipelineConfigError("outputs must define at least one named output")
    node_ids = {node.node_id for node in nodes}
    outputs: dict[str, OutputConfig] = {}
    for raw_id, raw_output in values.items():
        output_id = _identifier(raw_id, "output identifiers")
        location = f"outputs.{output_id}"
        value = _mapping(raw_output, location)
        _only_keys(value, {"from", "node", "path", "format"}, location)
        if "from" in value and "node" in value:
            raise PipelineConfigError(f"{location} cannot define both 'from' and 'node'")
        reference_value = value.get("from", value.get("node"))
        if reference_value is None:
            raise PipelineConfigError(f"Missing required value: {location}.from")
        reference = _identifier(reference_value, f"{location}.from")
        if reference not in node_ids:
            raise PipelineConfigError(
                f"{location}.from references missing node {reference!r}"
            )
        output_format = _identifier(value.get("format", "vrt"), f"{location}.format")
        path = _resolve_path(
            _required(value, "path", location), base_dir, f"{location}.path"
        )
        outputs[output_id] = OutputConfig(
            output_id, reference, path, output_format.lower()
        )
    return outputs


def _validate_processors(
    nodes: tuple[PipelineNode, ...],
    sources: Mapping[str, S2SourceConfig],
    outputs: Mapping[str, OutputConfig],
    output_grid: OutputGridConfig,
    registry: ProcessorRegistry,
) -> None:
    for node in nodes:
        processor = registry.get(node.node_type)
        node_outputs = tuple(
            output for output in outputs.values() if output.node == node.node_id
        )
        try:
            processor.validate(
                node.config,
                node_id=node.node_id,
                inputs=node.inputs,
                sources=sources,
                outputs=node_outputs,
                output_grid=output_grid,
            )
        except (TypeError, ValueError) as error:
            raise PipelineConfigError(
                f"Invalid node {node.node_id!r} ({node.node_type}): {error}"
            ) from error


def load_pipeline(
    path: Path,
    *,
    registry: ProcessorRegistry | None = None,
) -> PipelineConfig:
    """Load, strictly validate, and dependency-order one pipeline YAML document."""
    pipeline_path = Path(path).expanduser().resolve()
    if not pipeline_path.is_file():
        raise PipelineConfigError(f"Pipeline YAML does not exist: {pipeline_path}")
    try:
        loaded = yaml.safe_load(pipeline_path.read_text())
    except yaml.YAMLError as error:
        raise PipelineConfigError(f"Could not parse pipeline YAML: {error}") from error
    root = _mapping(loaded, "pipeline")
    _only_keys(
        root,
        {"version", "execution", "selection", "output_grid", "sources", "nodes", "outputs"},
        "pipeline",
    )
    version = _required(root, "version", "pipeline")
    if isinstance(version, bool) or version != 1:
        raise PipelineConfigError("pipeline.version must be 1")
    selected_registry = registry or get_default_registry()
    base_dir = pipeline_path.parent
    execution = _parse_execution(_required(root, "execution", "pipeline"), base_dir)
    selection = _parse_selection(_required(root, "selection", "pipeline"), base_dir)
    output_grid = _parse_output_grid(
        _required(root, "output_grid", "pipeline"), selection
    )
    sources = _parse_sources(_required(root, "sources", "pipeline"), base_dir)
    nodes = _parse_nodes(_required(root, "nodes", "pipeline"), selected_registry)
    ordered_nodes = _topological_order(nodes)
    outputs = _parse_outputs(
        _required(root, "outputs", "pipeline"), nodes, base_dir
    )
    _validate_processors(nodes, sources, outputs, output_grid, selected_registry)
    return PipelineConfig(
        pipeline_path,
        version,
        execution,
        selection,
        output_grid,
        sources,
        nodes,
        ordered_nodes,
        outputs,
        selected_registry,
    )
