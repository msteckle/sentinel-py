"""
The pipeline/config.py module contains the PipelineConfig dataclass needed to load and
validate a processing pipeline YAML file that is supplied in ``sentinel-py run``. The
YAML is divided into sections, each corresponding to a specific aspect of the pipeline
configuration. This file also contains dataclasses for each section of the pipeline
YAML. If the pipeline YAML needs to accept new sections, a new corresponding dataclass
should be created and used in the PipelineConfig dataclass.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from pydantic import BaseModel, ValidationError

from sentinel_py.pipeline.grid import (
    ChunkShape,
    GridPlanningError,
    GridSpec,
    plan_aoi_grid,
)
from sentinel_py.pipeline.processor import ProcessorRegistry, get_default_registry

_SEASON_PATTERN = re.compile(r"^(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")

########################################################################################
# Custom YAML-related exceptions
########################################################################################


class PipelineConfigError(ValueError):
    """Raised when a pipeline document does not match the supported schema."""


########################################################################################
# Helper functions for validating/standardizing YAML section values
########################################################################################


def _mapping(value: Any, location: str) -> dict[str, Any]:
    """Ensure the value is a mapping (dictionary)."""
    if not isinstance(value, dict):
        raise PipelineConfigError(f"{location} must be a mapping")
    return value


def _sequence(value: Any, location: str) -> list[Any]:
    """Ensure the value is a sequence (list)."""
    if not isinstance(value, list):
        raise PipelineConfigError(f"{location} must be a list")
    return value


def _only_keys(value: dict[str, Any], allowed: set[str], location: str) -> None:
    """Ensure that the dictionary contains only the allowed keys."""
    unexpected = sorted(set(value) - allowed)
    if unexpected:
        raise PipelineConfigError(
            f"Unsupported key(s) in {location}: {', '.join(unexpected)}"
        )


def _required(value: dict[str, Any], key: str, location: str) -> Any:
    """Ensure that the specified key exists in the dictionary and return its value."""
    if key not in value:
        raise PipelineConfigError(f"Missing required value: {location}.{key}")
    return value[key]


def _identifier(value: Any, location: str) -> str:
    """Ensure the value is a non-empty string and return it."""
    if not isinstance(value, str) or not value.strip():
        raise PipelineConfigError(f"{location} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, location: str) -> int:
    """Ensure the value is a positive integer and return it."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PipelineConfigError(f"{location} must be a positive integer")
    return value


def _resolve_path(value: Any, base_dir: Path, location: str) -> Path:
    """Ensure the value is a valid path string and return the resolved Path object."""
    if not isinstance(value, str) or not value.strip():
        raise PipelineConfigError(f"{location} must be a non-empty path string")
    path = Path(value).expanduser()
    return (base_dir / path).resolve() if not path.is_absolute() else path.resolve()


def _season(value: Any, location: str) -> tuple[int, int]:
    """
    Ensure the value is a valid season string in MM-DD format and return it as a tuple
    of (month, day).
    """
    if not isinstance(value, str) or _SEASON_PATTERN.fullmatch(value) is None:
        raise PipelineConfigError(f"{location} must use MM-DD format")
    month, day = (int(part) for part in value.split("-"))
    try:
        from datetime import date

        date(2000, month, day)
    except ValueError as error:
        raise PipelineConfigError(f"{location} is not a valid month and day") from error
    return month, day


def _finite_number(value: Any, location: str, *, positive: bool) -> float:
    """Ensure the value is a finite number (optionally positive) and return it."""
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
    """
    Ensure the value is a coordinate pair (optionally allowing a scalar) and return it
    as a tuple of floats.
    """
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


########################################################################################
# Data classes for each section of a pipeline YAML
########################################################################################


@dataclass(frozen=True)
class ExecutionConfig:
    """
    Data class and validation for the execution section of the YAML.

    Arguments
    ---------
    method : Literal["local", "dask", "slurm"]
        Execution backend method.
    workers : int, optional
        Number of workers to use (default is 1).
    threads_per_worker : int, optional
        Number of threads per worker (default is 1).
    local_directory : Path | None, optional
        Local directory for temporary storage (default is None).
    scheduler_address : str | None, optional
        Address of the Dask scheduler (default is None).
    jobs : int | None, optional
        Number of jobs to submit (default is None).
    cores_per_job : int | None, optional
        Number of cores per job (default is None).
    processes_per_job : int | None, optional
        Number of processes per job (default is None).
    memory_per_job : str | None, optional
        Memory allocation per job (default is None).
    queue : str | None, optional
        Queue name for job submission (default is None).
    account : str | None, optional
        Account name for job submission (default is None).
    walltime : str | None, optional
        Walltime for job submission (default is None).
    job_extra_directives : tuple[str, ...], optional
        Extra directives for the job script (default is ()).
    job_script_prologue : tuple[str, ...], optional
        Prologue for the job script (default is ()).
    interface : str | None, optional
        Network interface to use (default is None).
    """

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

    @classmethod
    def from_mapping(cls, raw: Any, base_dir: Path):

        # Extract the execution configuration from the raw mapping and validate its keys
        value = _mapping(raw, "execution")
        allowed = {
            "method",
            "workers",
            "threads_per_worker",
            "local_directory",
            "scheduler_address",
            "jobs",
            "cores_per_job",
            "processes_per_job",
            "memory_per_job",
            "queue",
            "account",
            "walltime",
            "job_extra_directives",
            "job_script_prologue",
            "interface",
        }
        _only_keys(value, allowed, "execution")

        # Extract and validate the allowed values
        # Ensure the method is one of the allowed values
        method = _required(value, "method", "execution")
        if method not in {"local", "dask", "slurm"}:
            raise PipelineConfigError("execution.method must be local, dask, or slurm")

        # Ensure the workers and threads per worker are positive integers
        workers = _positive_int(value.get("workers", 1), "execution.workers")
        threads = _positive_int(
            value.get("threads_per_worker", 1), "execution.threads_per_worker"
        )

        # Ensure the local directory is resolved correctly
        local_directory = (
            _resolve_path(
                value["local_directory"], base_dir, "execution.local_directory"
            )
            if value.get("local_directory") is not None
            else None
        )

        # Ensure the scheduler address is valid and required for Dask method
        scheduler_address = value.get("scheduler_address")
        if scheduler_address is not None and (
            not isinstance(scheduler_address, str) or not scheduler_address.strip()
        ):
            raise PipelineConfigError("execution.scheduler_address must be a string")

        # Raise an error if the scheduler address is required but not provided
        if method == "dask" and scheduler_address is None:
            raise PipelineConfigError(
                "execution.scheduler_address is required when method is dask"
            )

        # Check for unsupported keys in local execution method
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

        # Check for unsupported keys in Dask execution method
        if method == "dask":
            unsupported = sorted(
                key for key in value if key not in {"method", "scheduler_address"}
            )
            if unsupported:
                raise PipelineConfigError(
                    "Unsupported external Dask execution value(s): "
                    + ", ".join(unsupported)
                )

        # Ensure the Slurm execution parameters are positive integers if provided
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
                "method",
                "local_directory",
                "jobs",
                "cores_per_job",
                "processes_per_job",
                "memory_per_job",
                "queue",
                "account",
                "walltime",
                "job_extra_directives",
                "job_script_prologue",
                "interface",
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

        # Validate job extra directives and job script prologue
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

        # Validate that certain execution values are non-empty strings
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

        # Return the validated execution configuration
        return cls(
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


@dataclass(frozen=True)
class SelectionConfig:
    """
    Data class representing spatial and temporal selections.

    Arguments
    ---------
    aoi : Path | None
        Area of interest file path (default is None).
    years : tuple[int, ...] | None
        Years to select (default is None).
    start_period : tuple[int, int]
        Start period as a tuple of (month, day).
    end_period : tuple[int, int]
        End period as a tuple of (month, day).
    """

    aoi: Path | None
    years: tuple[int, ...] | None
    start_period: tuple[int, int]
    end_period: tuple[int, int]

    @classmethod
    def from_mapping(cls, raw: Any, base_dir: Path):

        # Extract the "selection" section from the raw mapping and validate its keys
        value = _mapping(raw, "selection")
        _only_keys(value, {"aoi", "years", "speriod", "eperiod"}, "selection")

        # Resolve the area of interest (AOI) file path and validate its existence
        aoi = (
            _resolve_path(value["aoi"], base_dir, "selection.aoi")
            if value.get("aoi") is not None
            else None
        )
        if aoi is not None and not aoi.is_file():
            raise PipelineConfigError(f"selection.aoi does not exist: {aoi}")

        # Process the years selection and validate the values
        raw_years = value.get("years")
        years = None
        if raw_years is not None:
            items = _sequence(raw_years, "selection.years")
            if not items:
                raise PipelineConfigError("selection.years cannot be empty")
            if any(
                isinstance(year, bool) or not isinstance(year, int) for year in items
            ):
                raise PipelineConfigError("selection.years must contain integers")
            years = tuple(sorted(set(items)))
            if any(year < 2015 or year > 9999 for year in years):
                raise PipelineConfigError(
                    "selection.years must be between 2015 and 9999"
                )

        # Process the start and end periods and validate the chronological order
        start = _season(value.get("speriod", "01-01"), "selection.speriod")
        end = _season(value.get("eperiod", "12-31"), "selection.eperiod")
        if end < start:
            raise PipelineConfigError(
                "selection.eperiod must be on or after selection.speriod"
            )

        # Return the validated values
        return cls(aoi=aoi, years=years, start_period=start, end_period=end)


@dataclass(frozen=True)
class OutputGridConfig:
    """
    Data class representing the output grid configuration.

    Arguments
    ---------
    crs : str
        Coordinate reference system of the output grid.
    resolution : tuple[float, float]
        Resolution of the output grid in the x and y directions.
    extent : Literal["aoi"] | None, optional
        Extent of the output grid (default is None).
    anchor : tuple[float, float], optional
        Anchor point of the output grid (default is (0.0, 0.0)).
    chunks : ChunkShape | None, optional
        Chunk shape for the output grid (default is None).
    grid : GridSpec | None, optional
        Canonical grid specification if available (default is None).
    """

    crs: str
    resolution: tuple[float, float]
    extent: Literal["aoi"] | None = None
    anchor: tuple[float, float] = (0.0, 0.0)
    chunks: ChunkShape | None = None
    grid: GridSpec | None = None

    @property
    def is_canonical(self) -> bool:
        """Return whether this request has a concrete AOI-derived pixel grid."""
        return self.grid is not None

    @staticmethod
    def _parse_chunks(raw: Any) -> ChunkShape:
        """Parse the ``output_grid.chunks`` mapping."""
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

    @classmethod
    def from_mapping(cls, raw: Any, selection: SelectionConfig):

        # Extract and validate the output grid configuration from the raw mapping
        value = _mapping(raw, "output_grid")

        # Ensure the CRS is specified and valid
        crs = _identifier(_required(value, "crs", "output_grid"), "output_grid.crs")
        if crs.lower() == "native":
            _only_keys(value, {"crs", "resolution_m"}, "output_grid")
            resolution_m = _positive_int(
                _required(value, "resolution_m", "output_grid"),
                "output_grid.resolution_m",
            )
            resolution = float(resolution_m), float(resolution_m)
            return OutputGridConfig("native", resolution)

        # Ensure only the expected keys are present in the output grid configuration
        _only_keys(
            value,
            {"crs", "resolution", "extent", "anchor", "chunks"},
            "output_grid",
        )

        # Ensure that an AOI is available for planning the canonical output grid
        if selection.aoi is None:
            raise PipelineConfigError(
                "selection.aoi is required for an AOI-derived canonical output grid"
            )

        # Ensure the resolution is a valid coordinate pair
        resolution = _coordinate_pair(
            _required(value, "resolution", "output_grid"),
            "output_grid.resolution",
            positive=True,
            allow_scalar=True,
        )

        # Ensure the extent is set to 'aoi'
        extent = value.get("extent")
        if extent != "aoi":
            raise PipelineConfigError("output_grid.extent must be 'aoi'")

        # Ensure the anchor is a valid coordinate pair
        anchor = _coordinate_pair(
            value.get("anchor", [0, 0]),
            "output_grid.anchor",
            positive=False,
            allow_scalar=False,
        )

        chunks = cls._parse_chunks(_required(value, "chunks", "output_grid"))

        # Plan the canonical output grid based on the AOI, resolution, anchor, and chunks
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

        # Return the finalized output grid configuration
        return cls(
            grid.crs,
            resolution,
            extent="aoi",
            anchor=anchor,
            chunks=chunks,
            grid=grid,
        )


@dataclass(frozen=True)
class S2SourceConfig:
    """
    Data class representing a local Sentinel-2 Level-2A source.

    Arguments
    ---------
    source_id : str
        Identifier for the source.
    data_dir : Path
        Directory containing the source data.
    source_type : str, optional
        Type of the source (default is "s2.l2a.local").
    """

    source_id: str
    data_dir: Path
    source_type: str = "s2.l2a.local"

    @classmethod
    def from_mapping(cls, raw: Any, base_dir: Path):
        # Parse the sources section of the pipeline configuration
        values = _mapping(raw, "sources")
        if not values:
            raise PipelineConfigError("sources must define at least one source")

        # Loop through each source defined in the configuration
        sources: dict[str, S2SourceConfig] = {}
        for raw_id, raw_source in values.items():
            # Ensure the source identifier is valid and extract the mapping
            source_id = _identifier(raw_id, "source identifiers")
            value = _mapping(raw_source, f"sources.{source_id}")
            _only_keys(value, {"type", "data_dir"}, f"sources.{source_id}")

            # Extract and validate the source type and data directory
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
                raise PipelineConfigError(
                    f"S2 source directory does not exist: {data_dir}"
                )
            sources[source_id] = S2SourceConfig(source_id, data_dir, source_type)
        return sources


@dataclass(frozen=True)
class OutputConfig:
    """Named materialization target associated with a pipeline node."""

    output_id: str
    node: str
    path: Path
    format: str

    @classmethod
    def parse_many(
        cls,
        raw: Any,
        nodes: tuple[NodeConfig, ...],
        base_dir: Path,
    ) -> dict[str, OutputConfig]:
        """Parse and validate the named ``outputs`` mapping."""
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
                raise PipelineConfigError(
                    f"{location} cannot define both 'from' and 'node'"
                )
            reference_value = value.get("from", value.get("node"))
            if reference_value is None:
                raise PipelineConfigError(f"Missing required value: {location}.from")
            node_id = _identifier(reference_value, f"{location}.from")
            if node_id not in node_ids:
                raise PipelineConfigError(
                    f"{location}.from references missing node {node_id!r}"
                )
            output_format = _identifier(
                value.get("format", "cog"), f"{location}.format"
            )
            path = _resolve_path(
                _required(value, "path", location), base_dir, f"{location}.path"
            )
            outputs[output_id] = cls(output_id, node_id, path, output_format.lower())
        return outputs


@dataclass(frozen=True)
class NodeConfig:
    """
    Data class representing one validated semantic node in the pipeline graph.

    Arguments
    ---------
    node_id : str
        Identifier for the node.
    node_type : str
        Type of the node.
    inputs : Mapping[str, str]
        Mapping of input names to source node IDs.
    config : BaseModel
        Configuration for the node's processor.

    """

    node_id: str
    node_type: str
    inputs: Mapping[str, str]
    config: BaseModel

    @staticmethod
    def _parse_inputs(raw: Any, location: str) -> dict[str, str]:
        """Parse a node's optional ``inputs`` mapping."""
        if raw is None:
            return {}
        values = _mapping(raw, location)
        inputs: dict[str, str] = {}
        for raw_name, raw_reference in values.items():
            name = _identifier(raw_name, f"{location} names")
            inputs[name] = _identifier(raw_reference, f"{location}.{name}")
        return inputs

    @classmethod
    def parse_many(
        cls,
        raw: Any,
        registry: ProcessorRegistry,
    ) -> tuple[NodeConfig, ...]:
        """Parse the ordered ``nodes`` list and validate each processor config."""
        values = _sequence(raw, "nodes")
        if not values:
            raise PipelineConfigError("nodes must contain at least one node")

        nodes: list[NodeConfig] = []
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

            inputs = cls._parse_inputs(value.pop("inputs", None), f"{location}.inputs")
            value.pop("id")
            value.pop("type")
            try:
                processor_config = processor.config_model.model_validate(value)
            except ValidationError as error:
                raise PipelineConfigError(
                    f"Invalid configuration for node {node_id!r}: {error}"
                ) from error
            nodes.append(cls(node_id, node_type, inputs, processor_config))
        return tuple(nodes)


########################################################################################
# Data class for the YAML
########################################################################################


@dataclass(frozen=True)
class PipelineConfig:
    """
    Data class representing the entire pipeline configuration, including sources, nodes,
    outputs, and execution settings.

    Arguments
    ---------
    path : Path
        Path to the pipeline configuration file.
    execution : ExecutionConfig
        Execution settings for the pipeline.
    selection : SelectionConfig
        Selection criteria for the pipeline.
    output_grid : OutputGridConfig
        Output grid configuration.
    sources : Mapping[str, S2SourceConfig]
        Mapping of source IDs to their configurations.
    nodes : tuple[NodeConfig, ...]
        Tuple of pipeline node configurations.
    ordered_nodes : tuple[NodeConfig, ...]
        Tuple of pipeline node configurations in dependency order.
    outputs : Mapping[str, OutputConfig]
        Mapping of output IDs to their configurations.
    """

    path: Path
    execution: ExecutionConfig
    selection: SelectionConfig
    output_grid: OutputGridConfig
    sources: Mapping[str, S2SourceConfig]
    nodes: tuple[NodeConfig, ...]
    ordered_nodes: tuple[NodeConfig, ...]
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

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        registry: ProcessorRegistry | None = None,
    ) -> PipelineConfig:
        """Load one YAML document and construct its fully validated configuration."""

        # Ensure the pipeline YAML file exists and is readable
        pipeline_path = Path(path).expanduser().resolve()
        if not pipeline_path.is_file():
            raise PipelineConfigError(f"Pipeline YAML does not exist: {pipeline_path}")
        try:
            loaded = yaml.safe_load(pipeline_path.read_text())
        except yaml.YAMLError as error:
            raise PipelineConfigError(
                f"Could not parse pipeline YAML: {error}"
            ) from error

        # Extract the root mapping and ensure it contains only the expected keys
        root = _mapping(loaded, "pipeline")
        _only_keys(
            root,
            {
                "version",
                "execution",
                "selection",
                "output_grid",
                "sources",
                "nodes",
                "outputs",
            },
            "pipeline",
        )

        # Determine the processor registry to use, defaulting if none is provided
        registry = get_default_registry()

        # Parse the individual sections of the pipeline configuration
        base_dir = pipeline_path.parent

        # Execution section
        execution = ExecutionConfig.from_mapping(
            _required(root, "execution", "pipeline"), base_dir
        )
        # Selection section
        selection = SelectionConfig.from_mapping(
            _required(root, "selection", "pipeline"), base_dir
        )
        # Output section
        output_grid = OutputGridConfig.from_mapping(
            _required(root, "output_grid", "pipeline"), selection
        )
        # Sources section
        sources = S2SourceConfig.from_mapping(
            _required(root, "sources", "pipeline"), base_dir
        )
        # Nodes section
        nodes = NodeConfig.parse_many(_required(root, "nodes", "pipeline"), registry)
        ordered_nodes = cls._topological_order(nodes)
        # Outputs section
        outputs = OutputConfig.parse_many(
            _required(root, "outputs", "pipeline"), nodes, base_dir
        )

        # Validate each processor specified under the nodes section
        cls._validate_processors(nodes, sources, outputs, output_grid, registry)
        return cls(
            pipeline_path,
            execution,
            selection,
            output_grid,
            sources,
            nodes,
            ordered_nodes,
            outputs,
            registry,
        )

    @staticmethod
    def _topological_order(
        nodes: tuple[NodeConfig, ...],
    ) -> tuple[NodeConfig, ...]:
        """Return nodes in dependency order and reject missing references or cycles."""
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
        ordered: list[NodeConfig] = []

        def visit(node: NodeConfig) -> None:
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

    @staticmethod
    def _validate_processors(
        nodes: tuple[NodeConfig, ...],
        sources: Mapping[str, S2SourceConfig],
        outputs: Mapping[str, OutputConfig],
        output_grid: OutputGridConfig,
        registry: ProcessorRegistry,
    ) -> None:
        """Run processor-specific validation after all pipeline sections are parsed."""
        for node in nodes:
            processor = registry.get(node.node_type)
            node_outputs = tuple(
                output for output in outputs.values() if output.node == node.node_id
            )
            try:
                processor.validate(
                    cast(Any, node.config),
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
