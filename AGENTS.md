# Sentinel-py contributor instructions

These rules exist so that code written here comes out clean the first time and does not need to be refactored, simplified, or de-duplicated afterwards. Read the whole file before writing code. When a rule here conflicts with a habit from other projects, this file wins.

## Project intent

Sentinel-py queries and downloads Sentinel-1 data from ASF and Sentinel-2 data from CDSE (plus ArcticDEM from PGC), then runs YAML-defined processing pipelines over downloaded data. Processing must scale from serial/local execution to parallel Dask and Slurm workloads over many terabytes. Sentinel-2 processing is the baseline and Sentinel-1 processing is now in scope. Sentinel-1 GRD preprocessing (orbit, noise removal, calibration, terrain correction) runs by invoking the latest SNAP `gpt` executable once per scene with a user-supplied graph XML; do not reimplement SAR calibration or terrain correction in Python. Everything downstream of the per-scene SNAP outputs (compositing, dB conversion, indices, texture, smoothing, point sampling) stays in the Python DAG. SNAP is an external executable, not a Python dependency, and must remain optional so query/download works without it.

## Working rules for agents

- Do not stage, commit, stash, or otherwise touch the git index. Leave all changes in the working tree. When a body of work is done, suggest grouped `git add <files> && git commit -m "<one line>"` commands the user can copy, grouped by what changed.
- For multi-phase work, stop at each phase boundary, report what changed and what was verified, and wait for the user to commit before continuing.
- Do not add or replace a dependency without the user's approval. Keep query/download functionality independent of optional processing and HPC dependencies.
- Preserve unrelated user changes. Never rewrite or delete downloaded data, caches, logs, or processing outputs as incidental cleanup.
- Do not recursively inspect `.venv`, `data`, caches, `__pycache__`, or generated outputs unless the task specifically concerns them.
- Do not report success you did not observe. If credentials, large data, or an HPC environment prevent verification, say so.

## Module map

Find the right home before writing code. Source is under `src/sentinel_py`; tests mirror it under `tests`.

| Location | Responsibility |
|---|---|
| `enums.py` | Every controlled vocabulary (CLI choices, pipeline choices, status values). |
| `aoi.py` | AOI file parsing, batching, GeoJSON conversion, lat/lon grids, `atomic_aoi_geometries`. |
| `log.py` | `get_logger`; persistent file logging plus Rich console output. |
| `cache/` | Cache primitives: `base.py` (keys, atomic writes, `QueryCache`), `rowstore.py` (`RowStore`, `RowWriter`), `registry.py` (`REGISTRY` of named namespaces), `maintenance.py` (inspect, compact, and prune; backs `sentinel-py cache`). |
| `download/` | Provider query and download: `asf.py`, `cdse.py`, `pgc.py`, `s1_orbits.py`, `snap_auxdata.py`, shared `base.py` and `preflight.py`. |
| `cli/` | Typer commands. `main.py` only assembles the app; one module per subcommand; shared options in `options.py`. |
| `pipeline/config.py` | YAML parsing and validation into frozen dataclasses. No execution, no I/O beyond reading config. |
| `pipeline/grid.py` | Output grids, AOI planning (`AOIUnit`, `AOIPlan`), lazy window/warp primitives, `RASTERIO_LOCK`. |
| `pipeline/artifacts.py` | `RasterArtifact` and `PointSampleArtifact`. |
| `pipeline/base.py` | The generic `Registry` shared by processors, readers, and writers. |
| `pipeline/snap.py` | SNAP `gpt` execution for Sentinel-1: gpt/version resolution, per-scene jobs, DEM clipping, offline SNAP user directory, cached per-scene outputs; graphs live in `pipeline/snap_graphs/`. |
| `pipeline/dem_coverage.py` | Geometric check of `snap_gpt` DEM tiles against each scene's processing region (`find_dem_gaps`); backs the processor preflight and `sentinel-py pgc missing`. |
| `pipeline/processors/` | Semantic transformations; `base.py` holds `BaseProcessor`, `make_recipe_id`, `mask_nodata`. |
| `pipeline/readers/` | Source adapters; `base.py` holds `BaseReader` and point-sampling helpers. |
| `pipeline/writers/` | Output writers; `base.py` holds `BaseOutputWriter` and the writer registry. |
| `pipeline/execution.py`, `pipeline/run.py` | Dask backends and building/running the shared task graph. |

## Use what exists before writing anything new

Do not re-implement these. If one is almost right, extend it (and its tests) rather than writing a second copy.

| Need | Use |
|---|---|
| Retry with exponential backoff | `download.base.retry_call` |
| Progress bar, status text, thread-pool download loop | `download.base.make_progress`, `status_text`, `run_download_pool` |
| Download summary, `.part` paths, safe filename from URL | `download.base.DownloadSummary`, `part_path`, `url_filename` |
| Storage preflight for a manifest | `download.preflight.manifest_storage_summary`, `confirm_download` |
| Query result cache (key, directory, reuse, save) | `cache.QueryCache` |
| Enumerate, compact, or prune caches | `cache.list_entries`, `compact_stores`, `select_prunable`, `delete_entries` |
| Keyed state/rows with checkpoints | `cache.REGISTRY[...]` then `.store(root)` or `.store_at(path)`; `RowStore.writer(...)` |
| Deterministic hash of parameters | `cache.deterministic_cache_key` |
| Atomic file writes | `cache.write_parquet_atomic`, `write_json_atomic` |
| Shared CLI options, year/season parsing, AOI loading, manifest lookup | `cli.options` |
| Output grid for an AOI unit | `pipeline.grid.grid_for_unit` |
| Native raster read/reproject onto the grid | `pipeline.grid.window_transform`, `native_pixel_window`, `warp_to_window` |
| Recipe ID for a processor result | `pipeline.processors.base.make_recipe_id` |
| Nodata masking, single upstream input | `mask_nodata`, `BaseProcessor.single_input` |
| Point-observation rows | `pipeline.readers.base.point_row` |
| Which cached CDSE assets satisfy a request | `download.cdse.AssetSelection` |

## Python conventions

- Start every module with a docstring, then `from __future__ import annotations`. Add type hints and detailed NumPy-style docstrings (`Parameters`, `Returns`, `Raises` where they apply) to every function, method, and class that is not a trivial private one-liner. Wrap Python and docstring text at 88 columns.
- Match the layout of `src/sentinel_py/pipeline/processors/s2_index.py`: module docstring, constants, then banner-delimited sections (a line of 88 `#` characters, a title line, another `#` line, two blank lines) ordered helpers first and the public class last.
- Do not extract tiny functions or classes unless the abstraction removes real duplication or is reused. Two call sites of three lines do not need a helper; the same 15-line block in three modules does.
- Prefer cohesive modules over deeply nested or widely interconnected class hierarchies. Put base classes shared by modules in the nearest package-level `base.py`.
- There is no numeric module size cap, but do not grow the largest modules (`download/cdse.py`, `pipeline/config.py`, `pipeline/readers/s2.py`, `download/asf.py`). Put new functionality in a new cohesive module and import it.
- Libraries report through a supplied or module logger and never `print`; only `cli/` modules write to the console. Use Rich for progress via `make_progress`. Use `sentinel_py.log.get_logger` for CLI loggers with a name that is unique per command.
- Use `X | None` unions, `pathlib.Path`, and keyword-only arguments for options. Do not catch broad exceptions to hide failures; convert them to a clear `ValueError`, `RuntimeError`, or `typer.BadParameter`.

## Enums and constants

- Define every fixed set of string choices in `src/sentinel_py/enums.py`, never in the module that first needs it, and never as a `Literal` or raw-string set that duplicates an existing enum. Reuse an existing enum when its members match.
- New enums are `StrEnum` so members format as their value in f-strings, logs, and cache keys. (Older CLI vocabularies are `(str, Enum)`; a plain `(str, Enum)` member formats as `ClassName.member` on Python 3.12+.)
- Exception: node configurations are `strict=True` Pydantic models that receive plain YAML strings, so string-choice fields there stay `Literal[...]`. Compare against the enum's values where useful.
- Persisted vocabularies (`DownloadStatus`, `DownloadAction`, `WriteStatus`) are written to state files; never rename a member's value.
- Shared literals (S2 nodata, endpoints, default config file names) are named constants in one place, not repeated.

## CLI conventions

- Use Typer with `typing.Annotated` so types, validation, requirements, and help text stay together. Reuse the aliases in `cli/options.py` (`YearsOpt`, `SPeriodOpt`, `EPeriodOpt`, `LogOpt`, `VerboseOpt`, `YesOpt`, `cache_dir_option`) and helpers (`parse_years`, `validate_season`, `load_aoi_geometries`, `load_manifest`, `echo_summary`). Use the `PANEL_*` constants for help panels.
- Put each subcommand in its own module under `cli/<provider>/`; keep `cli/main.py` to assembly. Every command has a docstring and `-> None`.
- Public flag names, defaults, and exit codes are API. After changing a command, compare `--help` output with the previous version; only help text may change without approval.

## Adding things (checklists)

New processor: copy `s2_index.py`. Module docstring; `PROCESSOR_VERSION`; frozen `strict=True` config model; subclass `BaseProcessor` with `type_name`, `config_model`, `version = PROCESSOR_VERSION`; `validate` calls `super().validate(...)` first (it checks output formats, so do not re-check them); use `single_input`, `make_recipe_id`, `mask_nodata`, `grid_for_unit`, and `RasterArtifact.map_datasets`; register in `processors/__init__.py`; tests for raster path, point path, validation errors, and recipe-ID sensitivity. Update `docs/development/processors.md` if the contract changes.

New reader or writer: subclass `BaseReader` or `BaseOutputWriter`, take `source_type`/`format_name` from the `SourceType`/`OutputFormat` enums, declare `accepted_artifact_kinds` on writers, register in the default registry, and use the public config validators `only_keys`, `required`, `resolve_path` from `pipeline/config.py`.

New provider command: query and download live in `download/<provider>.py`, use `QueryCache`, `retry_call`, `run_download_pool`, `DownloadSummary`, and `manifest_storage_summary`; the CLI lives in `cli/<provider>/query.py` and `download.py` and uses `cli/options.py`. Cache namespaces are declared once in `cache/registry.py`.

New YAML section or key: add a frozen dataclass and validation in `pipeline/config.py` (table-driven where keys depend on a mode, as `ExecutionConfig` is), keep validation separate from execution, and update `docs/development/yaml.md`. A new execution backend extends `ExecutionMethod` and the key table.

## Processing and scale constraints

- Treat pipeline YAML as a versioned, validated DAG. Keep configuration parsing separate from execution, processors, readers, and writers.
- Preserve lazy raster I/O and build shared Dask graphs so multiple outputs reuse upstream reads. Do not eagerly load full scenes, mosaics, or time series into memory.
- Keep execution-backend behavior consistent across local, external Dask, and Slurm modes. Paths and state must work on shared filesystems and worker processes.
- Design work at scene/chunk granularity, bound memory and concurrency, and avoid per-pixel Python loops or large task payloads.
- Make interrupted work resumable. Write data, manifests, cache entries, and state atomically, flush state in batches through `RowStore.writer`, and make cleanup (partial `.part` files) run on `KeyboardInterrupt` too.
- SNAP steps: take scene, DEM, output grid, and graph parameters from the pipeline YAML, never from hard-coded paths. Compute nodes may be offline, so orbit files and other auxiliary data must be staged during query/download or an explicit prepare step, never fetched by `gpt` at run time. Include the SNAP version, graph hash, orbit file, DEM identity and vertical datum, and output grid in cache keys, and record the exact `gpt` command per scene.
- Log configuration, cache decisions, progress summaries, failures, and enough identifiers to reproduce a run. Avoid noisy per-item logs at normal verbosity.

## Caching, versions, and compatibility

- Include every result-affecting input in cache keys and recipe IDs. A key that omits an input causes silent stale results; a key that gains one invalidates users' caches.
- Existing cache keys, state-file schemas, and recipe IDs are compatibility surface. Before changing a key builder, record the current hash for fixed inputs in a test (see `test_query_cache_key_is_stable_across_releases`), and change it only with the user's approval.
- Changing what a processor produces requires bumping its `PROCESSOR_VERSION`, which deliberately regenerates its cached output once. Say so in the change summary.
- Maintain backward compatibility for public CLI flags, YAML schemas, cache formats, and resumable state unless the user explicitly approves a breaking change.

## Pitfalls that have bitten this codebase

- Never import `RASTERIO_LOCK` by name. Reference it as `pipeline_grid.RASTERIO_LOCK` (`import sentinel_py.pipeline.grid as pipeline_grid`); Dask may pickle closures by value and a lock cannot be pickled. Hold the lock around every native GDAL read, merge, and warp.
- Do not unify code that only looks similar. The S2 reader filters acquisitions to a season while the composite bins already-filtered dates; the two byte formatters intentionally differ. Check what each copy actually guarantees, and pin behavior with a test, before merging.
- Do not change a function's observable error order or messages casually; CLI users and tests depend on them.
- `(str, Enum)` members printed in f-strings give `ClassName.member`; use `StrEnum` or `.value`.
- Registries must be memoized module-level singletons; build them once.
- Tests that need the home directory must never touch the real `~/.sentinel-py`; `tests/conftest.py` redirects it.

## Testing

- Tests mirror source modules: `tests/test_<package>_<module>.py` (for example `test_download_asf.py`, `test_cli_asf.py`, `test_pipeline_config.py`, `test_writers_cog.py`). Put a test next to the module it exercises, not in a convenient existing file.
- Use `tests/helpers.py` for shared builders (`runner`, `write_polygon_aoi`, `write_s5_config`, `write_earthdata_config`, `write_s2_pipeline_yaml`) instead of copying them into a new test file. Tests use temporary directories, mocks, and small synthetic fixtures; they must not contact ASF/CDSE/PGC, submit Slurm jobs, or download imagery.
- Before refactoring a long or behavior-sensitive function, write characterization tests against the current behavior first, then refactor. Pin cache keys, task IDs, and recipe IDs with recorded values.
- New commands need CLI tests for success, a cached/no-op path, and a failing path with its exit code. New validation needs tests for every rejection message.
- `tests/test_repo_conventions.py` enforces structure (module and public docstrings, enums only in `enums.py`, processor version and registration, no `print` in libraries). Fix the code rather than weakening the test.

## Documentation and completion

- Keep each Markdown prose paragraph on one source line; do not hard-wrap it. Code fences, lists, and tables may use separate lines.
- Update CLI help, examples, schema documentation (`docs/development/yaml.md`, `docs/development/processors.md`), and tests when behavior or configuration changes.
- Before declaring work done, run and report exactly what ran: `uv run ruff check src tests`, a focused `uv run pytest -q tests/<file>.py`, then `uv run pytest -q` for cross-cutting changes. For CLI changes also check `uv run sentinel-py --help` and the affected subcommand's `--help`.
- Finish by listing anything you did not do or could not verify, and any user-visible effect (for example a processor version bump that regenerates a cache).
