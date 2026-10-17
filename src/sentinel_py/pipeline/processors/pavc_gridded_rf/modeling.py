"""Per-PFT random-forest cover model: fit at plots, predict lazily on the AOI boxes.

The node takes one joined feature cube per AOI box (``join`` of S2 indices, terrain
and S1). While the graph is built it samples the cube at the field plots, fits one
forest per PFT (see :mod:`.training`) and saves the models; the output is a lazy
``uint8`` cube with one band per PFT holding percent cover (0-100, nodata 255) that
workers compute chunk by chunk from the saved models.
"""

from __future__ import annotations

import functools
import importlib.metadata
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import AOIKind, MissingCoverPolicy, PFTClass
from sentinel_py.pipeline.artifacts import RasterArtifact
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
    make_recipe_id,
    select_cube_variable,
)
from sentinel_py.pipeline.processors.pavc_gridded_rf.training import (
    DEFAULT_ID_COLUMN,
    ForestParams,
    file_digest,
    fit_pft_models,
    load_manifest,
    load_pft_training,
    model_path,
    sample_features,
)

PROCESSOR_VERSION = 1
OUTPUT_VARIABLE = "cover"
OUTPUT_NODATA = 255
MAX_COVER = 100
_COPIED_COORDS = ("time", "y", "x", "product_id", "granule_id", "source_fingerprint")
_COPIED_ATTRS = ("crs", "transform", "bounds", "time_representation")


########################################################################################
# Node configuration
########################################################################################


class PavcGriddedRFNodeConfig(BaseModel):
    """YAML configuration of one ``pavc_gridded_rf`` node.

    Relative paths resolve against the directory of the pipeline file.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    training: str = Field(min_length=1)
    pft_table: str = Field(min_length=1)
    model_dir: str = Field(min_length=1)
    pfts: tuple[str, ...]
    id_column: str = DEFAULT_ID_COLUMN
    n_estimators: int = Field(100, ge=1)
    max_depth: int | None = Field(10, ge=1)
    max_features: int | float | Literal["sqrt", "log2"] | None = 10
    seed: int = 42
    sample_radius_m: float = Field(0.0, ge=0)
    drop_qa_flagged: bool = True
    missing_cover: Literal["zero", "drop"] = "zero"
    n_jobs: int = Field(1, ge=-1)

    @field_validator("pfts", mode="before")
    @classmethod
    def _pfts(cls, value: Any) -> Any:
        """Accept a YAML list of known PFT names without repeats."""
        if not isinstance(value, (list, tuple)) or not value:
            raise ValueError("pfts must be a non-empty list of PFT names")
        known = {member.value for member in PFTClass}
        unknown = sorted({str(item) for item in value} - known)
        if unknown:
            raise ValueError(
                f"unknown PFT(s): {', '.join(unknown)}; choose from "
                + ", ".join(sorted(known))
            )
        if len(set(value)) != len(value):
            raise ValueError("pfts must not repeat a PFT")
        return tuple(value)


########################################################################################
# Prediction helpers
########################################################################################


@functools.lru_cache(maxsize=32)
def _load_model(path: str) -> Any:
    """Load a saved forest once per worker process, set to predict serially.

    Parameters
    ----------
    path
        Path of a ``<pft>.joblib`` file.

    Returns
    -------
    Any
        The fitted ``RandomForestRegressor``.
    """
    import joblib

    model = joblib.load(path)
    model.set_params(n_jobs=1)
    return model


def _predict_block(block: np.ndarray, *, model_paths: tuple[str, ...]) -> np.ndarray:
    """Predict integer percent cover for every PFT in one chunk of features.

    Parameters
    ----------
    block
        Features shaped ``(band, y, x)``.
    model_paths
        One saved model per output band, in output order.

    Returns
    -------
    numpy.ndarray
        ``uint8`` cube ``(pft, y, x)`` clipped to 0-100 and rounded; pixels with any
        non-finite feature hold the nodata value.
    """
    bands, height, width = block.shape
    out = np.full((len(model_paths), height, width), OUTPUT_NODATA, dtype=np.uint8)
    if height == 0 or width == 0:
        return out
    flat = np.asarray(block, dtype=np.float32).reshape(bands, -1).T
    valid = np.isfinite(flat).all(axis=1)
    if not valid.any():
        return out
    rows = flat[valid]
    for index, path in enumerate(model_paths):
        predicted = np.clip(_load_model(path).predict(rows), 0, MAX_COVER)
        plane = out[index].reshape(-1)
        plane[valid] = np.rint(predicted).astype(np.uint8)
    return out


def _predict_dataset(
    source: Any,
    variable: str,
    config: PavcGriddedRFNodeConfig,
    *,
    model_paths: tuple[str, ...],
    recipe_id: str,
) -> Any:
    """Build the lazy per-PFT cover cube of one AOI unit.

    Parameters
    ----------
    source
        Joined feature cube of the unit.
    variable
        Banded feature variable of ``source``.
    config
        Node configuration (supplies the PFT band names).
    model_paths
        Saved models in ``config.pfts`` order.
    recipe_id
        Recipe identifier stamped on the output.

    Returns
    -------
    xarray.Dataset
        ``cover`` variable ``(time, band, y, x)`` of ``uint8`` with nodata 255.
    """
    import dask.array as da
    import xarray as xr

    features = source[variable].data[0].rechunk({0: -1})
    predicted = da.map_blocks(
        _predict_block,
        features,
        model_paths=model_paths,
        dtype=np.uint8,
        chunks=((len(model_paths),), features.chunks[1], features.chunks[2]),
        meta=np.empty((0, 0, 0), dtype=np.uint8),
    )
    coords = {name: source.coords[name] for name in _COPIED_COORDS}
    coords["band"] = list(config.pfts)
    dataset = xr.Dataset(
        {OUTPUT_VARIABLE: (("time", "band", "y", "x"), predicted[None, ...])},
        coords=coords,
        attrs={
            **{key: source.attrs[key] for key in _COPIED_ATTRS if key in source.attrs},
            "nodata": OUTPUT_NODATA,
            "processor": "pavc_gridded_rf",
            "recipe_id": recipe_id,
        },
    )
    dataset[OUTPUT_VARIABLE].attrs.update(
        nodata=OUTPUT_NODATA, long_name="percent cover by PFT"
    )
    return dataset


########################################################################################
# Processor class for pavc_gridded_rf
########################################################################################


class PavcGriddedRFProcessor(BaseProcessor):
    """Fit a random forest per PFT at field plots and map percent cover on boxes."""

    type_name = "pavc_gridded_rf"
    config_model = PavcGriddedRFNodeConfig
    version = PROCESSOR_VERSION

    def validate(
        self,
        config: ProcessorConfig,
        *,
        node_id: str,
        inputs: Mapping[str, str],
        sources: Mapping[str, Any],
        outputs: tuple[Any, ...],
        output_grid: Any,
    ) -> None:
        """Require exactly one input, besides the generic output checks."""
        super().validate(
            config, node_id=node_id, inputs=inputs, sources=sources,
            outputs=outputs, output_grid=output_grid,
        )
        if not isinstance(config, PavcGriddedRFNodeConfig):
            raise TypeError(f"{node_id} has an invalid pavc_gridded_rf configuration")
        self.single_input(inputs)

    def execute_raster(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Fit (or reuse) the PFT models, then return the lazy cover cubes."""
        if not isinstance(config, PavcGriddedRFNodeConfig):
            raise TypeError("Invalid pavc_gridded_rf configuration")
        source = self.single_input(inputs)
        if not isinstance(source, RasterArtifact):
            raise TypeError("pavc_gridded_rf input must be a RasterArtifact")
        logger = context.logger
        datasets = source.datasets
        variable = select_cube_variable(
            next(iter(datasets.values())), None, owner=self.type_name
        )
        features = self._feature_names(datasets, variable)

        base = Path(context.pipeline.path).parent
        training_csv = self._existing_file(base, config.training, "training")
        pft_csv = self._existing_file(base, config.pft_table, "pft_table")
        recipe_id = self._recipe_id(
            config, datasets, features, file_digest(training_csv), file_digest(pft_csv)
        )
        model_dir = (base / config.model_dir).resolve() / recipe_id

        manifest = load_manifest(model_dir, config.pfts)
        if manifest is not None and manifest["features"] == features:
            logger.info("Reusing PFT models %s (recipe %s).", model_dir, recipe_id)
        else:
            tables = load_pft_training(
                training_csv,
                pft_csv,
                pfts=config.pfts,
                id_column=config.id_column,
                drop_qa_flagged=config.drop_qa_flagged,
                missing_cover=MissingCoverPolicy(config.missing_cover),
                logger=logger,
            )
            self._fit(config, tables, datasets, variable, model_dir, context)

        model_paths = tuple(str(model_path(model_dir, pft)) for pft in config.pfts)
        logger.info(
            "PFT cover node=%s recipe=%s pfts=%s AOI units=%d",
            context.node.node_id, recipe_id, ",".join(config.pfts), len(datasets),
        )
        return RasterArtifact(
            source.plan,
            {
                key: _predict_dataset(
                    dataset, variable, config,
                    model_paths=model_paths, recipe_id=recipe_id,
                )
                for key, dataset in datasets.items()
            },
        )

    def execute_points(
        self,
        config: ProcessorConfig,
        inputs: Mapping[str, Any],
        context: ProcessorContext,
    ) -> Any:
        """Reject point AOIs: prediction needs the polygon boxes around the plots."""
        del config, inputs, context
        raise ValueError(
            f"{self.type_name} requires a {AOIKind.polygon.value} AOI (the boxes "
            "around the training plots), not a point AOI"
        )

    def _feature_names(self, datasets: Mapping[Any, Any], variable: str) -> list[str]:
        """Return the shared feature band names, checking single time steps."""
        names = None
        for key, dataset in datasets.items():
            if dataset.sizes["time"] != 1:
                raise ValueError(
                    f"{self.type_name} needs one time step per AOI unit; {key[0]} has "
                    f"{dataset.sizes['time']} (use a static or single-window composite)"
                )
            bands = [str(band) for band in dataset.band.values]
            if names is None:
                names = bands
            elif bands != names:
                raise ValueError(
                    f"{self.type_name} input AOI units have different feature bands"
                )
        if not names:
            raise ValueError(f"{self.type_name} input has no AOI units")
        return names

    @staticmethod
    def _existing_file(base: Path, value: str, label: str) -> Path:
        """Resolve a configured path against ``base`` and require that it exists."""
        path = (base / Path(value).expanduser()).resolve()
        if not path.is_file():
            raise ValueError(f"pavc_gridded_rf {label} file not found: {path}")
        return path

    def _recipe_id(
        self,
        config: PavcGriddedRFNodeConfig,
        datasets: Mapping[Any, Any],
        features: list[str],
        training_digest: str,
        pft_table_digest: str,
    ) -> str:
        """Return the recipe identifier covering inputs, tables and hyperparameters."""
        try:
            sklearn_version = importlib.metadata.version("scikit-learn")
        except importlib.metadata.PackageNotFoundError as error:
            raise RuntimeError(
                "pavc_gridded_rf requires scikit-learn; install it with the 'ml' extra"
            ) from error
        return make_recipe_id(
            self.type_name,
            self.version,
            source_recipes=sorted(
                {str(dataset.attrs.get("recipe_id")) for dataset in datasets.values()}
            ),
            source_fingerprints=deterministic_cache_key(
                sorted(
                    [key[0], key[1], [str(v) for v in dataset.source_fingerprint.values]]
                    for key, dataset in datasets.items()
                )
            ),
            features=features,
            training=training_digest,
            pft_table=pft_table_digest,
            sklearn=".".join(sklearn_version.split(".")[:2]),
            **{
                key: value
                for key, value in config.model_dump().items()
                if key not in {"training", "pft_table", "model_dir", "n_jobs"}
            },
        )

    def _fit(
        self,
        config: PavcGriddedRFNodeConfig,
        tables: Mapping[str, Any],
        datasets: Mapping[Any, Any],
        variable: str,
        model_dir: Path,
        context: ProcessorContext,
    ) -> None:
        """Sample features at the plots and fit the per-PFT forests."""
        import pandas as pd

        logger: logging.Logger = context.logger
        points = pd.concat(tables.values())[
            ["aoi_id", "latitude", "longitude"]
        ].drop_duplicates("aoi_id")
        label = f"Sampling training features ({context.node.node_id})"
        sampled, outside = sample_features(
            datasets,
            variable,
            points,
            radius_m=config.sample_radius_m,
            execution=context.pipeline.execution,
            report=lambda done, total: context.report_progress(label, done, total),
        )
        if outside:
            logger.warning(
                "%d of %d training plots fall outside every AOI box and were skipped.",
                outside, len(points),
            )
        if sampled.empty:
            raise ValueError("No training plot falls inside an AOI box of this input")
        fit_pft_models(
            tables,
            sampled,
            params=ForestParams(
                n_estimators=config.n_estimators,
                max_depth=config.max_depth,
                max_features=config.max_features,
                seed=config.seed,
                n_jobs=config.n_jobs,
            ),
            model_dir=model_dir,
            logger=logger,
        )
