"""Per-PFT random-forest cover model: fit at plots, predict lazily on grids.

Fitting runs in a point-AOI pipeline: the node takes the joined feature rows of the
field plots (one row per plot, from ``join`` of S2 indices, terrain and S1), fits one
forest per PFT (see :mod:`.training`) and saves the models. Prediction runs in a
polygon-AOI pipeline whose ``models`` key points at those saved models; the output is
a lazy ``uint8`` cube with one band per PFT holding percent cover (0-100, nodata 255)
that workers compute chunk by chunk.
"""

from __future__ import annotations

import functools
import importlib.metadata
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sentinel_py.cache import deterministic_cache_key
from sentinel_py.enums import AOIKind, MissingCoverPolicy, PFTClass
from sentinel_py.pipeline.artifacts import PointSampleArtifact, RasterArtifact
from sentinel_py.pipeline.processors.base import (
    BaseProcessor,
    ProcessorConfig,
    ProcessorContext,
    make_recipe_id,
    select_cube_variable,
)
from sentinel_py.pipeline.readers.base import (
    POINT_METADATA_COLUMNS,
    S1_POINT_ATTRIBUTE_COLUMNS,
    S2_POINT_ATTRIBUTE_COLUMNS,
)
from sentinel_py.pipeline.processors.pavc_gridded_rf.training import (
    DEFAULT_ID_COLUMN,
    MANIFEST_NAME,
    OOB_NAME,
    ForestParams,
    file_digest,
    fit_pft_models,
    load_manifest,
    features_digest,
    load_pft_cover,
    model_path,
    read_manifest,
)

PROCESSOR_VERSION = 2
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

    The node either fits models in a point-AOI pipeline (``pft_table``, ``model_dir``
    and ``pfts``, plus optional hyperparameters) or predicts in a polygon-AOI
    pipeline with models fitted by an earlier run (``models``, optionally ``pfts``);
    the two key sets cannot be mixed. Relative paths resolve against the directory of
    the pipeline file.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    models: str | None = Field(None, min_length=1)
    pft_table: str | None = Field(None, min_length=1)
    model_dir: str | None = Field(None, min_length=1)
    pfts: tuple[str, ...] | None = None
    id_column: str = DEFAULT_ID_COLUMN
    n_estimators: int = Field(100, ge=1)
    max_depth: int | None = Field(10, ge=1)
    max_features: int | float | Literal["sqrt", "log2"] | None = 10
    seed: int = 42
    drop_qa_flagged: bool = True
    missing_cover: Literal["zero", "drop"] = "zero"
    n_jobs: int = Field(1, ge=-1)

    @field_validator("pfts", mode="before")
    @classmethod
    def _pfts(cls, value: Any) -> Any:
        """Accept a YAML list of known PFT names without repeats."""
        if value is None:
            return None
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

    @model_validator(mode="after")
    def _one_mode(self) -> PavcGriddedRFNodeConfig:
        """Require either the fit keys or ``models``, never a mix."""
        if self.models is None:
            missing = [
                key
                for key in ("pft_table", "model_dir", "pfts")
                if getattr(self, key) is None
            ]
            if missing:
                raise ValueError(
                    "set 'models' to predict with saved models, or give "
                    + ", ".join(missing)
                    + " to fit them"
                )
            return self
        fit_only = sorted(self.model_fields_set - {"models", "pfts"})
        if fit_only:
            raise ValueError(
                "'models' predicts with saved models, so these fit keys are not "
                "allowed: " + ", ".join(fit_only)
            )
        return self


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
    pfts: tuple[str, ...],
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
    pfts
        PFT names, used as the output band names.
    model_paths
        Saved models in ``pfts`` order.
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
    coords["band"] = list(pfts)
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
    """Fit a random forest per PFT at field plots and map percent cover on grids."""

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
        """Predict cover with saved models, returning the lazy cover cubes."""
        if not isinstance(config, PavcGriddedRFNodeConfig):
            raise TypeError("Invalid pavc_gridded_rf configuration")
        if config.models is None:
            raise ValueError(
                f"{self.type_name} fits models in a {AOIKind.point.value} AOI pipeline "
                "(one point per plot); use 'models' to predict on a "
                f"{AOIKind.polygon.value} AOI"
            )
        source = self.single_input(inputs)
        if not isinstance(source, RasterArtifact):
            raise TypeError("pavc_gridded_rf input must be a RasterArtifact")
        datasets = source.datasets
        variable = select_cube_variable(
            next(iter(datasets.values())), None, owner=self.type_name
        )
        features = self._feature_names(datasets, variable)
        base = Path(context.pipeline.path).parent
        pfts, model_dir, recipe_id = self._saved_models(config, base, datasets, features)
        context.logger.info(
            "Predicting with saved PFT models %s: node=%s recipe=%s pfts=%s AOI "
            "units=%d",
            model_dir, context.node.node_id, recipe_id, ",".join(pfts), len(datasets),
        )
        model_paths = tuple(str(model_path(model_dir, pft)) for pft in pfts)
        return RasterArtifact(
            source.plan,
            {
                key: _predict_dataset(
                    dataset, variable, pfts,
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
        """Fit (or reuse) the PFT models from the plots' feature rows.

        Returns one row per plot with its observed cover and out-of-bag prediction
        for every PFT, so a point pipeline has an output to write.
        """
        import pandas as pd

        if not isinstance(config, PavcGriddedRFNodeConfig):
            raise TypeError("Invalid pavc_gridded_rf configuration")
        if config.models is not None:
            raise ValueError(
                f"{self.type_name} predicts with saved models on a "
                f"{AOIKind.polygon.value} AOI, not a {AOIKind.point.value} AOI"
            )
        source = self.single_input(inputs)
        if not isinstance(source, PointSampleArtifact):
            raise TypeError("pavc_gridded_rf input must be a PointSampleArtifact")
        logger = context.logger
        features = self._plot_features(source.rows)
        names = list(features.columns[1:])
        base = Path(context.pipeline.path).parent
        pft_csv = self._existing_file(base, config.pft_table, "pft_table")
        recipe_id = self._fit_recipe_id(config, features, file_digest(pft_csv))
        model_dir = (base / config.model_dir).resolve() / recipe_id

        manifest = load_manifest(model_dir, config.pfts)
        if manifest is not None and manifest["features"] == names:
            logger.info("Reusing PFT models %s (recipe %s).", model_dir, recipe_id)
        else:
            tables = load_pft_cover(
                pft_csv,
                features["aoi_id"].tolist(),
                pfts=config.pfts,
                id_column=config.id_column,
                drop_qa_flagged=config.drop_qa_flagged,
                missing_cover=MissingCoverPolicy(config.missing_cover),
                logger=logger,
            )
            fit_pft_models(
                tables,
                features,
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
        logger.info(
            "PFT models node=%s recipe=%s dir=%s", context.node.node_id, recipe_id,
            model_dir,
        )
        identity = [
            column
            for column in ("aoi_id", "component_id", "point_index", "longitude", "latitude")
            if column in source.rows.columns
        ]
        rows = source.rows[identity].merge(
            pd.read_parquet(model_dir / OOB_NAME), on="aoi_id", how="left"
        )
        return PointSampleArtifact(rows, source.plan)

    def _plot_features(self, rows: Any) -> Any:
        """Return ``aoi_id`` plus the numeric feature columns, one row per plot.

        Raises
        ------
        ValueError
            If a plot has several rows (for example several time steps) or the rows
            carry no numeric feature column.
        """
        from pandas.api.types import is_numeric_dtype

        names = [
            column
            for column in rows.columns
            if column not in POINT_METADATA_COLUMNS
            and column not in S1_POINT_ATTRIBUTE_COLUMNS
            and column not in S2_POINT_ATTRIBUTE_COLUMNS
            and is_numeric_dtype(rows[column])
        ]
        if not names:
            raise ValueError(f"{self.type_name} input has no numeric feature columns")
        if rows["aoi_id"].duplicated().any():
            raise ValueError(
                f"{self.type_name} needs one row per plot; some plots have several "
                "(use a single-window composite so each plot has one time step)"
            )
        features = rows[["aoi_id", *names]].copy()
        features[names] = features[names].astype("float32")
        return features

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

    def _saved_models(
        self,
        config: PavcGriddedRFNodeConfig,
        base: Path,
        datasets: Mapping[Any, Any],
        features: list[str],
    ) -> tuple[tuple[str, ...], Path, str]:
        """Locate saved models and check they were fitted on these feature bands.

        Parameters
        ----------
        config
            Node configuration in predict mode.
        base
            Directory of the pipeline file.
        datasets
            Input cubes, used for the recipe identifier.
        features
            Feature band names of the input cubes.

        Returns
        -------
        tuple
            The PFTs to predict, the model directory, and the recipe identifier,
            which covers the content of the model files.

        Raises
        ------
        ValueError
            If the directory has no complete manifest, a requested PFT has no model,
            or the feature bands differ from those the models were fitted on.
        """
        model_dir = (base / Path(config.models).expanduser()).resolve()
        manifest = read_manifest(model_dir)
        if manifest is None:
            raise ValueError(
                f"pavc_gridded_rf models directory has no {MANIFEST_NAME}: {model_dir}"
            )
        pfts = config.pfts or tuple(manifest["pfts"])
        absent = [pft for pft in pfts if pft not in manifest["pfts"]]
        if absent:
            raise ValueError(
                f"saved models in {model_dir} do not include: {', '.join(absent)}; "
                f"available: {', '.join(manifest['pfts'])}"
            )
        if load_manifest(model_dir, pfts) is None:
            raise ValueError(f"model file(s) missing in {model_dir}")
        if manifest["features"] != features:
            fitted, given = manifest["features"], features
            first = next(
                (i for i, (a, b) in enumerate(zip(fitted, given)) if a != b),
                min(len(fitted), len(given)),
            )
            raise ValueError(
                f"input has {len(given)} feature bands but the saved models were "
                f"fitted on {len(fitted)}; they first differ at position {first} "
                f"(models: {fitted[first:first + 1] or 'end'}, "
                f"input: {given[first:first + 1] or 'end'})"
            )
        recipe_id = make_recipe_id(
            self.type_name,
            self.version,
            mode="predict",
            **self._input_identity(datasets),
            features=features,
            models={
                pft: file_digest(model_path(model_dir, pft)) for pft in pfts
            },
        )
        return tuple(pfts), model_dir, recipe_id

    @staticmethod
    def _input_identity(datasets: Mapping[Any, Any]) -> dict[str, Any]:
        """Return recipe parameters identifying the upstream recipes and scenes."""
        return {
            "source_recipes": sorted(
                {str(dataset.attrs.get("recipe_id")) for dataset in datasets.values()}
            ),
            "source_fingerprints": deterministic_cache_key(
                sorted(
                    [key[0], key[1], [str(v) for v in dataset.source_fingerprint.values]]
                    for key, dataset in datasets.items()
                )
            ),
        }

    @staticmethod
    def _existing_file(base: Path, value: str, label: str) -> Path:
        """Resolve a configured path against ``base`` and require that it exists."""
        path = (base / Path(value).expanduser()).resolve()
        if not path.is_file():
            raise ValueError(f"pavc_gridded_rf {label} file not found: {path}")
        return path

    def _fit_recipe_id(
        self, config: PavcGriddedRFNodeConfig, features: Any, pft_table_digest: str
    ) -> str:
        """Return the recipe identifier of a fit: feature values, cover table, settings."""
        try:
            sklearn_version = importlib.metadata.version("scikit-learn")
        except importlib.metadata.PackageNotFoundError as error:
            raise RuntimeError(
                "pavc_gridded_rf requires scikit-learn; install it with the 'ml' extra"
            ) from error
        return make_recipe_id(
            self.type_name,
            self.version,
            features=list(features.columns[1:]),
            feature_values=features_digest(features),
            pft_table=pft_table_digest,
            sklearn=".".join(sklearn_version.split(".")[:2]),
            **{
                key: value
                for key, value in config.model_dump().items()
                if key not in {"models", "pft_table", "model_dir", "n_jobs"}
            },
        )
