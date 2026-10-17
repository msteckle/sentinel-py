"""Training data, feature sampling, and model fitting for the PFT cover forests.

The loader joins the selected field plots with the PFT cover table. Feature values at
the plots are read from the lazy upstream cubes of the plots' own AOI boxes, one
random forest is fitted per PFT, and each model is saved with a manifest so a rerun
with the same recipe skips sampling and fitting.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from affine import Affine
from pyproj import CRS, Transformer

from sentinel_py.cache import write_json_atomic, write_parquet_atomic
from sentinel_py.enums import PFT_COVER_NODATA, MissingCoverPolicy

DEFAULT_ID_COLUMN = "site_visit_id"
QA_FLAG_COLUMN = "qa_review_flag"
QA_FLAG_REJECT = "yes"
LATITUDE_COLUMN = "latitude"
LONGITUDE_COLUMN = "longitude"
MIN_TRAINING_ROWS = 2
MANIFEST_NAME = "manifest.json"
FEATURES_NAME = "training_features.parquet"
MODEL_SUFFIX = ".joblib"
TOP_FEATURES = 5

_LOGGER = logging.getLogger(__name__)


########################################################################################
# Table helpers
########################################################################################


def file_digest(path: Path) -> str:
    """Return the SHA-256 hex digest of a file's content.

    Parameters
    ----------
    path
        File to hash.

    Returns
    -------
    str
        Hex digest, used so a changed training table changes the recipe ID.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path, required: Sequence[str], label: str) -> pd.DataFrame:
    """Read a CSV and check that it has the required columns and unique plot IDs.

    Parameters
    ----------
    path
        CSV file to read.
    required
        Column names that must be present; the first is the plot ID column.
    label
        Human-readable table name used in error messages.

    Returns
    -------
    pandas.DataFrame
        The table, with the ID column read as text.

    Raises
    ------
    ValueError
        If the file lacks required columns or repeats a plot ID.
    """
    table = pd.read_csv(path, dtype={required[0]: str})
    missing = [column for column in required if column not in table.columns]
    if missing:
        raise ValueError(f"{label} {path} is missing columns: {', '.join(missing)}.")
    if table[required[0]].duplicated().any():
        raise ValueError(f"{label} {path} repeats values of {required[0]!r}.")
    return table


########################################################################################
# Public loader
########################################################################################


def load_pft_training(
    training_path: Path,
    pft_table_path: Path,
    *,
    pfts: Sequence[str],
    id_column: str = DEFAULT_ID_COLUMN,
    drop_qa_flagged: bool = True,
    missing_cover: MissingCoverPolicy | str = MissingCoverPolicy.zero,
    logger: logging.Logger | None = None,
) -> dict[str, pd.DataFrame]:
    """Join selected plots with the PFT cover table and split them per PFT.

    The training CSV selects plots and supplies their coordinates; the PFT cover
    table supplies the cover values and the ``qa_review_flag``. Plots flagged
    ``yes`` are dropped when ``drop_qa_flagged`` is true, then plots with no
    measured value for any requested PFT are dropped. Cover values equal to
    ``-9999`` are unmeasured: ``zero`` treats them as 0 and ``drop`` removes the
    plot from that PFT's table only.

    Parameters
    ----------
    training_path
        CSV with ``id_column``, ``latitude`` and ``longitude`` for the plots to use.
    pft_table_path
        CSV with ``id_column``, ``qa_review_flag`` and one column per PFT.
    pfts
        PFT column names to build tables for.
    id_column
        Plot identifier shared by both tables (the pipeline ``aoi_id``).
    drop_qa_flagged
        Drop plots whose ``qa_review_flag`` is ``yes``.
    missing_cover
        Policy for ``-9999`` cover values.
    logger
        Logger for row counts; defaults to the module logger.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Per PFT, a frame with columns ``aoi_id``, ``latitude``, ``longitude`` and
        ``cover`` (percent, 0-100).

    Raises
    ------
    ValueError
        If columns are missing, IDs repeat, no plots remain, or a PFT keeps fewer
        than two training rows.
    """
    logger = logger or _LOGGER
    policy = MissingCoverPolicy(missing_cover)
    pfts = list(pfts)
    plots = _read_csv(
        training_path, [id_column, LATITUDE_COLUMN, LONGITUDE_COLUMN], "Training table"
    )
    cover = _read_csv(pft_table_path, [id_column, QA_FLAG_COLUMN, *pfts], "PFT table")

    table = plots[[id_column, LATITUDE_COLUMN, LONGITUDE_COLUMN]].merge(
        cover[[id_column, QA_FLAG_COLUMN, *pfts]], on=id_column, how="inner"
    )
    selected = len(plots)
    joined = len(table)
    if drop_qa_flagged:
        flagged = table[QA_FLAG_COLUMN].astype(str).str.strip().str.lower()
        table = table[flagged != QA_FLAG_REJECT]
    values = table[pfts].astype(float).where(lambda frame: frame != PFT_COVER_NODATA)
    table = table.loc[values.notna().any(axis=1)]
    values = values.loc[table.index]
    logger.info(
        "PFT training plots: %d selected, %d with cover data, %d after QA/empty "
        "filtering (missing_cover=%s).",
        selected,
        joined,
        len(table),
        policy.value,
    )
    if table.empty:
        raise ValueError("No training plots remain after joining and filtering.")

    result: dict[str, pd.DataFrame] = {}
    for pft in pfts:
        frame = pd.DataFrame(
            {
                "aoi_id": table[id_column].to_numpy(),
                LATITUDE_COLUMN: table[LATITUDE_COLUMN].to_numpy(dtype=float),
                LONGITUDE_COLUMN: table[LONGITUDE_COLUMN].to_numpy(dtype=float),
                "cover": values[pft].to_numpy(dtype=float),
            }
        )
        if policy is MissingCoverPolicy.zero:
            frame["cover"] = np.nan_to_num(frame["cover"].to_numpy(), nan=0.0)
        else:
            frame = frame.dropna(subset=["cover"])
        if len(frame) < MIN_TRAINING_ROWS:
            raise ValueError(
                f"PFT {pft!r} has {len(frame)} training row(s); at least "
                f"{MIN_TRAINING_ROWS} are required."
            )
        result[pft] = frame.reset_index(drop=True)
    return result


########################################################################################
# Feature sampling at the training points
########################################################################################


def point_window(
    affine: Affine, shape: tuple[int, int], x: float, y: float, radius_m: float
) -> tuple[slice, slice, np.ndarray] | None:
    """Locate a point in a raster and the pixels within ``radius_m`` of it.

    Parameters
    ----------
    affine
        Pixel-to-CRS transform of the raster.
    shape
        ``(height, width)`` of the raster.
    x, y
        Point coordinates in the raster CRS (metres for a projected CRS).
    radius_m
        Neighbourhood radius; 0 selects only the pixel containing the point.

    Returns
    -------
    tuple[slice, slice, numpy.ndarray] or None
        Row slice, column slice, and a boolean mask over that window selecting
        pixels whose centres lie within the radius (the containing pixel is always
        selected); ``None`` if the point falls outside the raster.
    """
    col_f, row_f = ~affine @ (x, y)
    row, col = int(np.floor(row_f)), int(np.floor(col_f))
    height, width = shape
    if not (0 <= row < height and 0 <= col < width):
        return None
    half = int(np.ceil(radius_m / min(abs(affine.a), abs(affine.e))))
    rows = slice(max(row - half, 0), min(row + half + 1, height))
    cols = slice(max(col - half, 0), min(col + half + 1, width))
    row_index, col_index = np.mgrid[rows, cols]
    center_x, center_y = affine @ (col_index + 0.5, row_index + 0.5)
    mask = (center_x - x) ** 2 + (center_y - y) ** 2 <= radius_m**2
    mask[row - rows.start, col - cols.start] = True
    return rows, cols, mask


def _window_median(block: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Return the per-band NaN-aware median of the masked pixels of a window.

    Parameters
    ----------
    block
        Feature window shaped ``(band, y, x)``.
    mask
        Boolean ``(y, x)`` selection of the pixels to summarise.

    Returns
    -------
    numpy.ndarray
        ``float32`` vector with one value per band; NaN where no pixel is valid.
    """
    values = np.asarray(block, dtype=np.float32)[:, mask]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.nanmedian(values, axis=1).astype(np.float32)


def sample_features(
    datasets: Mapping[tuple[str, str], Any],
    variable: str,
    points: pd.DataFrame,
    *,
    radius_m: float,
    execution: Any,
    report: Callable[[int, int], None] | None = None,
) -> tuple[pd.DataFrame, int]:
    """Read the feature vector at each training point from its AOI unit's cube.

    Parameters
    ----------
    datasets
        Upstream cubes keyed by ``(aoi_id, component_id)``; each carries ``crs`` and
        ``transform`` attributes and a single time step.
    variable
        Banded cube variable holding the features.
    points
        Unique plots with ``aoi_id``, ``latitude`` and ``longitude`` columns.
    radius_m
        Neighbourhood radius; the median of the pixels within it is used.
    execution
        Pipeline execution configuration used to compute the sampling tasks.
    report
        Optional ``(completed, total)`` progress callback.

    Returns
    -------
    tuple[pandas.DataFrame, int]
        A table with ``aoi_id`` plus one column per feature band, and the number of
        plots that had no AOI unit containing them.
    """
    import dask
    from sentinel_py.pipeline.execution import compute_dask_tasks

    units: dict[str, list[tuple[str, str]]] = {}
    for key in datasets:
        units.setdefault(key[0], []).append(key)
    transformers: dict[str, Transformer] = {}
    tasks: list[Any] = []
    owners: list[str] = []
    for aoi_id, lat, lon in zip(
        points["aoi_id"], points[LATITUDE_COLUMN], points[LONGITUDE_COLUMN]
    ):
        for key in units.get(aoi_id, ()):
            dataset = datasets[key]
            crs = str(dataset.attrs["crs"])
            if crs not in transformers:
                transformers[crs] = Transformer.from_crs(
                    "EPSG:4326", CRS.from_user_input(crs), always_xy=True
                )
            x, y = transformers[crs].transform(lon, lat)
            data = dataset[variable].data
            window = point_window(
                Affine(*dataset.attrs["transform"]),
                (data.shape[2], data.shape[3]),
                x,
                y,
                radius_m,
            )
            if window is not None:
                rows, cols, mask = window
                tasks.append(
                    dask.delayed(_window_median)(data[0, :, rows, cols], mask)
                )
                owners.append(aoi_id)
                break
    names = [str(band) for band in next(iter(datasets.values())).band.values]
    completed = 0

    def on_result(_: Any) -> None:
        """Count one finished sampling task."""
        nonlocal completed
        completed += 1
        if report is not None:
            report(completed, len(tasks))

    if report is not None:
        report(0, len(tasks))
    computed = compute_dask_tasks(tasks, execution, progress_callback=on_result)
    features = pd.DataFrame(
        np.vstack(computed) if computed else np.empty((0, len(names)), np.float32),
        columns=names,
    )
    features.insert(0, "aoi_id", owners)
    return features, len(points) - len(owners)


########################################################################################
# Model fitting and persistence
########################################################################################


@dataclass(frozen=True)
class ForestParams:
    """Random-forest hyperparameters shared by every PFT model."""

    n_estimators: int
    max_depth: int | None
    max_features: int | float | str | None
    seed: int
    n_jobs: int


def model_path(model_dir: Path, pft: str) -> Path:
    """Return where the fitted model of one PFT is stored in ``model_dir``."""
    return model_dir / f"{pft}{MODEL_SUFFIX}"


def read_manifest(model_dir: Path) -> dict[str, Any] | None:
    """Return the manifest saved in ``model_dir``, or ``None`` if there is none."""
    manifest = model_dir / MANIFEST_NAME
    return json.loads(manifest.read_text()) if manifest.is_file() else None


def load_manifest(model_dir: Path, pfts: Sequence[str]) -> dict[str, Any] | None:
    """Return the manifest of a complete model directory, or ``None``.

    Parameters
    ----------
    model_dir
        Directory of one recipe's models.
    pfts
        PFTs whose model files must all exist.

    Returns
    -------
    dict or None
        The saved manifest (features and per-PFT fit summaries) when the manifest
        and every requested model file exist, otherwise ``None``.
    """
    if not all(model_path(model_dir, pft).is_file() for pft in pfts):
        return None
    return read_manifest(model_dir)


def fit_pft_models(
    tables: Mapping[str, pd.DataFrame],
    features: pd.DataFrame,
    *,
    params: ForestParams,
    model_dir: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Fit and save one random forest per PFT from sampled feature values.

    Plots whose feature vector has any non-finite value are left out, as the
    original workflow dropped plots with missing satellite features. The manifest is
    written last, so an interrupted fit is never mistaken for a complete one. The
    sampled feature table is saved beside the models so the plots' features can be
    inspected or reused without resampling.

    Parameters
    ----------
    tables
        Per-PFT frames from :func:`load_pft_training`.
    features
        Output of :func:`sample_features`.
    params
        Forest hyperparameters.
    model_dir
        Directory receiving ``<pft>.joblib`` files and the manifest.
    logger
        Logger for per-PFT fit summaries.

    Returns
    -------
    dict
        The manifest: feature names, PFTs, and per-PFT training size, out-of-bag
        R2 and most important features.

    Raises
    ------
    RuntimeError
        If scikit-learn is not installed.
    ValueError
        If ``max_features`` exceeds the feature count or a PFT keeps too few rows.
    """
    try:
        import joblib
        from sklearn.ensemble import RandomForestRegressor
    except ImportError as error:
        raise RuntimeError(
            "pavc_gridded_rf requires scikit-learn; install it with the 'ml' extra"
        ) from error
    names = [column for column in features.columns if column != "aoi_id"]
    if isinstance(params.max_features, int) and params.max_features > len(names):
        raise ValueError(
            f"max_features={params.max_features} exceeds the {len(names)} input "
            "feature bands"
        )
    model_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {}
    for pft, table in tables.items():
        merged = table.merge(features, on="aoi_id", how="inner")
        matrix = merged[names].to_numpy(dtype=np.float32)
        usable = np.isfinite(matrix).all(axis=1)
        if usable.sum() < MIN_TRAINING_ROWS:
            raise ValueError(
                f"PFT {pft!r} has {int(usable.sum())} plot(s) with complete feature "
                f"values out of {len(merged)} sampled inside the AOI boxes; at least "
                f"{MIN_TRAINING_ROWS} are required"
            )
        if not usable.all():
            logger.warning(
                "%s: %d plot(s) have missing features (for example %s).",
                pft,
                int((~usable).sum()),
                ", ".join(merged["aoi_id"][~usable].head(3)),
            )
        forest = RandomForestRegressor(
            n_estimators=params.n_estimators,
            max_depth=params.max_depth,
            max_features=params.max_features,
            random_state=params.seed,
            n_jobs=params.n_jobs,
            oob_score=True,
        )
        with warnings.catch_warnings():
            # Tiny local test sets leave some rows out of every tree's bag.
            warnings.filterwarnings("ignore", message="Some inputs do not have OOB")
            forest.fit(matrix[usable], merged["cover"].to_numpy()[usable])
        part = model_path(model_dir, pft).with_suffix(".part")
        try:
            joblib.dump(forest, part)
            os.replace(part, model_path(model_dir, pft))
        finally:
            part.unlink(missing_ok=True)
        oob = float(getattr(forest, "oob_score_", np.nan))
        top = np.argsort(forest.feature_importances_)[::-1][:TOP_FEATURES]
        summary[pft] = {
            "n_train": int(usable.sum()),
            "oob_r2": oob if np.isfinite(oob) else None,
            "top_features": [names[index] for index in top],
        }
        logger.info(
            "Fitted %s: %d plots (%d dropped for incomplete features), OOB R2=%s, "
            "top features: %s",
            pft,
            summary[pft]["n_train"],
            len(table) - summary[pft]["n_train"],
            "n/a" if summary[pft]["oob_r2"] is None else f"{oob:.3f}",
            ", ".join(summary[pft]["top_features"]),
        )
    write_parquet_atomic(features, model_dir / FEATURES_NAME)
    manifest = {"features": names, "pfts": list(tables), "models": summary}
    write_json_atomic(model_dir / MANIFEST_NAME, manifest)
    return manifest
