"""PFT cover tables and random-forest fitting for the PFT cover processor.

The loader reads the PFT cover table and splits it per PFT for the plots that a point
pipeline sampled. One random forest is fitted per PFT, and each model is saved with a
manifest so a rerun with the same recipe skips fitting.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sentinel_py.cache import write_json_atomic, write_parquet_atomic
from sentinel_py.enums import PFT_COVER_NODATA, MissingCoverPolicy

DEFAULT_ID_COLUMN = "site_visit_id"
QA_FLAG_COLUMN = "qa_review_flag"
QA_FLAG_REJECT = "yes"
MIN_TRAINING_ROWS = 2
MANIFEST_NAME = "manifest.json"
FEATURES_NAME = "training_features.parquet"
OOB_NAME = "oob_predictions.parquet"
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


def features_digest(features: pd.DataFrame) -> str:
    """Return a digest of a feature table that ignores its row order.

    Parameters
    ----------
    features
        Table with ``aoi_id`` and one column per feature band.

    Returns
    -------
    str
        SHA-256 hex digest of the sorted rows, so that changed feature values (not
        only changed scenes) change the recipe ID of a fit.
    """
    ordered = features.sort_values("aoi_id").reset_index(drop=True)
    hashes = pd.util.hash_pandas_object(ordered, index=False).to_numpy()
    return hashlib.sha256(hashes.tobytes()).hexdigest()


########################################################################################
# Cover table loader
########################################################################################


def load_pft_cover(
    pft_table_path: Path,
    plot_ids: Sequence[str],
    *,
    pfts: Sequence[str],
    id_column: str = DEFAULT_ID_COLUMN,
    drop_qa_flagged: bool = True,
    missing_cover: MissingCoverPolicy | str = MissingCoverPolicy.zero,
    logger: logging.Logger | None = None,
) -> dict[str, pd.DataFrame]:
    """Select the sampled plots from the PFT cover table and split them per PFT.

    Plots flagged ``yes`` in ``qa_review_flag`` are dropped when ``drop_qa_flagged``
    is true, then plots with no measured value for any requested PFT are dropped.
    Cover values equal to ``-9999`` are unmeasured: ``zero`` treats them as 0 and
    ``drop`` removes the plot from that PFT's table only.

    Parameters
    ----------
    pft_table_path
        CSV with ``id_column``, ``qa_review_flag`` and one column per PFT.
    plot_ids
        Plot identifiers that were sampled (the point AOI's ``aoi_id`` values); other
        rows of the table are ignored.
    pfts
        PFT column names to build tables for.
    id_column
        Plot identifier column of the table.
    drop_qa_flagged
        Drop plots whose ``qa_review_flag`` is ``yes``.
    missing_cover
        Policy for ``-9999`` cover values.
    logger
        Logger for row counts; defaults to the module logger.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Per PFT, a frame with columns ``aoi_id`` and ``cover`` (percent, 0-100).

    Raises
    ------
    ValueError
        If columns are missing, IDs repeat, no plots remain, or a PFT keeps fewer
        than two training rows.
    """
    logger = logger or _LOGGER
    policy = MissingCoverPolicy(missing_cover)
    pfts = list(pfts)
    required = [id_column, QA_FLAG_COLUMN, *pfts]
    cover = pd.read_csv(pft_table_path, dtype={id_column: str})
    missing = [column for column in required if column not in cover.columns]
    if missing:
        raise ValueError(
            f"PFT table {pft_table_path} is missing columns: {', '.join(missing)}."
        )
    if cover[id_column].duplicated().any():
        raise ValueError(f"PFT table {pft_table_path} repeats values of {id_column!r}.")

    table = cover[required][cover[id_column].isin(set(plot_ids))]
    joined = len(table)
    if drop_qa_flagged:
        flagged = table[QA_FLAG_COLUMN].astype(str).str.strip().str.lower()
        table = table[flagged != QA_FLAG_REJECT]
    values = table[pfts].astype(float).where(lambda frame: frame != PFT_COVER_NODATA)
    table = table.loc[values.notna().any(axis=1)]
    values = values.loc[table.index]
    logger.info(
        "PFT training plots: %d sampled, %d found in the cover table, %d after "
        "QA/empty filtering (missing_cover=%s).",
        len(set(plot_ids)),
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
    sampled feature table and the per-plot observed and out-of-bag cover
    (``oob_predictions.parquet``) are saved beside the models.

    Parameters
    ----------
    tables
        Per-PFT frames from :func:`load_pft_cover`.
    features
        Table with ``aoi_id`` and one column per feature band, one row per plot.
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
    oob_columns: list[pd.DataFrame] = []
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
        oob_columns.append(
            pd.DataFrame(
                {
                    f"{pft}_observed": merged["cover"].to_numpy()[usable],
                    f"{pft}_oob": np.rint(np.clip(forest.oob_prediction_, 0, 100)),
                },
                index=merged["aoi_id"].to_numpy()[usable],
            )
        )
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
    oob_table = pd.concat(oob_columns, axis=1).rename_axis("aoi_id").reset_index()
    for column in oob_table.columns[1:]:
        if column.endswith("_oob"):
            oob_table[column] = oob_table[column].astype("Int16")
    write_parquet_atomic(oob_table, model_dir / OOB_NAME)
    manifest = {"features": names, "pfts": list(tables), "models": summary}
    write_json_atomic(model_dir / MANIFEST_NAME, manifest)
    return manifest
