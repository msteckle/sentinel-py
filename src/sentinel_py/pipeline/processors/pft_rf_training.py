"""Training-table preparation for the per-PFT random-forest cover processor."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd

from sentinel_py.enums import PFT_COVER_NODATA, MissingCoverPolicy

DEFAULT_ID_COLUMN = "site_visit_id"
QA_FLAG_COLUMN = "qa_review_flag"
QA_FLAG_REJECT = "yes"
LATITUDE_COLUMN = "latitude"
LONGITUDE_COLUMN = "longitude"
MIN_TRAINING_ROWS = 2

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
