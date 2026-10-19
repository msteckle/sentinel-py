"""Compare the baseline and grouped Sentinel-1 test outputs row by row.

Run from the examples directory after both ``s1_group_test.sbatch`` jobs finish:

    python s1_group_test_compare.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

OUTPUT = Path("../data/processed")
KEY = ["aoi_id", "product_id"]
BANDS = ["VV", "VH"]


def main() -> None:
    """Print row counts, missing pairs and value differences between the two runs."""
    baseline = pd.read_parquet(OUTPUT / "s1_group_test_baseline" / "samples.parquet")
    grouped = pd.read_parquet(OUTPUT / "s1_group_test_grouped" / "samples.parquet")
    print(f"rows: baseline={len(baseline)} grouped={len(grouped)}")

    merged = baseline[KEY + BANDS].merge(
        grouped[KEY + BANDS], on=KEY, how="outer", suffixes=("_base", "_group"), indicator=True
    )
    only_base = int((merged["_merge"] == "left_only").sum())
    only_group = int((merged["_merge"] == "right_only").sum())
    print(f"pairs only in baseline: {only_base}   only in grouped: {only_group}")

    both = merged[merged["_merge"] == "both"]
    for band in BANDS:
        a, b = both[f"{band}_base"], both[f"{band}_group"]
        nan_mismatch = int((a.isna() != b.isna()).sum())
        valid = a.notna() & b.notna()
        diff = (a[valid] - b[valid]).abs()
        relative = diff / a[valid].abs().clip(lower=1e-12)
        print(
            f"{band}: compared={int(valid.sum())} nan_mismatch={nan_mismatch} "
            f"max_abs_diff={diff.max():.3g} median_rel_diff={np.median(relative):.3g} "
            f"max_rel_diff={relative.max():.3g}"
        )
    print("Expect identical pairs and near-zero differences; large ones need a look.")


if __name__ == "__main__":
    main()
