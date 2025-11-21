import datetime as dt
from pathlib import Path
import re

def seasonal_date_ranges(
    start_year: int,
    end_year: int,
    start_month: int,
    start_day: int,
    end_month: int,
    end_day: int,
) -> list[tuple[str, str]]:
    """
    Build a list of (start_iso, end_iso) date windows for each year in [start_year, end_year].

    Example:
        seasonal_date_ranges(2019, 2024, 6, 1, 8, 31)
        -> [
             ("2019-06-01T00:00:00Z", "2019-08-31T23:59:59Z"),
             ...,
             ("2024-06-01T00:00:00Z", "2024-08-31T23:59:59Z"),
           ]
    """
    ranges: list[tuple[str, str]] = []
    for year in range(start_year, end_year + 1):
        start = dt.datetime(year, start_month, start_day, 0, 0, 0)
        end = dt.datetime(year, end_month, end_day, 23, 59, 59)
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        ranges.append((start_iso, end_iso))
    return ranges


def parse_years(years_str: str) -> set[int]:
    """Parse a space-separated years string like '2020 2021' into a set of ints."""
    years_str = years_str.strip()
    if not years_str:
        return set()
    return {int(y) for y in years_str.split()}


def in_season_window(
    d: dt.date,
    start_md: tuple[int, int],
    end_md: tuple[int, int],
) -> bool:
    """Check if date d is within the MM-DD window, allowing wrap-around."""
    md = (d.month, d.day)
    if end_md >= start_md:
        # e.g. 06-01 → 08-31
        return start_md <= md <= end_md
    else:
        # wrap-around, e.g. 11-01 → 02-28
        return md >= start_md or md <= end_md
    

def extract_s2_acq_date(band_path: Path) -> dt.date | None:
    """
    Extract acquisition date from Sentinel-2 filename or SAFE name.
    Examples:
      S2B_MSIL2A_20200616T213529_...
      T06WVB_20200616T213529_B03_20m.jp2
    """
    s = band_path.name
    m = re.search(r"_([0-9]{8})T[0-9]{6}", s)
    if not m:
        # fallback: try the parent .SAFE dir name
        for parent in band_path.parents:
            if parent.name.endswith(".SAFE"):
                m2 = re.search(r"_([0-9]{8})T[0-9]{6}", parent.name)
                if m2:
                    m = m2
                    break
        if not m:
            return None

    return dt.datetime.strptime(m.group(1), "%Y%m%d").date()