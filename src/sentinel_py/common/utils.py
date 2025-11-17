from datetime import datetime

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
        start = datetime(year, start_month, start_day, 0, 0, 0)
        end = datetime(year, end_month, end_day, 23, 59, 59)
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        ranges.append((start_iso, end_iso))
    return ranges