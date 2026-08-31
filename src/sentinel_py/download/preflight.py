"""Storage information shared by provider download implementations."""

from dataclasses import dataclass


@dataclass(frozen=True)
class DownloadStorageSummary:
    """Known storage footprint for a resolved download plan."""

    asset_count: int
    known_total_bytes: int
    known_additional_bytes: int
    unknown_size_assets: int = 0
