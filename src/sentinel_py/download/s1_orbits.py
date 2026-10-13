"""Stage Sentinel-1 precise and restituted orbit files for offline processing.

SNAP's ``Apply-Orbit-File`` operator can download orbit files itself, but compute
nodes are frequently offline. This module resolves one orbit file per Sentinel-1
GRD/SLC scene while the network is available (the download stage), stores the files
under ``<out_dir>/orbits`` and records the scene-to-orbit assignment in
``<out_dir>/.sentinel-py/s1_orbits.parquet`` for later processing stages.
"""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from sentinel_py.cache import REGISTRY
from sentinel_py.download.base import make_progress, part_path, retry_call
from sentinel_py.enums import DownloadStatus, S1OrbitPolicy

# ASF mirrors the ESA/Copernicus POD orbit archive over plain HTTP without
# credentials. Directory listings are nginx autoindex pages.
ORBIT_BASE_URLS = {
    "POEORB": "https://s1qc.asf.alaska.edu/aux_poeorb/",
    "RESORB": "https://s1qc.asf.alaska.edu/aux_resorb/",
}
ORBIT_DIRNAME = "orbits"
ORBIT_STATE_RELPATH = Path(REGISTRY["s1_orbits"].relpath)
ORBIT_STATE_COLUMNS = [
    "scene",
    "platform",
    "scene_start",
    "scene_stop",
    "orbit_type",
    "orbit_file",
    "orbit_path",
    "valid_start",
    "valid_stop",
    "status",
    "error",
    "checked_at",
]
# Orbit state vectors are consumed only by calibration-capable products.
ORBIT_PRODUCT_TYPES = ("GRD", "SLC")

_SCENE_RE = re.compile(
    r"^(?P<platform>S1[A-D])_(?P<mode>[A-Z0-9]{2})_(?P<product>[A-Z0-9_]{4})_"
    r"(?P<pols>\d[A-Z]{3})_(?P<start>\d{8}T\d{6})_(?P<stop>\d{8}T\d{6})_"
)
_ORBIT_RE = re.compile(
    r"^(?P<platform>S1[A-D])_OPER_AUX_(?P<kind>POEORB|RESORB)_OPOD_"
    r"(?P<created>\d{8}T\d{6})_V(?P<vstart>\d{8}T\d{6})_(?P<vstop>\d{8}T\d{6})\.EOF$"
)
_HREF_RE = re.compile(r'href="(S1[A-D]_OPER_AUX_(?:POEORB|RESORB)_[^"/]+\.EOF)"')
_TIME_FORMAT = "%Y%m%dT%H%M%S"


@dataclass(frozen=True)
class SceneTimes:
    """Platform and acquisition window parsed from a Sentinel-1 scene name."""

    scene: str
    platform: str
    product: str
    start: datetime
    stop: datetime


@dataclass(frozen=True)
class OrbitFile:
    """One remote orbit file and the validity window encoded in its name."""

    name: str
    kind: str
    platform: str
    created: datetime
    valid_start: datetime
    valid_stop: datetime

    @property
    def url(self) -> str:
        """Return the file's download URL on the ASF mirror."""
        return ORBIT_BASE_URLS[self.kind] + self.name

    def covers(self, start: datetime, stop: datetime) -> bool:
        """Return whether the file's validity window spans an acquisition."""
        return self.valid_start <= start and stop <= self.valid_stop


@dataclass(frozen=True)
class OrbitStagingSummary:
    """Outcome of one orbit-staging invocation."""

    staged: int = 0
    reused: int = 0
    downloaded_files: int = 0
    missing: tuple[str, ...] = ()
    failed: int = 0
    messages: list[str] = field(default_factory=list)


def _utc(value: str) -> datetime:
    """Parse an orbit-listing timestamp as a UTC datetime."""
    return datetime.strptime(value, _TIME_FORMAT).replace(tzinfo=timezone.utc)


def parse_scene_name(name: str) -> SceneTimes | None:
    """Parse platform and acquisition times from a Sentinel-1 scene name.

    Parameters
    ----------
    name : str
        Scene or granule name, with or without a ``.zip``/``.SAFE`` suffix.

    Returns
    -------
    SceneTimes | None
        Parsed values, or ``None`` when the name is not a Sentinel-1 product name.
    """
    stem = Path(str(name)).name
    for suffix in (".zip", ".SAFE"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    match = _SCENE_RE.match(stem)
    if match is None:
        return None
    return SceneTimes(
        scene=stem,
        platform=match["platform"],
        product=match["product"],
        start=_utc(match["start"]),
        stop=_utc(match["stop"]),
    )


def parse_orbit_filename(name: str) -> OrbitFile | None:
    """Parse an ESA POD orbit filename, returning ``None`` for other names."""
    match = _ORBIT_RE.match(name)
    if match is None:
        return None
    return OrbitFile(
        name=name,
        kind=match["kind"],
        platform=match["platform"],
        created=_utc(match["created"]),
        valid_start=_utc(match["vstart"]),
        valid_stop=_utc(match["vstop"]),
    )


def select_orbit(
    candidates: list[OrbitFile], platform: str, start: datetime, stop: datetime
) -> OrbitFile | None:
    """Return the most recently produced orbit file covering an acquisition."""
    covering = [
        orbit
        for orbit in candidates
        if orbit.platform == platform and orbit.covers(start, stop)
    ]
    return max(covering, key=lambda orbit: orbit.created, default=None)


def _kinds_for_policy(policy: S1OrbitPolicy) -> tuple[str, ...]:
    """Return the orbit kinds to stage for a policy, in preference order."""
    return {
        S1OrbitPolicy.precise: ("POEORB",),
        S1OrbitPolicy.restituted: ("RESORB",),
        S1OrbitPolicy.precise_or_restituted: ("POEORB", "RESORB"),
        S1OrbitPolicy.none: (),
    }[policy]


def list_remote_orbits(
    kind: str, session: requests.Session, *, timeout: float = 60.0
) -> list[OrbitFile]:
    """List every orbit file of one type on the ASF mirror."""
    response = session.get(ORBIT_BASE_URLS[kind], timeout=timeout)
    response.raise_for_status()
    orbits = (parse_orbit_filename(name) for name in _HREF_RE.findall(response.text))
    return [orbit for orbit in orbits if orbit is not None and orbit.kind == kind]


def _relative_orbit_path(orbit: OrbitFile) -> Path:
    """Return an orbit file's path relative to the output directory."""
    return Path(ORBIT_DIRNAME) / orbit.kind / orbit.platform / orbit.name


def _download_orbit(
    orbit: OrbitFile,
    out_dir: Path,
    session: requests.Session,
    *,
    retries: int,
    retry_backoff: float,
    logger: logging.Logger,
) -> Path:
    """Download one orbit file to a ``.part`` sibling and atomically rename it."""
    target = out_dir / _relative_orbit_path(orbit)
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = part_path(target.parent, target.name)

    def fetch() -> Path:
        """Stream one orbit file and verify it is a non-empty XML document."""
        response = session.get(orbit.url, timeout=120, stream=True)
        response.raise_for_status()
        with partial.open("wb") as stream:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                stream.write(chunk)
        # Orbit files are small XML documents; reject empty or HTML error bodies.
        with partial.open("rb") as stream:
            head = stream.read(5)
        if partial.stat().st_size == 0 or head != b"<?xml":
            raise OSError(f"invalid orbit file content for {orbit.name}")
        os.replace(partial, target)
        return target

    def is_retryable(error: Exception) -> bool:
        """Retry network/IO errors unless the server reported a client error."""
        if not isinstance(error, (requests.RequestException, OSError)):
            return False
        status = getattr(getattr(error, "response", None), "status_code", None)
        return status is None or status == 429 or status >= 500

    def log_retry(error: Exception, attempt: int, delay: float) -> None:
        """Log a retryable orbit download failure."""
        logger.warning(
            "Retrying orbit %s after attempt %d/%d failed: %s; waiting %.1fs",
            orbit.name,
            attempt + 1,
            retries + 1,
            error,
            delay,
        )

    return retry_call(
        fetch,
        retries=retries,
        backoff=retry_backoff,
        is_retryable=is_retryable,
        on_retry=log_retry,
        cleanup=lambda: partial.unlink(missing_ok=True),
    )


def _product_scene_names(products: pd.DataFrame) -> list[str]:
    """Return unique scene names for manifest rows that need orbit files."""
    names: list[str] = []
    for product in products.to_dict("records"):
        granule = product.get("granule")
        source = (
            granule
            if granule is not None and not pd.isna(granule)
            else Path(str(product.get("url", ""))).name
        )
        scene = parse_scene_name(str(source))
        if scene is not None and scene.product.startswith(ORBIT_PRODUCT_TYPES):
            names.append(scene.scene)
    return list(dict.fromkeys(names))


def stage_orbits(
    products: pd.DataFrame,
    out_dir: Path,
    policy: S1OrbitPolicy,
    *,
    session: requests.Session | None = None,
    processes: int = 4,
    retries: int = 3,
    retry_backoff: float = 2.0,
    logger: logging.Logger | None = None,
) -> OrbitStagingSummary:
    """Resolve and download one orbit file per GRD/SLC scene in an ASF manifest.

    Existing assignments are reused. Scenes assigned a restituted orbit are
    re-resolved when ``policy`` permits precise orbits, so repeating a download
    upgrades recent scenes once their precise orbit is published. Scenes with no
    covering orbit are reported as missing rather than treated as errors.

    Parameters
    ----------
    products : pandas.DataFrame
        ASF manifest rows; ``granule`` or ``url`` identifies each scene.
    out_dir : pathlib.Path
        Download directory; orbits are stored in ``<out_dir>/orbits``.
    policy : S1OrbitPolicy
        Which orbit file types may be used. ``none`` disables staging.
    session : requests.Session, optional
        HTTP session (injectable for tests).
    processes : int
        Parallel orbit downloads.
    retries : int
        Retry attempts per file after a transient failure.
    retry_backoff : float
        Base delay in seconds for exponential retry backoff.
    logger : logging.Logger, optional
        Logger for decisions and failures.

    Returns
    -------
    OrbitStagingSummary
        Counts, missing scenes, and user-facing messages.
    """
    logger = logger or logging.getLogger(__name__)
    out_dir = Path(out_dir)
    kinds = _kinds_for_policy(policy)
    scenes = _product_scene_names(products)
    if not kinds or not scenes:
        return OrbitStagingSummary(
            messages=["Orbit staging: nothing to do."]
            if kinds
            else ["Orbit staging disabled."]
        )

    state_store = REGISTRY["s1_orbits"].store(out_dir)
    state_path = state_store.path
    state = state_store.read()
    if state.empty:
        state = pd.DataFrame(columns=ORBIT_STATE_COLUMNS)
    previous = (
        state.drop_duplicates(subset=["scene"], keep="last")
        .set_index("scene")
        .to_dict("index")
        if not state.empty
        else {}
    )

    reused: dict[str, dict[str, Any]] = {}
    pending: list[SceneTimes] = []
    # Scenes already holding a restituted orbit only look for a precise upgrade.
    upgrade_only: set[str] = set()
    for scene in scenes:
        row = previous.get(scene)
        usable = (
            row is not None
            and row.get("status") == DownloadStatus.staged
            and row.get("orbit_type") in kinds
            and (out_dir / str(row.get("orbit_path"))).is_file()
        )
        # Allow a precise orbit to replace a restituted one when now permitted.
        upgradable = (
            usable and row["orbit_type"] == "RESORB" and "POEORB" in kinds
        )
        if usable and not upgradable:
            reused[scene] = row
            continue
        if upgradable:
            upgrade_only.add(scene)
        parsed = parse_scene_name(scene)
        assert parsed is not None
        pending.append(parsed)

    session = session or requests.Session()
    remote: dict[str, list[OrbitFile]] = {}
    assignments: dict[str, OrbitFile] = {}
    errors: dict[str, str] = {}
    try:
        for kind in kinds:
            unresolved = [
                s
                for s in pending
                if s.scene not in assignments
                and not (kind == "RESORB" and s.scene in upgrade_only)
            ]
            if not unresolved:
                break
            logger.info("Listing %s orbit files for %d scene(s)", kind, len(unresolved))
            remote[kind] = list_remote_orbits(kind, session)
            for scene in unresolved:
                orbit = select_orbit(
                    remote[kind], scene.platform, scene.start, scene.stop
                )
                if orbit is not None:
                    assignments[scene.scene] = orbit
    except requests.RequestException as error:
        logger.error("Could not list orbit files: %s", error)
        for scene in pending:
            if scene.scene not in assignments:
                errors[scene.scene] = f"orbit listing failed: {error}"

    # A previously staged restituted orbit remains valid if no precise one exists.
    for scene in pending:
        row = previous.get(scene.scene)
        if (
            scene.scene not in assignments
            and row is not None
            and row.get("status") == DownloadStatus.staged
            and (out_dir / str(row.get("orbit_path"))).is_file()
            and row.get("orbit_type") in kinds
        ):
            reused[scene.scene] = row

    unique = {orbit.name: orbit for orbit in assignments.values()}
    needed = {
        name: orbit
        for name, orbit in unique.items()
        if not (out_dir / _relative_orbit_path(orbit)).is_file()
    }
    failed_files: dict[str, str] = {}
    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    failed = 0

    def _scene_row(scene: SceneTimes) -> dict[str, Any]:
        """Build the state row for one scene once its orbit outcome is known."""
        nonlocal failed
        orbit = assignments.get(scene.scene)
        base = {
            "scene": scene.scene,
            "platform": scene.platform,
            "scene_start": scene.start.isoformat(),
            "scene_stop": scene.stop.isoformat(),
            "checked_at": now,
        }
        if orbit is not None and orbit.name not in failed_files:
            return {
                **base,
                "orbit_type": orbit.kind,
                "orbit_file": orbit.name,
                "orbit_path": _relative_orbit_path(orbit).as_posix(),
                "valid_start": orbit.valid_start.isoformat(),
                "valid_stop": orbit.valid_stop.isoformat(),
                "status": DownloadStatus.staged,
                "error": None,
            }
        error = errors.get(scene.scene) or (
            failed_files.get(orbit.name) if orbit is not None else None
        )
        if error is not None:
            failed += 1
        else:
            missing.append(scene.scene)
        return {
            **base,
            "orbit_type": None,
            "orbit_file": None,
            "orbit_path": None,
            "valid_start": None,
            "valid_stop": None,
            "status": (
                DownloadStatus.failed if error is not None else DownloadStatus.missing
            ),
            "error": error,
        }

    # Scenes whose orbit file is already on disk (or that have no orbit) are final
    # now; the rest are finalized as their orbit file finishes downloading.
    awaiting: dict[str, list[SceneTimes]] = {}
    immediate: list[SceneTimes] = []
    for scene in pending:
        if scene.scene in reused:
            continue
        orbit = assignments.get(scene.scene)
        if orbit is not None and orbit.name in needed:
            awaiting.setdefault(orbit.name, []).append(scene)
        else:
            immediate.append(scene)

    # Rows are checkpointed in batches, so an interrupted run keeps every scene
    # whose outcome was already known; the writer compacts on a clean exit.
    with state_store.writer(batch_rows=500, flush_seconds=30.0) as state_writer:

        def _record(scenes_to_record: list[SceneTimes]) -> None:
            """Add staged-orbit result rows to the shared state."""
            for scene in scenes_to_record:
                row = _scene_row(scene)
                rows.append(row)
                state_writer.add([row])

        _record(immediate)
        if needed:
            with make_progress(eta=False) as progress:
                task = progress.add_task(
                    "Downloading Sentinel-1 orbit files", total=len(needed)
                )
                with ThreadPoolExecutor(max_workers=max(1, processes)) as pool:
                    futures = {
                        pool.submit(
                            _download_orbit,
                            orbit,
                            out_dir,
                            session,
                            retries=retries,
                            retry_backoff=retry_backoff,
                            logger=logger,
                        ): name
                        for name, orbit in needed.items()
                    }
                    try:
                        for future in as_completed(futures):
                            name = futures[future]
                            try:
                                future.result()
                            except Exception as error:  # noqa: BLE001 - per file
                                logger.error("FAILED orbit %s: %s", name, error)
                                failed_files[name] = str(error)
                            _record(awaiting.pop(name, []))
                            progress.advance(task)
                    except BaseException:
                        # Do not start queued downloads after an interrupt.
                        pool.shutdown(wait=False, cancel_futures=True)
                        raise

    staged_now = sum(1 for row in rows if row["status"] == DownloadStatus.staged)
    messages = [
        f"Orbit staging: {staged_now} scene(s) resolved, {len(reused)} reused, "
        f"{len(needed) - len(failed_files)} file(s) downloaded, "
        f"{len(missing)} without a published orbit, {failed} failed",
        f"Orbit status:     {state_path}",
    ]
    if missing:
        logger.warning(
            "No %s orbit published yet for %d scene(s); rerun the download later: %s",
            "/".join(kinds),
            len(missing),
            ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else ""),
        )
    return OrbitStagingSummary(
        staged=staged_now,
        reused=len(reused),
        downloaded_files=len(needed) - len(failed_files),
        missing=tuple(missing),
        failed=failed,
        messages=messages,
    )
