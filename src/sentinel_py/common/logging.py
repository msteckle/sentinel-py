import logging
import datetime as dt
from pathlib import Path


DEFAULT_LOG_DIR = Path.home() / ".sentinel-py" / "logs"
def get_logger(
    logpath: Path = None,
    name: str = __name__,
    verbose: bool = False,
) -> logging.Logger:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    if logpath is None:
        log_dir = DEFAULT_LOG_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        logfile = log_dir / f"sentinel_py_{timestamp}.log"
    else:
        logpath = Path(logpath)
        if logpath.exists() and logpath.is_dir():
            logpath.mkdir(parents=True, exist_ok=True)
            logfile = logpath / f"sentinel_py_{timestamp}.log"
        else:
            parent = logpath.parent
            if parent != Path("."):
                parent.mkdir(parents=True, exist_ok=True)
            prefix = logpath.name
            logfile = parent / f"{prefix}_{timestamp}.log"

    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)   # let handlers filter

    formatter = logging.Formatter(
        "%(asctime)s:%(name)s:%(levelname)8s:%(funcName)s:%(message)s"
    )

    # Console handler (only warnings unless verbose)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG if verbose else logging.WARNING)
    sh.setFormatter(formatter)

    # File handler
    fh = logging.FileHandler(logfile, mode="w")
    fh.setLevel(logging.DEBUG if verbose else logging.INFO)
    fh.setFormatter(formatter)

    logger.addHandler(sh)
    logger.addHandler(fh)

    if not verbose:
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logger
