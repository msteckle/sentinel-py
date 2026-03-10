import logging
from rich.logging import RichHandler
import datetime as dt
from pathlib import Path


DEFAULT_LOG_DIR = Path.home() / ".sentinel-py" / "logs"
def get_logger(
    name: str = __name__,
    logpath: Path = None,
    verbose: bool = False,
) -> logging.Logger:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    # if no logpath provided, use default log dir/logfile
    if logpath is None:
        log_dir = DEFAULT_LOG_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        logfile = log_dir / f"sentinel_py_{timestamp}.log"
    # if logpath is provided
    else:
        # and logpath is an existing directory, create logfile inside it
        logpath = Path(logpath)
        if logpath.exists() and logpath.is_dir():
            logpath.mkdir(parents=True, exist_ok=True)
            logfile = logpath / f"sentinel_py_{timestamp}.log"
        # otherwise treat logpath as a file path (even if it doesn't exist yet)
        else:
            parent = logpath.parent
            if parent != Path("."):
                parent.mkdir(parents=True, exist_ok=True)
            prefix = logpath.name
            logfile = parent / f"{prefix}_{timestamp}.log"

    # create the logger
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s:%(name)s:%(levelname)8s:%(funcName)s:%(message)s"
    )

    # Console handler (only warnings unless verbose)
    sh = RichHandler(level=logging.DEBUG if verbose else logging.WARNING)
    sh.setFormatter(formatter)

    # File handler
    fh = logging.FileHandler(logfile, mode="w")
    fh.setLevel(logging.DEBUG if verbose else logging.INFO)
    fh.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger
