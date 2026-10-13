"""Persistent file logging plus Rich console output for CLI commands."""

import datetime as dt
import logging
from pathlib import Path
from typing import Optional

from rich.logging import RichHandler

from sentinel_py.cache import SENTINEL_PY_HOME

DEFAULT_LOG_DIR = SENTINEL_PY_HOME / "logs"


def get_logger(
    name: str = __name__,
    logpath: Optional[Path] = None,
    verbose: bool = False,
) -> logging.Logger:
    """Create a named logger that writes a timestamped file and a Rich console feed.

    Parameters
    ----------
    name : str
        Logger name; use a unique name per command because handlers are reset.
    logpath : pathlib.Path, optional
        Log file prefix, or an existing directory to place the log file in. Defaults
        to ``DEFAULT_LOG_DIR``.
    verbose : bool, default False
        Show INFO and DEBUG records on the console and in the file; otherwise the
        console shows warnings and the file records INFO and above.

    Returns
    -------
    logging.Logger
        Configured logger with console and file handlers.
    """
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
    fh.stream = open(logfile, "w", buffering=1)
    fh.setLevel(logging.DEBUG if verbose else logging.INFO)
    fh.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger
