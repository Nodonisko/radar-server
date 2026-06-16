"""Shared logging setup for radar_server CLIs."""

from __future__ import annotations

import logging
import sys

_LOG_FORMAT_WITH_TIME = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
_LOG_FORMAT_NO_TIME = "%(levelname)s:%(name)s:%(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: int | str = logging.INFO) -> None:
    """Configure root logging. Timestamps are omitted on Linux (journald adds them)."""
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    if sys.platform == "darwin":
        logging.basicConfig(level=level, format=_LOG_FORMAT_WITH_TIME, datefmt=_LOG_DATEFMT)
    else:
        logging.basicConfig(level=level, format=_LOG_FORMAT_NO_TIME)
