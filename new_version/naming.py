"""Filename helpers."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

TIMESTAMP_PATTERN = re.compile(r"(\d{8})(\d{6})")


def extract_timestamp(name: str) -> Optional[datetime]:
    match = TIMESTAMP_PATTERN.search(name)
    if not match:
        return None
    date_part, time_part = match.groups()
    return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")


def timestamp_stub(ts: datetime) -> str:
    return ts.strftime("%Y%m%d_%H%M")


def background_filename(ts: datetime, forecast: bool = False, offset: Optional[int] = None) -> str:
    suffix = ""
    if forecast:
        suffix = f"_forecast_fct{offset:02d}" if offset is not None else "_forecast"
    return f"background_radar_{timestamp_stub(ts)}{suffix}_300.png"


def overlay_filename(ts: datetime, variant: str, forecast: bool = False, offset: Optional[int] = None) -> str:
    suffix = ""
    if forecast:
        suffix = f"_forecast_fct{offset:02d}" if offset is not None else "_forecast"
    return f"radar_{timestamp_stub(ts)}{suffix}_{variant}.png"


def local_radar_hdf(path: Path) -> Path:
    return path


def extract_forecast_timestamp(filename: str) -> Optional[datetime]:
    """Extract timestamp from forecast TAR filename.

    Expected format: T_PABV23_C_OKPR_YYYYMMDD.HHMM.ft60s10.tar
    """
    import re
    # Pattern: YYYYMMDD.HHMM in the filename
    pattern = r"(\d{8})\.(\d{4})"
    match = re.search(pattern, filename)
    if not match:
        return None

    date_part, time_part = match.groups()
    # Convert HHMM to HHMM00 (add seconds)
    full_time = time_part + "00"
    return datetime.strptime(f"{date_part}{full_time}", "%Y%m%d%H%M%S")


