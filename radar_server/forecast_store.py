"""On-disk store for computed forecast fields.

Forecast generation writes each extrapolated :class:`RadarField` to a
compressed ``.npz`` file under the forecast product's ``field_dir``. These
files act as the "inputs" of forecast frame rendering, mirroring how observed
products consume downloaded HDF files: rendering becomes ordinary, idempotent,
restart-durable work driven by the filesystem.

Only the latest issue time matters, so :func:`write_forecast_fields` performs
an atomic clear-and-replace of the whole field set.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np

from .config import ForecastProduct
from .render_jobs import output_base
from .rendering.core import GeoTransform, RadarField

LOGGER = logging.getLogger(__name__)

FIELD_SUFFIX = ".npz"
_PART_SUFFIX = ".part"
_STALE_PART_SECONDS = 3600


@dataclass(frozen=True)
class StoredForecastField:
    field: RadarField
    issue_timestamp: datetime
    minute: int


@dataclass(frozen=True)
class ForecastRenderUnit:
    """A stored field file whose rendered outputs are (partially) missing."""

    forecast: ForecastProduct
    issue_timestamp: datetime
    minute: int
    field_path: Path
    base: str


def forecast_base(forecast: ForecastProduct, issue_timestamp: datetime, minute: int) -> str:
    return f"{output_base(forecast.parent, issue_timestamp)}_fct{minute}"


def field_path(forecast: ForecastProduct, issue_timestamp: datetime, minute: int) -> Path:
    return forecast.field_dir / f"{forecast_base(forecast, issue_timestamp, minute)}{FIELD_SUFFIX}"


def frame_output_paths(forecast: ForecastProduct, base: str) -> tuple[Path, ...]:
    """Sidecar and PNG variant paths for one rendered forecast frame."""

    sidecar = forecast.output_dir / f"{base}.json"
    variants = tuple(forecast.output_dir / f"{base}_{name}.png" for name, _ in forecast.render_variants)
    return (sidecar, *variants)


def expected_forecast_paths(forecast: ForecastProduct, issue_timestamp: datetime) -> tuple[Path, ...]:
    """All output paths a fully rendered forecast issue should have."""

    paths: list[Path] = []
    for minute in forecast.minutes:
        paths.extend(frame_output_paths(forecast, forecast_base(forecast, issue_timestamp, minute)))
    return tuple(paths)


def save_field(field: RadarField, path: Path, *, issue_timestamp: datetime, minute: int) -> None:
    """Write one field atomically (``.part`` then rename)."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + _PART_SUFFIX)
    transform = field.transform
    with tmp_path.open("wb") as fh:
        np.savez_compressed(
            fh,
            values=field.values,
            x_min=transform.x_min,
            y_max=transform.y_max,
            px=transform.px,
            py=transform.py,
            width=transform.width,
            height=transform.height,
            crs=field.crs,
            quantity=field.quantity,
            valid_timestamp=field.timestamp.isoformat(),
            issue_timestamp=issue_timestamp.isoformat(),
            minute=minute,
        )
    tmp_path.replace(path)


def load_field(path: Path) -> StoredForecastField:
    with np.load(path, allow_pickle=False) as data:
        transform = GeoTransform(
            x_min=float(data["x_min"]),
            y_max=float(data["y_max"]),
            px=float(data["px"]),
            py=float(data["py"]),
            width=int(data["width"]),
            height=int(data["height"]),
        )
        field = RadarField(
            values=np.asarray(data["values"], dtype=np.float32),
            crs=str(data["crs"]),
            transform=transform,
            quantity=str(data["quantity"]),
            timestamp=datetime.fromisoformat(str(data["valid_timestamp"])),
        )
        return StoredForecastField(
            field=field,
            issue_timestamp=datetime.fromisoformat(str(data["issue_timestamp"])),
            minute=int(data["minute"]),
        )


def read_field_metadata(path: Path) -> tuple[datetime, int]:
    """Return ``(issue_timestamp, minute)`` without materializing values."""

    with np.load(path, allow_pickle=False) as data:
        return datetime.fromisoformat(str(data["issue_timestamp"])), int(data["minute"])


def write_forecast_fields(
    forecast: ForecastProduct,
    issue_timestamp: datetime,
    fields_by_minute: Mapping[int, RadarField],
) -> dict[int, Path]:
    """Persist a full issue's field set, removing any superseded field files."""

    forecast.field_dir.mkdir(parents=True, exist_ok=True)
    final_paths: dict[int, Path] = {}
    for minute, field in sorted(fields_by_minute.items()):
        path = field_path(forecast, issue_timestamp, minute)
        save_field(field, path, issue_timestamp=issue_timestamp, minute=minute)
        final_paths[minute] = path

    keep = set(final_paths.values())
    for existing in forecast.field_dir.glob(f"*{FIELD_SUFFIX}"):
        if existing not in keep:
            existing.unlink(missing_ok=True)
            LOGGER.debug("Removed superseded forecast field %s", existing)
    return final_paths


def existing_field_paths(forecast: ForecastProduct, issue_timestamp: datetime) -> dict[int, Path]:
    """Field files already on disk for one issue time, keyed by lead minute."""

    found: dict[int, Path] = {}
    for minute in forecast.minutes:
        path = field_path(forecast, issue_timestamp, minute)
        if path.exists():
            found[minute] = path
    return found


def discover_forecast_render_units(forecast: ForecastProduct) -> list[ForecastRenderUnit]:
    """Stored fields whose rendered outputs are missing (startup reconciliation)."""

    units: list[ForecastRenderUnit] = []
    if not forecast.field_dir.exists():
        return units
    for path in sorted(forecast.field_dir.glob(f"*{FIELD_SUFFIX}")):
        try:
            issue_timestamp, minute = read_field_metadata(path)
        except Exception:
            LOGGER.warning("Skipping unreadable forecast field %s", path)
            continue
        base = path.stem
        if all(output.exists() for output in frame_output_paths(forecast, base)):
            continue
        units.append(
            ForecastRenderUnit(
                forecast=forecast,
                issue_timestamp=issue_timestamp,
                minute=minute,
                field_path=path,
                base=base,
            )
        )
    return units


def prune_forecast_fields(
    forecasts: Iterable[ForecastProduct],
    *,
    now: datetime | None = None,
) -> tuple[Path, ...]:
    """Delete stored fields older than retention plus stale ``.part`` leftovers."""

    reference = now or datetime.utcnow()
    deleted: list[Path] = []
    for forecast in forecasts:
        keep_for = forecast.retention.keep_for_seconds
        if keep_for is None or not forecast.field_dir.exists():
            continue
        cutoff = reference - timedelta(seconds=keep_for)
        for path in sorted(forecast.field_dir.glob(f"*{FIELD_SUFFIX}")):
            try:
                issue_timestamp, _minute = read_field_metadata(path)
            except Exception:
                LOGGER.warning("Deleting unreadable forecast field %s", path)
                path.unlink(missing_ok=True)
                deleted.append(path)
                continue
            if issue_timestamp < cutoff:
                path.unlink(missing_ok=True)
                deleted.append(path)
        for part in sorted(forecast.field_dir.glob(f"*{_PART_SUFFIX}")):
            modified = datetime.utcfromtimestamp(part.stat().st_mtime)
            if modified < reference - timedelta(seconds=_STALE_PART_SECONDS):
                part.unlink(missing_ok=True)
                deleted.append(part)
    return tuple(deleted)
