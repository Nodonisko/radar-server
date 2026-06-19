"""Fetch configured radar inputs into local timestamped files.

This module is the orchestration layer: it discovers remote files for an
``InputConfig`` and materializes them locally, dispatching the source-specific
work to the per-source modules in :mod:`radar_server.sources`. A later
orchestration layer can index the returned records and decide which products are
ready to render.

The shared records, errors, and HTTP helpers live in
:mod:`radar_server.sources.base` and are re-exported here for backward
compatibility, so callers can keep importing them from ``radar_server.fetching``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import (
    ArcoZarrSource,
    HttpDirectorySource,
    InputConfig,
    OrdApiSource,
)
from .sources import arco
from .sources import http_directory
from .sources import ord as ord_source
from .sources.base import (
    CHUNK_SIZE,
    FetchError,
    LocalInputFile,
    RateLimitedError,
    RemoteInputFile,
    SkippableFetchError,
    as_utc,
    request,
)

__all__ = [
    "RemoteInputFile",
    "LocalInputFile",
    "InputSyncResult",
    "FetchError",
    "RateLimitedError",
    "SkippableFetchError",
    "discover_remote_files",
    "sync_input",
    "sync_inputs",
    "download_remote_file",
    "remote_files_from_ord_payload",
]

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class InputSyncResult:
    input: InputConfig
    files: tuple[LocalInputFile, ...] = ()
    error: FetchError | None = None


def discover_remote_files(
    input_config: InputConfig,
    *,
    now: datetime | None = None,
    limit: int | None = None,
) -> list[RemoteInputFile]:
    """Discover remote files for one configured input without downloading them."""

    if not input_config.enabled:
        return []

    source = input_config.source
    if isinstance(source, HttpDirectorySource):
        files = http_directory.discover(input_config, source)
    elif isinstance(source, OrdApiSource):
        files = ord_source.discover(input_config, source, now=now, limit=limit)
    elif isinstance(source, ArcoZarrSource):
        files = arco.discover(input_config, source, now=now, limit=limit)
    else:
        raise TypeError(f"unsupported source config: {source!r}")

    files.sort(key=lambda item: item.timestamp, reverse=True)
    if limit is not None:
        return files[:limit]
    return files


def sync_input(
    input_config: InputConfig,
    *,
    now: datetime | None = None,
    limit: int | None = None,
) -> list[LocalInputFile]:
    """Discover and download missing files for one input."""

    reference = now or datetime.now(timezone.utc)
    remotes = [
        remote
        for remote in discover_remote_files(input_config, now=reference, limit=limit)
        if _within_input_retention(input_config, remote.timestamp, reference)
    ]
    files: list[LocalInputFile] = []
    for remote in remotes:
        try:
            files.append(download_remote_file(remote))
        except SkippableFetchError as exc:
            LOGGER.info("Skipping %s: %s", remote.filename, exc)
    return files


def sync_inputs(
    input_configs: list[InputConfig] | tuple[InputConfig, ...],
    *,
    now: datetime | None = None,
    limit_per_input: int | None = None,
) -> list[InputSyncResult]:
    """Sync multiple inputs, isolating failures to the input that failed."""

    results: list[InputSyncResult] = []
    for input_config in input_configs:
        try:
            files = sync_input(input_config, now=now, limit=limit_per_input)
        except FetchError as exc:
            LOGGER.warning("Input %s sync failed: %s", input_config.id, exc)
            results.append(InputSyncResult(input_config, error=exc))
        else:
            results.append(InputSyncResult(input_config, files=tuple(files)))
    return results


def download_remote_file(remote: RemoteInputFile) -> LocalInputFile:
    """Download one remote input file if it is not already present locally."""

    destination = remote.input.local_dir / remote.filename
    if destination.exists() and destination.stat().st_size > 0:
        return LocalInputFile(remote.input, remote.timestamp, destination, remote, downloaded=False)

    if isinstance(remote.input.source, ArcoZarrSource):
        return arco.download_frame(remote, destination)

    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_name(f"{destination.name}.part")
    LOGGER.info("Downloading %s -> %s", remote.url, destination)
    response = request("GET", remote.url, stream=True)
    with tmp_path.open("wb") as fp:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                fp.write(chunk)
    tmp_path.replace(destination)
    return LocalInputFile(remote.input, remote.timestamp, destination, remote, downloaded=True)


def remote_files_from_ord_payload(
    input_config: InputConfig,
    payload: dict[str, Any],
    *,
    quantity: str,
) -> list[RemoteInputFile]:
    """Extract ODIM download links from an ORD CoverageJSON-style payload."""

    return ord_source.remote_files_from_payload(input_config, payload, quantity=quantity)


def _within_input_retention(input_config: InputConfig, timestamp: datetime, now: datetime) -> bool:
    keep_for = input_config.retention.keep_for_seconds
    if keep_for is None:
        return True
    return timestamp >= as_utc(now).replace(tzinfo=None) - timedelta(seconds=keep_for)
