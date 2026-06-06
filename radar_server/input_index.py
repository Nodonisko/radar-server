"""Filesystem-backed snapshot of local radar input files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from .config import InputConfig, ProductConfig
from .fetching import LocalInputFile, RemoteInputFile


@dataclass(frozen=True)
class LocalInputIndex:
    """Immutable index of recent local input files by input ID and timestamp."""

    files: dict[str, dict[datetime, tuple[LocalInputFile, ...]]]

    @classmethod
    def from_filesystem(cls, inputs: Iterable[InputConfig], *, now: datetime | None = None) -> "LocalInputIndex":
        files: dict[str, dict[datetime, tuple[LocalInputFile, ...]]] = {}
        for input_config in inputs:
            by_timestamp: dict[datetime, tuple[LocalInputFile, ...]] = {}
            for item in _scan_local_input(input_config, cutoff=_input_cutoff(input_config, now=now)):
                existing = by_timestamp.get(item.timestamp, ())
                by_timestamp[item.timestamp] = _dedupe_local_files((*existing, item))
            if by_timestamp:
                files[input_config.id] = by_timestamp
        return cls(files=files)

    def files_for(self, input_config: InputConfig, timestamp: datetime) -> tuple[LocalInputFile, ...]:
        return self.files.get(input_config.id, {}).get(timestamp, ())

    def timestamps_for(self, input_config: InputConfig) -> set[datetime]:
        return set(self.files.get(input_config.id, {}))

    def ready_timestamps(self, product: ProductConfig) -> set[datetime]:
        if not product.enabled or not product.inputs:
            return set()

        timestamp_sets = [self.timestamps_for(input_config) for input_config in product.inputs]
        if not timestamp_sets:
            return set()
        ready = timestamp_sets[0].copy()
        for timestamps in timestamp_sets[1:]:
            ready &= timestamps
        return ready


def _scan_local_input(input_config: InputConfig, *, cutoff: datetime | None = None) -> tuple[LocalInputFile, ...]:
    if not input_config.local_dir.exists():
        return ()

    files: list[LocalInputFile] = []
    for path in sorted(input_config.local_dir.iterdir(), key=lambda item: item.name, reverse=True):
        if not path.is_file() or path.name.endswith(".part") or path.suffix.lower() not in input_config.file_suffixes:
            continue
        timestamp = input_config.timestamp_from_name(path.name)
        if timestamp is None:
            continue
        if cutoff is not None and timestamp < cutoff:
            break
        remote = RemoteInputFile(
            input=input_config,
            timestamp=timestamp,
            url=path.resolve().as_uri(),
            filename=path.name,
            metadata={"source": "local_scan"},
        )
        files.append(LocalInputFile(input_config, timestamp, path, remote, downloaded=False))
    return tuple(files)


def _input_cutoff(input_config: InputConfig, *, now: datetime | None) -> datetime | None:
    keep_for = input_config.retention.keep_for_seconds
    if keep_for is None:
        return None
    return (now or datetime.utcnow()) - timedelta(seconds=keep_for)


def _dedupe_local_files(files: Iterable[LocalInputFile]) -> tuple[LocalInputFile, ...]:
    by_path: dict[Path, LocalInputFile] = {}
    for item in files:
        by_path[item.path] = item
    return tuple(by_path.values())
