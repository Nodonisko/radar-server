"""In-memory index of fetched radar input files."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from .config import InputConfig, ProductConfig
from .fetching import InputSyncResult, LocalInputFile, RemoteInputFile


@dataclass
class InputRegistry:
    """Index local input files by input ID and exact timestamp."""

    _files: dict[str, dict[datetime, tuple[LocalInputFile, ...]]] = field(default_factory=dict)

    @classmethod
    def from_local_inputs(cls, inputs: Iterable[InputConfig], *, now: datetime | None = None) -> "InputRegistry":
        registry = cls()
        input_configs = tuple(inputs)
        registry.scan_local_inputs(input_configs, now=now)
        registry.prune(input_configs, now=now)
        return registry

    def add(self, files: Iterable[LocalInputFile]) -> None:
        for item in files:
            by_timestamp = self._files.setdefault(item.input.id, {})
            existing = by_timestamp.get(item.timestamp, ())
            by_timestamp[item.timestamp] = _dedupe_local_files((*existing, item))

    def add_sync_result(self, result: InputSyncResult) -> None:
        if result.error is None:
            self.add(result.files)
            self.prune([result.input])

    def add_sync_results(self, results: Iterable[InputSyncResult]) -> None:
        inputs: list[InputConfig] = []
        for result in results:
            inputs.append(result.input)
            self.add_sync_result(result)
        self.prune(inputs)

    def scan_local_inputs(self, inputs: Iterable[InputConfig], *, now: datetime | None = None) -> None:
        for input_config in inputs:
            self.add(_scan_local_input(input_config, cutoff=_input_cutoff(input_config, now=now)))

    def files_for(self, input_config: InputConfig, timestamp: datetime) -> tuple[LocalInputFile, ...]:
        return self._files.get(input_config.id, {}).get(timestamp, ())

    def timestamps_for(self, input_config: InputConfig) -> set[datetime]:
        return set(self._files.get(input_config.id, {}))

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

    def prune(self, inputs: Iterable[InputConfig], *, now: datetime | None = None) -> None:
        """Drop registry entries older than each input's availability window."""

        reference = now or _latest_timestamp(self._files)
        if reference is None:
            return

        for input_config in inputs:
            keep_for = input_config.retention.keep_for_seconds
            if keep_for is None:
                continue
            cutoff = reference - timedelta(seconds=keep_for)
            self.prune_input(input_config, cutoff=cutoff)

    def prune_input(self, input_config: InputConfig, *, cutoff: datetime) -> None:
        by_timestamp = self._files.get(input_config.id)
        if not by_timestamp:
            return

        for timestamp in tuple(by_timestamp):
            if timestamp < cutoff:
                del by_timestamp[timestamp]
        if not by_timestamp:
            del self._files[input_config.id]


def _scan_local_input(input_config: InputConfig, *, cutoff: datetime | None = None) -> tuple[LocalInputFile, ...]:
    if not input_config.local_dir.exists():
        return ()

    files: list[LocalInputFile] = []
    for path in sorted(input_config.local_dir.iterdir()):
        if not path.is_file() or path.name.endswith(".part") or path.suffix.lower() not in input_config.file_suffixes:
            continue
        timestamp = input_config.timestamp_from_name(path.name)
        if timestamp is None:
            continue
        if cutoff is not None and timestamp < cutoff:
            continue
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


def _latest_timestamp(files: dict[str, dict[datetime, tuple[LocalInputFile, ...]]]) -> datetime | None:
    latest: datetime | None = None
    for by_timestamp in files.values():
        for timestamp in by_timestamp:
            if latest is None or timestamp > latest:
                latest = timestamp
    return latest
