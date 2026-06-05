"""Filesystem pruning for downloaded inputs and rendered products."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from .config import InputConfig, ProductConfig, RenderContext

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PruneResult:
    deleted: tuple[Path, ...] = ()

    @property
    def deleted_count(self) -> int:
        return len(self.deleted)


def prune_all(
    *,
    inputs: Iterable[InputConfig],
    products: Iterable[ProductConfig],
    now: datetime | None = None,
) -> PruneResult:
    deleted: list[Path] = []
    deleted.extend(prune_input_files(inputs, now=now).deleted)
    deleted.extend(prune_product_outputs(products, now=now).deleted)
    return PruneResult(tuple(deleted))


def prune_input_files(inputs: Iterable[InputConfig], *, now: datetime | None = None) -> PruneResult:
    reference = now or datetime.utcnow()
    deleted: list[Path] = []
    for input_config in inputs:
        keep_for = input_config.retention.keep_for_seconds
        if keep_for is None or not input_config.local_dir.exists():
            continue
        cutoff = reference - timedelta(seconds=keep_for)
        for path in sorted(input_config.local_dir.iterdir()):
            if not _is_prunable_input_file(path, input_config):
                continue
            timestamp = input_config.timestamp_from_name(path.name)
            if timestamp is None or timestamp >= cutoff:
                continue
            _unlink(path)
            deleted.append(path)
    return PruneResult(tuple(deleted))


def prune_product_outputs(products: Iterable[ProductConfig], *, now: datetime | None = None) -> PruneResult:
    reference = now or datetime.utcnow()
    deleted: list[Path] = []
    for product in products:
        keep_for = product.retention.keep_for_seconds
        if keep_for is None or not product.output_dir.exists():
            continue
        cutoff = reference - timedelta(seconds=keep_for)
        for sidecar in sorted(product.output_dir.glob("*.json")):
            timestamp = _timestamp_from_product_sidecar(product, sidecar)
            if timestamp is None or timestamp >= cutoff:
                continue
            for path in _output_frame_paths(product, sidecar):
                if path.exists():
                    _unlink(path)
                    deleted.append(path)
    return PruneResult(tuple(deleted))


def _is_prunable_input_file(path: Path, input_config: InputConfig) -> bool:
    return (
        path.is_file()
        and not path.name.endswith(".part")
        and path.suffix.lower() in input_config.file_suffixes
    )


def _timestamp_from_product_sidecar(product: ProductConfig, sidecar: Path) -> datetime | None:
    prefix = _product_prefix(product)
    suffix = sidecar.stem.removeprefix(prefix)
    if suffix == sidecar.stem or not suffix:
        return None
    if suffix.startswith("_"):
        suffix = suffix[1:]
    for fmt in ("%Y%m%d_%H%M", "%Y%m%d_%H%M%S"):
        try:
            return datetime.strptime(suffix, fmt)
        except ValueError:
            continue
    return None


def _product_prefix(product: ProductConfig) -> str:
    marker = datetime(2000, 1, 2, 3, 4)
    base = product.base_name(RenderContext(product=product, timestamp=marker))
    suffix = marker.strftime("%Y%m%d_%H%M")
    if base.endswith(suffix):
        return base[: -len(suffix)]
    return ""


def _output_frame_paths(product: ProductConfig, sidecar: Path) -> tuple[Path, ...]:
    base = sidecar.stem
    variants = tuple(product.output_dir / f"{base}_{name}.png" for name, _ in product.render.variants)
    return (sidecar, *variants)


def _unlink(path: Path) -> None:
    try:
        path.unlink()
        LOGGER.debug("Pruned %s", path)
    except FileNotFoundError:
        pass
