"""End-to-end PNG pipeline: HDF5 -> reprojected, colorized, optimized PNG(s).

    decode -> reproject(lossless Web Mercator) -> [downsample] -> colorize -> encode

Single frame (`render_radar_png`) or merged multi-file composite
(`render_composite_png`). Both reproject to a lossless Web Mercator grid, emit
the full overlay plus any smaller variants (block/max-pooled), and write a JSON
sidecar with the shared lat/lon bounds so the consumer can place every variant
on a web map for any country.
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .colorize import colorize
from .composite import composite_to_web_mercator
from .core import PaletteSpec, RadarField
from .decode import load_odim_hdf
from .downsample import downsample_max
from .encode import write_png
from .reproject import lonlat_bounds, to_web_mercator

LOGGER = logging.getLogger(__name__)

_MAX_WORKERS = max(1, os.cpu_count() or 1)

# (variant name, downsample factor). 1.0 = full lossless; >1 = max-pooled
# coarser (factor may be fractional, e.g. 1.5).
DEFAULT_VARIANTS: Tuple[Tuple[str, float], ...] = (("overlay", 1.0), ("overlay_small", 1.5))

Bounds = Tuple[float, float, float, float]  # west, south, east, north (WGS84)


@dataclass(frozen=True)
class RenderResult:
    base: str
    variants: Dict[str, Path]
    sidecar: Path
    bounds: Bounds


def _emit(
    field: RadarField,
    output_dir: Path,
    palette: PaletteSpec,
    base: str,
    variants: Sequence[Tuple[str, float]],
    optimize: bool,
    sources: Sequence[str],
) -> RenderResult:
    """Shared tail: downsample -> colorize -> encode each variant, write sidecar."""
    if not variants:
        raise ValueError("variants must not be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    bounds = lonlat_bounds(field)

    written: Dict[str, Path] = {}
    manifest: Dict[str, dict] = {}
    for name, factor in variants:
        variant_field = downsample_max(field, factor)
        image = colorize(variant_field, palette)
        path = output_dir / f"{base}_{name}.png"
        write_png(image, path, optimize=optimize)
        written[name] = path
        manifest[name] = {
            "file": path.name,
            "width": variant_field.transform.width,
            "height": variant_field.transform.height,
        }

    sidecar = output_dir / f"{base}.json"
    sidecar.write_text(
        json.dumps(
            {
                "timestamp": field.timestamp.isoformat(),
                "quantity": field.quantity,
                "palette": palette.name,
                "crs": field.crs,
                "bounds": {"west": bounds[0], "south": bounds[1], "east": bounds[2], "north": bounds[3]},
                "sources": list(sources),
                "variants": manifest,
            },
            indent=2,
        )
    )

    LOGGER.info("Rendered %s -> %s", base, ", ".join(p.name for p in written.values()))
    return RenderResult(base=base, variants=written, sidecar=sidecar, bounds=bounds)


def render_radar_png(
    hdf_path: Path,
    output_dir: Path,
    palette: PaletteSpec,
    *,
    base: str,
    variants: Sequence[Tuple[str, float]] = DEFAULT_VARIANTS,
    optimize: bool = True,
) -> RenderResult:
    """Render one HDF5 file to lossless Web Mercator overlay PNG(s).

    ``base`` is the output filename stem, chosen by the caller. The renderer is
    naming-agnostic; the data timestamp is recorded in the sidecar, not the name.
    """
    field = to_web_mercator(load_odim_hdf(hdf_path, quantity=palette.quantity))
    return _emit(field, output_dir, palette, base, variants, optimize, sources=[hdf_path.name])


def render_composite_png(
    hdf_paths: Iterable[Path],
    output_dir: Path,
    palette: PaletteSpec,
    *,
    base: str,
    bounds: Optional[Bounds] = None,
    variants: Sequence[Tuple[str, float]] = DEFAULT_VARIANTS,
    optimize: bool = True,
) -> RenderResult:
    """Merge multiple HDF5 files into one overlay PNG (+ variants).

    All inputs must share a timestamp. ``bounds`` (WGS84 ``west, south, east,
    north``) crops/extends the output; ``None`` uses the union of all inputs.
    ``base`` is the full output filename stem, supplied by the caller.
    """
    paths = list(hdf_paths)
    if not paths:
        raise ValueError("composite needs at least one input file")

    fields = [load_odim_hdf(p, quantity=palette.quantity) for p in paths]
    field = composite_to_web_mercator(fields, bounds=bounds)
    return _emit(field, output_dir, palette, base, variants, optimize, sources=[p.name for p in paths])


def render_batch(
    items: Iterable[Tuple[Path, str]],
    output_dir: Path,
    palette: PaletteSpec,
    *,
    variants: Sequence[Tuple[str, float]] = DEFAULT_VARIANTS,
    optimize: bool = True,
) -> List[RenderResult]:
    """Render many files concurrently. ``items`` is ``(hdf_path, base)`` pairs.

    The caller supplies each output ``base`` (naming is the controller's job).
    Bases must be unique within a batch, since concurrent renders to the same
    name would race on the output and temp files.
    """
    items = list(items)
    if not items:
        return []

    bases = [base for _, base in items]
    duplicates = sorted({b for b in bases if bases.count(b) > 1})
    if duplicates:
        raise ValueError(f"duplicate output bases in batch: {duplicates}")

    results: List[RenderResult] = []
    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(items))) as pool:
        futures = {
            pool.submit(render_radar_png, path, output_dir, palette, base=base, variants=variants, optimize=optimize): (path, base)
            for path, base in items
        }
        for future in as_completed(futures):
            path, base = futures[future]
            try:
                results.append(future.result())
            except Exception:
                # Skip a bad file rather than aborting the whole batch.
                LOGGER.exception("Failed to render %s (base=%s); skipping", path.name, base)
    if len(results) < len(items):
        LOGGER.warning("Rendered %d/%d files (%d failed)", len(results), len(items), len(items) - len(results))
    return results
