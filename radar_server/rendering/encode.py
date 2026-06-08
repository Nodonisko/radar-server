"""Encode an :class:`IndexedImage` to an optimized PNG.

The image is written as a paletted PNG with a single transparent index, then
optionally crushed with oxipng. Output dimensions equal the grid size exactly.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from PIL import Image

from .colorize import IndexedImage

LOGGER = logging.getLogger(__name__)

_OXIPNG_CHECKED = False


@dataclass
class PngWriteTimings:
    save: float = 0.0
    oxipng: float = 0.0


def _ensure_oxipng() -> None:
    global _OXIPNG_CHECKED
    if _OXIPNG_CHECKED:
        return
    if shutil.which("oxipng") is None:
        raise RuntimeError("oxipng not found in PATH; install it or pass optimize=False")
    _OXIPNG_CHECKED = True


def _run_oxipng(path: Path) -> None:
    result = subprocess.run(
        ("oxipng", "--opt", "3", "--strip", "safe", "--alpha", str(path)),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"oxipng failed for {path.name}: {result.stderr.strip()}")


def _to_pil(image: IndexedImage) -> Image.Image:
    height, width = image.indices.shape
    img = Image.frombytes("P", (width, height), image.indices.tobytes())
    flat: list[int] = []
    for rgb in image.palette:
        flat.extend(rgb)
    flat.extend((0, 0, 0))  # transparent index slot
    img.putpalette(flat)
    return img


def write_png(
    image: IndexedImage,
    path: Path,
    *,
    optimize: bool = True,
    timings: PngWriteTimings | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    img = _to_pil(image)

    # Write to a temp file and atomically rename so a server never serves a
    # half-written or half-optimized image. Clean up the temp file if any step
    # fails (e.g. oxipng errors) rather than leaving an orphan.
    tmp = path.with_suffix(".tmp.png")
    try:
        step_start = time.perf_counter()
        img.save(tmp, format="PNG", transparency=image.transparent_index, optimize=False)
        if timings is not None:
            timings.save += time.perf_counter() - step_start
        if optimize:
            step_start = time.perf_counter()
            _ensure_oxipng()
            _run_oxipng(tmp)
            if timings is not None:
                timings.oxipng += time.perf_counter() - step_start
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    LOGGER.debug("Wrote %s (%dx%d)", path.name, img.width, img.height)
    return path
