"""Encode an :class:`IndexedImage` to an optimized PNG.

The image is written as a paletted PNG with a single transparent index, then
optionally crushed with oxipng. Output dimensions equal the grid size exactly.
"""

from __future__ import annotations

import logging
import os
import shutil
import struct
import subprocess
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

from PIL import Image

from .colorize import IndexedImage

LOGGER = logging.getLogger(__name__)

_OXIPNG_CHECKED = False
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_PNG_TEXT_KEYWORD = "Comment"


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


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    return (
        struct.pack(">I", len(data))
        + chunk_type
        + data
        + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
    )


def _png_text_chunk(keyword: str, text: str) -> bytes:
    keyword_bytes = keyword.encode("latin-1")
    if not keyword_bytes or len(keyword_bytes) > 79 or b"\x00" in keyword_bytes:
        raise ValueError("PNG text keyword must be 1-79 Latin-1 bytes without NUL")
    text_bytes = text.encode("latin-1")
    return _png_chunk(b"tEXt", keyword_bytes + b"\x00" + text_bytes)


def _write_png_text(path: Path, *, keyword: str, text: str) -> None:
    """Insert a PNG tEXt chunk without touching compressed image data."""

    blob = path.read_bytes()
    if not blob.startswith(_PNG_SIGNATURE):
        raise ValueError(f"{path.name} is not a PNG file")

    keyword_bytes = keyword.encode("latin-1")
    replacement = _png_text_chunk(keyword, text)
    output = bytearray(_PNG_SIGNATURE)
    inserted = False
    saw_iend = False
    pos = len(_PNG_SIGNATURE)

    while pos < len(blob):
        if pos + 8 > len(blob):
            raise ValueError(f"{path.name} has a truncated PNG chunk header")
        length = struct.unpack(">I", blob[pos : pos + 4])[0]
        chunk_type = blob[pos + 4 : pos + 8]
        chunk_end = pos + 12 + length
        if chunk_end > len(blob):
            raise ValueError(f"{path.name} has a truncated PNG chunk")

        chunk = blob[pos:chunk_end]
        data = blob[pos + 8 : pos + 8 + length]
        is_same_text = chunk_type == b"tEXt" and data.startswith(keyword_bytes + b"\x00")

        if not inserted and chunk_type in {b"IDAT", b"IEND"}:
            output.extend(replacement)
            inserted = True
        if not is_same_text:
            output.extend(chunk)

        pos = chunk_end
        if chunk_type == b"IEND":
            saw_iend = True
            break

    if not saw_iend:
        raise ValueError(f"{path.name} is missing a PNG IEND chunk")
    path.write_bytes(bytes(output))


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
    comment: str | None = None,
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
        if comment is not None:
            _write_png_text(tmp, keyword=_PNG_TEXT_KEYWORD, text=comment)
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    LOGGER.debug("Wrote %s (%dx%d)", path.name, img.width, img.height)
    return path
