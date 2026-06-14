from __future__ import annotations

import struct
from pathlib import Path

import numpy as np

from radar_server.rendering.colorize import IndexedImage
from radar_server.rendering.encode import write_png


def _png_chunks(path: Path) -> list[tuple[bytes, bytes]]:
    blob = path.read_bytes()
    pos = 8
    chunks: list[tuple[bytes, bytes]] = []
    while pos < len(blob):
        length = struct.unpack(">I", blob[pos : pos + 4])[0]
        chunk_type = blob[pos + 4 : pos + 8]
        data = blob[pos + 8 : pos + 8 + length]
        chunks.append((chunk_type, data))
        pos += 12 + length
        if chunk_type == b"IEND":
            break
    return chunks


def _comment(path: Path) -> str | None:
    for chunk_type, data in _png_chunks(path):
        if chunk_type == b"tEXt" and data.startswith(b"Comment\x00"):
            return data.split(b"\x00", 1)[1].decode("latin-1")
    return None


def _idat_payload(path: Path) -> bytes:
    return b"".join(data for chunk_type, data in _png_chunks(path) if chunk_type == b"IDAT")


def _image() -> IndexedImage:
    return IndexedImage(
        indices=np.array([[0, 1], [1, 2]], dtype=np.uint8),
        palette=[(0, 0, 0), (255, 0, 0)],
        transparent_index=2,
    )


def test_write_png_comment_preserves_image_data(tmp_path: Path) -> None:
    plain = tmp_path / "plain.png"
    commented = tmp_path / "commented.png"
    comment = "GeoBox=11.27,48.04,19.64,51.46"

    write_png(_image(), plain, optimize=False)
    write_png(_image(), commented, optimize=False, comment=comment)

    assert _comment(commented) == comment
    assert _idat_payload(commented) == _idat_payload(plain)
    assert commented.stat().st_size - plain.stat().st_size == len("Comment") + 1 + len(comment) + 12
