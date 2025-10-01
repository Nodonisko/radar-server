"""Forecast TAR extraction helpers."""

from __future__ import annotations

import logging
import tarfile
from pathlib import Path
from typing import List

LOGGER = logging.getLogger(__name__)


def extract_forecast_tar(tar_path: Path, target_dir: Path) -> List[Path]:
    target_dir.mkdir(parents=True, exist_ok=True)
    extracted: List[Path] = []

    LOGGER.info("Extracting forecast TAR %s", tar_path.name)
    with tarfile.open(tar_path, "r") as archive:
        for member in archive.getmembers():
            if member.isfile() and member.name.endswith(".hdf"):
                archive.extract(member, path=target_dir)
                extracted_path = target_dir / member.name
                final_path = target_dir / extracted_path.name
                if extracted_path != final_path:
                    extracted_path.rename(final_path)
                extracted.append(final_path)

    LOGGER.debug("Extracted %d forecast files", len(extracted))
    return extracted



