"""High-level PNG generation pipeline."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

from .config import CONFIG
from .hdf_reader import load_radar_hdf
from .naming import background_filename, overlay_filename
from .png_renderer import render_main_map, render_overlays

LOGGER = logging.getLogger(__name__)


def generate_pngs(hdf_path: Path, forecast: bool = False, offset_minutes: int | None = None) -> Dict[str, Path]:
    product = load_radar_hdf(hdf_path)
    ts = product.metadata.timestamp

    if forecast and offset_minutes is None:
        raise ValueError("forecast PNG generation requires offset_minutes")

    output_dir = CONFIG.storage.forecast_output_dir if forecast else CONFIG.storage.radar_output_dir

    background_name = background_filename(ts, forecast=forecast, offset=offset_minutes)
    background_path = output_dir / background_name
    background_path.parent.mkdir(parents=True, exist_ok=True)

    result = {}

    if not forecast:
        render_main_map(product, background_path)
        result = {"background": background_path}

    overlays = render_overlays(product, background_path.with_suffix(".png"))

    for variant, path in overlays.items():
        overlay_name = overlay_filename(ts, variant, forecast=forecast, offset=offset_minutes)
        target_path = output_dir / overlay_name
        path.rename(target_path)
        result[variant] = target_path

    LOGGER.info("Generated %d PNG variants for %s", len(result), hdf_path.name)
    return result


def generate_pngs_batch(hdf_paths: List[Path], forecast: bool = False, offset_minutes_list: List[int | None] = None) -> Dict[Path, Dict[str, Path]]:
    """Generate PNGs for multiple HDF files in parallel."""
    if offset_minutes_list is None:
        offset_minutes_list = [None] * len(hdf_paths)
    elif len(offset_minutes_list) != len(hdf_paths):
        raise ValueError("offset_minutes_list must have same length as hdf_paths")

    # Process files in parallel batches
    results = {}

    with ThreadPoolExecutor(max_workers=CONFIG.rendering.max_workers) as executor:
        # Submit all tasks
        future_to_path = {
            executor.submit(generate_pngs, hdf_path, forecast, offset_minutes): hdf_path
            for hdf_path, offset_minutes in zip(hdf_paths, offset_minutes_list)
        }

        # Collect results as they complete
        for future in as_completed(future_to_path):
            hdf_path = future_to_path[future]
            try:
                result = future.result()
                results[hdf_path] = result
                LOGGER.debug("Completed processing %s", hdf_path.name)
            except Exception as e:
                LOGGER.error("Failed to process %s: %s", hdf_path.name, e)
                raise

    return results


