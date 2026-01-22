"""PNG rendering utilities."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from PIL import Image

from .config import CONFIG
from .hdf_reader import RadarProduct

LOGGER = logging.getLogger(__name__)


_OXIPNG_VALIDATED = False


def _ensure_oxipng_available() -> None:
    global _OXIPNG_VALIDATED
    if _OXIPNG_VALIDATED:
        return

    if shutil.which("oxipng") is None:
        raise RuntimeError("oxipng executable not found in PATH; install oxipng to enable PNG optimization")

    _OXIPNG_VALIDATED = True


def _run_oxipng(path: Path) -> None:
    result = subprocess.run(
        (
            "oxipng",
            "--opt",
            "max",
            "--strip",
            "safe",
            "--alpha",
            str(path),
        ),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"oxipng optimization failed for {path.name}: {result.stderr.strip()}")


def _optimize_png(temp_path: Path, final_path: Path) -> None:
    """Optimize PNG from temp_path and atomically rename to final_path."""
    if not temp_path.exists():
        raise FileNotFoundError(temp_path)

    _ensure_oxipng_available()

    start_time = time.perf_counter()
    original_size = temp_path.stat().st_size

    # Quantization phase
    quantize_start = time.perf_counter()
    with Image.open(temp_path) as image:
        optimized = image.copy()
        if optimized.mode == "RGBA":
            optimized = optimized.quantize(colors=64, method=Image.FASTOCTREE, dither=Image.Dither.NONE)
        quantize_time = time.perf_counter() - quantize_start

        # Save phase
        save_start = time.perf_counter()
        optimized.save(temp_path, format="PNG", optimize=True, compress_level=9)
        save_time = time.perf_counter() - save_start

    # oxipng phase
    oxipng_start = time.perf_counter()
    _run_oxipng(temp_path)
    oxipng_time = time.perf_counter() - oxipng_start

    total_time = time.perf_counter() - start_time
    optimized_size = temp_path.stat().st_size
    savings = (1 - optimized_size / original_size) * 100 if original_size else 0.0
    
    LOGGER.info(
        "Optimized %s: %.1fKB → %.1fKB (%.1f%%) | quantize=%.0fms save=%.0fms oxipng=%.0fms total=%.0fms",
        final_path.name,
        original_size / 1024,
        optimized_size / 1024,
        savings,
        quantize_time * 1000,
        save_time * 1000,
        oxipng_time * 1000,
        total_time * 1000,
    )

    # Atomic rename: ensures Caddy never serves a partially optimized file
    os.rename(temp_path, final_path)


def _create_colormap() -> Tuple[mcolors.ListedColormap, mcolors.BoundaryNorm]:
    cmap = mcolors.ListedColormap(CONFIG.rendering.color_steps)
    bounds = list(range(4, 68, 4))
    return cmap, mcolors.BoundaryNorm(bounds, cmap.N)


def _mask_reflectivity(data: np.ndarray) -> np.ndarray:
    masked = data.copy()
    masked[masked < 4] = np.nan
    return masked


def _render_single_overlay(product: RadarProduct, base_path: Path, name: str, dpi: int, scale: int) -> Tuple[str, Path, Path]:
    """Render a single overlay variant to a temp file."""
    start_time = time.perf_counter()
    
    cmap, norm = _create_colormap()
    lon_min, lon_max, lat_min, lat_max = product.metadata.bounds
    projection_extent = [lon_min, lon_max, lat_max, lat_min]

    target_width = product.metadata.grid_shape[1] * scale
    target_height = product.metadata.grid_shape[0] * scale
    figsize = (target_width / dpi, target_height / dpi)

    # Matplotlib rendering phase
    render_start = time.perf_counter()
    fig, ax = plt.subplots(figsize=figsize)
    ax.axis("off")
    masked = _mask_reflectivity(product.data)
    ax.imshow(
        masked,
        extent=projection_extent,
        cmap=cmap,
        norm=norm,
        interpolation="nearest",
        origin="upper",
        aspect="auto",
    )
    plt.subplots_adjust(0, 0, 1, 1)
    render_time = time.perf_counter() - render_start

    final_path = base_path.with_name(f"{base_path.stem}_{name}{base_path.suffix}")
    temp_path = final_path.with_suffix(".tmp.png")
    
    # Matplotlib save phase
    save_start = time.perf_counter()
    fig.savefig(temp_path, dpi=dpi, transparent=True, bbox_inches="tight", pad_inches=0)
    save_time = time.perf_counter() - save_start
    plt.close(fig)

    total_time = time.perf_counter() - start_time
    LOGGER.info(
        "Rendered %s overlay at %dx%d | render=%.0fms save=%.0fms total=%.0fms",
        name,
        target_width,
        target_height,
        render_time * 1000,
        save_time * 1000,
        total_time * 1000,
    )

    return name, temp_path, final_path


def render_overlays(product: RadarProduct, base_path: Path) -> Dict[str, Path]:
    """Render overlay variants in parallel."""
    overlay_configs = [
        ("overlay", CONFIG.rendering.overlay_target_dpi, 1),
        ("overlay2x", CONFIG.rendering.overlay_retina_dpi, 2),
    ]

    # Submit all overlay rendering tasks to thread pool
    with ThreadPoolExecutor(max_workers=CONFIG.rendering.max_workers) as executor:
        # Submit tasks
        future_to_config = {
            executor.submit(_render_single_overlay, product, base_path, name, dpi, scale): (name, dpi, scale)
            for name, dpi, scale in overlay_configs
        }

        # Collect results as they complete
        overlays = {}
        temp_to_final = {}
        for future in as_completed(future_to_config):
            name, temp_path, final_path = future.result()
            overlays[name] = final_path
            temp_to_final[temp_path] = final_path

    # Optimize all overlays in parallel, then atomically rename to final paths
    with ThreadPoolExecutor(max_workers=CONFIG.rendering.optimize_workers) as executor:
        future_to_paths = {
            executor.submit(_optimize_png, temp_path, final_path): (temp_path, final_path)
            for temp_path, final_path in temp_to_final.items()
        }

        # Wait for all optimizations to complete
        for future in as_completed(future_to_paths):
            future.result()  # Will raise exception if optimization failed

    return overlays


