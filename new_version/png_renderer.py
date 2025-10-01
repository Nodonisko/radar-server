"""PNG rendering utilities."""

from __future__ import annotations

import logging
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Tuple

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from PIL import Image

from .config import CONFIG
from .hdf_reader import RadarProduct

LOGGER = logging.getLogger(__name__)


CITY_MARKERS = {
    "Prague": (14.4378, 50.0755),
    "Brno": (16.6068, 49.1951),
    "Ostrava": (18.282, 49.8209),
    "Plzeň": (13.3775, 49.7384),
    "Olomouc": (17.2509, 49.5938),
    "České Budějovice": (14.4747, 48.9745),
    "Hradec Králové": (15.8327, 50.2103),
    "Bratislava": (17.1077, 48.1482),
    "Vienna": (16.3738, 48.2082),
    "Dresden": (13.7373, 51.0504),
    "Kraków": (19.945, 50.0647),
    "Wrocław": (17.0385, 51.1079),
}


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


def _optimize_png(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(path)

    _ensure_oxipng_available()

    original_size = path.stat().st_size

    with Image.open(path) as image:
        optimized = image.copy()
        if optimized.mode == "RGBA":
            optimized = optimized.quantize(colors=64, method=Image.FASTOCTREE, dither=Image.Dither.NONE)

        optimized.save(path, format="PNG", optimize=True, compress_level=9)

    _run_oxipng(path)

    optimized_size = path.stat().st_size
    savings = (1 - optimized_size / original_size) * 100 if original_size else 0.0
    LOGGER.debug(
        "Optimized %s: %.1fKB → %.1fKB (%.1f%%)",
        path.name,
        original_size / 1024,
        optimized_size / 1024,
        savings,
    )


def _create_colormap() -> Tuple[mcolors.ListedColormap, mcolors.BoundaryNorm]:
    cmap = mcolors.ListedColormap(CONFIG.rendering.color_steps)
    bounds = list(range(4, 68, 4))
    return cmap, mcolors.BoundaryNorm(bounds, cmap.N)


def _mask_reflectivity(data: np.ndarray) -> np.ndarray:
    masked = data.copy()
    masked[masked < 4] = np.nan
    return masked


def _figure_size(bounds: Tuple[float, float, float, float], base_width: float) -> Tuple[float, float]:
    lon_min, lon_max, lat_min, lat_max = bounds
    lon_span = lon_max - lon_min
    lat_span = lat_max - lat_min
    aspect = lon_span / lat_span if lat_span else 1.0
    return base_width, base_width / aspect


def _add_cities(ax, projection):
    for name, (lon, lat) in CITY_MARKERS.items():
        ax.plot(lon, lat, "o", color="red", markersize=4, markeredgecolor="white", markeredgewidth=0.5, transform=projection, zorder=10)
        ax.text(
            lon,
            lat,
            f"  {name}",
            transform=projection,
            fontsize=8,
            fontweight="bold",
            color="black",
            ha="left",
            va="center",
            zorder=11,
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.75, linewidth=0.3),
        )


def render_main_map(product: RadarProduct, output_path: Path) -> Path:
    cmap, norm = _create_colormap()
    projection = ccrs.PlateCarree()
    fig_size = _figure_size(product.metadata.bounds, CONFIG.rendering.base_figure_width_inches)

    LOGGER.info("Rendering composite map to %s", output_path)

    fig, ax = plt.subplots(figsize=fig_size, subplot_kw={"projection": projection})
    lon_min, lon_max, lat_min, lat_max = product.metadata.bounds
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=projection)

    ax.add_feature(cfeature.BORDERS.with_scale("50m"), linewidth=1.0, edgecolor="black", alpha=0.7)
    ax.add_feature(cfeature.COASTLINE.with_scale("50m"), linewidth=0.6, edgecolor="black", alpha=0.5)
    ax.add_feature(cfeature.RIVERS.with_scale("50m"), linewidth=0.4, edgecolor="blue", alpha=0.4)
    ax.add_feature(cfeature.LAKES.with_scale("50m"), facecolor="lightblue", edgecolor="blue", alpha=0.25)
    _add_cities(ax, projection)

    masked = _mask_reflectivity(product.data)
    im = ax.imshow(
        masked,
        extent=[lon_min, lon_max, lat_min, lat_max],
        cmap=cmap,
        norm=norm,
        interpolation="nearest",
        aspect="auto",
        origin="upper",
        transform=projection,
    )

    colorbar = plt.colorbar(im, ax=ax, shrink=0.75, pad=0.02)
    colorbar.set_label("Radar Reflectivity (dBZ)", fontsize=11)

    ax.set_title(
        "Czech Radar Composite – MAX_Z\n" + product.metadata.timestamp.strftime("%Y-%m-%d %H:%M UTC"),
        fontsize=13,
        fontweight="bold",
    )
    ax.text(
        0.01,
        0.01,
        "Data source: CHMI",
        transform=ax.transAxes,
        fontsize=7,
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.7),
    )

    plt.tight_layout()
    fig.savefig(output_path, dpi=CONFIG.rendering.main_map_dpi, facecolor="white", edgecolor="none", bbox_inches="tight")
    plt.close(fig)

    # Optimize in parallel with overlay optimizations
    _optimize_png(output_path)

    return output_path


def _render_single_overlay(product: RadarProduct, base_path: Path, name: str, dpi: int, scale: int) -> Tuple[str, Path]:
    """Render a single overlay variant."""
    cmap, norm = _create_colormap()
    lon_min, lon_max, lat_min, lat_max = product.metadata.bounds
    projection_extent = [lon_min, lon_max, lat_max, lat_min]

    target_width = product.metadata.grid_shape[1] * scale
    target_height = product.metadata.grid_shape[0] * scale
    figsize = (target_width / dpi, target_height / dpi)

    LOGGER.debug("Rendering %s overlay at %dx%d", name, target_width, target_height)

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

    output_path = base_path.with_name(f"{base_path.stem}_{name}{base_path.suffix}")
    fig.savefig(output_path, dpi=dpi, transparent=True, bbox_inches="tight", pad_inches=0)
    plt.close(fig)

    return name, output_path


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
        for future in as_completed(future_to_config):
            name, output_path = future.result()
            overlays[name] = output_path

    # Optimize all overlays in parallel
    overlay_paths = list(overlays.values())
    with ThreadPoolExecutor(max_workers=CONFIG.rendering.optimize_workers) as executor:
        future_to_path = {
            executor.submit(_optimize_png, path): path
            for path in overlay_paths
        }

        # Wait for all optimizations to complete
        for future in as_completed(future_to_path):
            future.result()  # Will raise exception if optimization failed

    return overlays


