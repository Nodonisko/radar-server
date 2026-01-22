"""Project-wide configuration constants."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def _get_cpu_count() -> int:
    """Get the number of available CPU threads."""
    return max(1, os.cpu_count() or 1)


def _get_optimize_workers() -> int:
    """Get the number of workers for PNG optimization (all CPU cores)."""
    return _get_cpu_count()


@dataclass(frozen=True)
class TimingConfig:
    publish_interval: int = 300  # seconds
    quick_check_interval: int = 3  # seconds
    quick_check_limit: int = 90


@dataclass(frozen=True)
class StorageConfig:
    radar_data_dir: Path = BASE_DIR / "radar_data"
    radar_output_dir: Path = BASE_DIR / "output"
    forecast_data_dir: Path = BASE_DIR / "radar_data_forecast"
    forecast_output_dir: Path = BASE_DIR / "output_forecast"
    min_tracked_files: int = 12
    max_tracked_files: int = 600 # little over 2 hours of data
    max_forecast_files: int = 12 # 1 hour of data


@dataclass(frozen=True)
class DevServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    enabled_only_on_macos: bool = True


@dataclass(frozen=True)
class SourceConfig:
    radar_base_url: str = "https://opendata.chmi.cz/meteorology/weather/radar/composite/maxz/hdf5/"
    forecast_base_url: str = "https://opendata.chmi.cz/meteorology/weather/radar/composite/fct_maxz/hdf5/"


@dataclass(frozen=True)
class RenderingConfig:
    # 4 dBZ steps, values below 4 are transparent
    color_steps: tuple[str, ...] = (
        "#390071",
        "#3001A9",
        "#0200FB",
        "#076CBC",
        "#00A400",
        "#00BB03",
        "#36D700",
        "#9CDD07",
        "#E0DC01",
        "#FBB200",
        "#F78600",
        "#FF5400",
        "#FE0100",
        "#A40003",
        "#FCFCFC",
    )
    overlay_target_dpi: int = 72
    overlay_retina_dpi: int = 144
    main_map_dpi: int = 300
    base_figure_width_inches: float = 12.0
    # Parallel processing configuration
    max_workers: int = field(default_factory=_get_cpu_count)  # Number of worker threads for parallel rendering
    optimize_workers: int = field(default_factory=_get_optimize_workers)  # Number of worker threads for PNG optimization


@dataclass(frozen=True)
class Config:
    timing: TimingConfig = TimingConfig()
    storage: StorageConfig = StorageConfig()
    sources: SourceConfig = SourceConfig()
    rendering: RenderingConfig = RenderingConfig()
    dev_server: DevServerConfig = DevServerConfig()

    def ensure_directories(self) -> None:
        for path in (
            self.storage.radar_data_dir,
            self.storage.radar_output_dir,
            self.storage.forecast_data_dir,
            self.storage.forecast_output_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


CONFIG = Config()
CONFIG.ensure_directories()


