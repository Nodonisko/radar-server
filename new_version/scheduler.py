"""Orchestrates fetching and processing."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

from .config import CONFIG
from .downloader import download_file, download_tar, iter_latest, list_remote_files
from .forecast import extract_forecast_tar
from .naming import extract_timestamp, extract_forecast_timestamp
from .png_pipeline import generate_pngs, generate_pngs_batch, generate_pngs_extended_batch

LOGGER = logging.getLogger(__name__)


class RadarScheduler:
    def __init__(self) -> None:
        self.config = CONFIG
        self.processed_radar: dict[str, datetime] = {}
        self.completed_forecasts: set[str] = set()
        self.quick_mode: bool = False
        self.quick_attempts: int = 0
        self.quick_last_attempt: datetime | None = None
        self.next_publish: datetime = self._calculate_next_expected()

    def _calculate_next_expected(self, reference: datetime | None = None) -> datetime:
        now = reference or datetime.utcnow()
        minute_bucket = (now.minute // 5 + 1) * 5
        if minute_bucket >= 60:
            next_time = (now.replace(second=0, microsecond=0) + timedelta(hours=1)).replace(minute=0)
        else:
            next_time = now.replace(minute=minute_bucket, second=0, microsecond=0)
        LOGGER.debug("Next expected publish at %s", next_time)
        return next_time

    def _radar_entries(self) -> list[str]:
        entries = list_remote_files(self.config.sources.radar_base_url)
        return iter_latest(entries, self.config.storage.min_tracked_files)

    def _overlay_exists(self, ts: datetime) -> bool:
        """Check if overlay PNG exists for the given timestamp."""
        overlay = self.config.storage.radar_output_dir / f"radar_{ts.strftime('%Y%m%d_%H%M')}_overlay.png"
        return overlay.exists()

    def _ensure_radar_backlog(self) -> tuple[bool, datetime | None, bool]:
        entries = self._radar_entries()
        if not entries:
            LOGGER.warning("No radar entries found")
            return False, None, False

        latest_timestamp = extract_timestamp(entries[0]) if entries else None
        processed_any = False
        latest_overlay_exists = False

        # Collect files to process in batches
        files_to_process = []
        for filename in reversed(entries):
            ts = extract_timestamp(filename)
            if not ts:
                LOGGER.debug("Skipping unrecognized radar filename %s", filename)
                continue

            local_path = self.config.storage.radar_data_dir / filename
            if not local_path.exists():
                downloaded = download_file(self.config.sources.radar_base_url, filename, local_path)
                if not downloaded:
                    continue
                processed_any = True

            overlay_path = self.config.storage.radar_output_dir / f"radar_{ts.strftime('%Y%m%d_%H%M')}_overlay.png"
            if not overlay_path.exists():
                files_to_process.append(local_path)

        # Process files in parallel batches if any need processing
        if files_to_process:
            LOGGER.info("Processing %d radar files in parallel", len(files_to_process))
            self._process_radar_batch(files_to_process)
            processed_any = True

        # Update processed radar map
        for filename in entries:
            ts = extract_timestamp(filename)
            if ts:
                self.processed_radar[filename] = ts

                if latest_timestamp and ts == latest_timestamp:
                    overlay_path = self.config.storage.radar_output_dir / f"radar_{ts.strftime('%Y%m%d_%H%M')}_overlay.png"
                    latest_overlay_exists = overlay_path.exists()

        # Keep processed map constrained to tracked files. Any missing image
        # within the window is regenerated immediately, so historical data stays
        # complete even if the process restarts mid-day.
        self.processed_radar = {fname: ts for fname, ts in self.processed_radar.items() if fname in entries}
        self._prune_radar_outputs()

        return processed_any, latest_timestamp, latest_overlay_exists

    def _process_radar_batch(self, hdf_paths: list[Path]) -> list[datetime]:
        """Process multiple radar files in parallel."""
        if not hdf_paths:
            return []

        # Use batch processing for multiple files
        results = generate_pngs_batch(hdf_paths)

        timestamps = []
        for hdf_path in hdf_paths:
            if hdf_path in results:
                pngs = results[hdf_path]
                timestamp = extract_timestamp(hdf_path.name)
                if not timestamp:
                    raise RuntimeError(f"Unable to parse timestamp from {hdf_path.name}")
                stub = timestamp.strftime("%Y%m%d%H%M")
                self.processed_radar[hdf_path.name] = timestamp
                timestamps.append(timestamp)
                LOGGER.info("Radar %s processed into %d files", stub, len(pngs))
            else:
                LOGGER.warning("No results for %s", hdf_path.name)

        return timestamps

    def _ensure_extended_backlog(self) -> bool:
        """Generate extended overlays for recent radar files if missing."""
        entries = self._radar_entries()
        if not entries:
            return False

        files_to_process: list[Path] = []
        for filename in reversed(entries):
            ts = extract_timestamp(filename)
            if not ts:
                continue

            standard_overlay = self.config.storage.radar_output_dir / f"radar_{ts.strftime('%Y%m%d_%H%M')}_overlay.png"
            if not standard_overlay.exists():
                continue

            extended_overlay = (
                self.config.storage.extended_output_dir / f"radar_{ts.strftime('%Y%m%d_%H%M')}_overlay_extended.png"
            )
            extended_overlay2x = (
                self.config.storage.extended_output_dir / f"radar_{ts.strftime('%Y%m%d_%H%M')}_overlay2x_extended.png"
            )
            if extended_overlay.exists() and extended_overlay2x.exists():
                continue

            local_path = self.config.storage.radar_data_dir / filename
            if not local_path.exists():
                continue

            files_to_process.append(local_path)

        if files_to_process:
            LOGGER.info("Processing %d extended radar files in parallel", len(files_to_process))
            generate_pngs_extended_batch(files_to_process)

        self._prune_extended_outputs()
        return bool(files_to_process)

    def _process_radar(self, hdf_path: Path) -> datetime:
        """Legacy single-file processing method for backward compatibility."""
        pngs = generate_pngs(hdf_path)
        timestamp = extract_timestamp(hdf_path.name)
        if not timestamp:
            raise RuntimeError(f"Unable to parse timestamp from {hdf_path.name}")
        stub = timestamp.strftime("%Y%m%d%H%M")
        self.processed_radar[hdf_path.name] = timestamp
        LOGGER.info("Radar %s processed into %d files", stub, len(pngs))
        return timestamp

    def _download_forecast_tar(self) -> Path | None:
        entries = list_remote_files(self.config.sources.forecast_base_url)
        if not entries:
            LOGGER.debug("No forecast archives available")
            return None
        latest_tar = entries[0]
        if latest_tar in self.completed_forecasts:
            LOGGER.debug("Latest forecast TAR already processed")
            return None
        tar_path = self.config.storage.forecast_data_dir / latest_tar
        if not tar_path.exists():
            download_tar(self.config.sources.forecast_base_url, latest_tar, tar_path)
        return tar_path

    def _process_forecast(self, radar_timestamp: datetime, tar_path: Path) -> None:
        extracted = extract_forecast_tar(tar_path, self.config.storage.forecast_data_dir)
        candidates: list[tuple[int, Path]] = []

        for hdf_file in extracted:
            ts = extract_timestamp(hdf_file.name)
            if not ts:
                LOGGER.debug("Skipping forecast %s (missing timestamp)", hdf_file.name)
                continue

            offset_match = hdf_file.stem.rsplit("_ft", maxsplit=1)
            if len(offset_match) != 2:
                LOGGER.warning("Cannot derive forecast offset from %s", hdf_file.name)
                continue
            try:
                label_offset = int(offset_match[1])
            except ValueError:
                LOGGER.warning("Invalid forecast offset in %s", hdf_file.name)
                continue

            delta_minutes = int(round((ts - radar_timestamp).total_seconds() / 60))
            if delta_minutes < 0:
                LOGGER.debug(
                    "Skipping forecast %s (timestamp %s precedes radar base %s)",
                    hdf_file.name,
                    ts,
                    radar_timestamp,
                )
                continue

            if delta_minutes != label_offset:
                LOGGER.debug(
                    "Forecast %s offset mismatch (timestamp delta %d, label %d)",
                    hdf_file.name,
                    delta_minutes,
                    label_offset,
                )
            final_offset = delta_minutes

            # Skip only if both variants already exist.
            # Use radar_timestamp (forecast generation time) since that's what the PNG filename uses.
            overlay_stub = f"radar_{radar_timestamp.strftime('%Y%m%d_%H%M')}_forecast_fct{final_offset:02d}"
            overlay_path = self.config.storage.forecast_output_dir / f"{overlay_stub}_overlay.png"
            overlay2x_path = self.config.storage.forecast_output_dir / f"{overlay_stub}_overlay2x.png"
            if overlay_path.exists() and overlay2x_path.exists():
                LOGGER.debug("Forecast overlays already exist for offset %d, skipping", final_offset)
                continue

            candidates.append((final_offset, hdf_file))

        candidates.sort(key=lambda item: item[0])

        # Process forecast files in parallel batches
        if candidates:
            forecast_files = [hdf_file for _, hdf_file in candidates]
            forecast_offsets = [offset for offset, _ in candidates]

            LOGGER.info("Processing %d forecast files in parallel", len(forecast_files))
            generate_pngs_batch(forecast_files, forecast=True, offset_minutes_list=forecast_offsets)

        self.completed_forecasts.add(tar_path.name)
        LOGGER.info("Forecast bundle %s processed", tar_path.name)
        self._prune_forecast_outputs()

    def _prune_forecast_outputs(self) -> None:
        limit = self.config.storage.max_forecast_files
        if limit <= 0:
            return
        outputs = sorted(self.config.storage.forecast_output_dir.glob("radar_*_forecast*.png"))
        for path in outputs[:-limit]:
            path.unlink(missing_ok=True)
        archives = sorted(self.config.storage.forecast_data_dir.rglob("*.hdf"))
        for path in archives[:-limit]:
            path.unlink(missing_ok=True)
        tarballs = sorted(self.config.storage.forecast_data_dir.rglob("*.tar"))
        for path in tarballs[:-limit]:
            path.unlink(missing_ok=True)
        for directory in sorted(self.config.storage.forecast_data_dir.glob("**/*"), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()

    def _prune_radar_outputs(self) -> None:
        limit = self.config.storage.max_tracked_files
        if limit <= 0:
            return
        # Use overlay files to determine which timestamps to keep
        overlays = sorted(self.config.storage.radar_output_dir.glob("radar_*_overlay.png"))
        keep_overlays = overlays[-limit:]
        keep_stubs = {
            f"{ov.stem.split('_')[1]}_{ov.stem.split('_')[2]}" for ov in keep_overlays if len(ov.stem.split('_')) >= 3
        }
        # Remove old overlay variants that are no longer in the retention window
        all_overlays = sorted(self.config.storage.radar_output_dir.glob("radar_*.png"))
        for overlay in all_overlays:
            parts = overlay.stem.split("_")
            if len(parts) >= 3 and f"{parts[1]}_{parts[2]}" not in keep_stubs:
                overlay.unlink(missing_ok=True)
        # Clean up legacy background files if any exist
        old_backgrounds = sorted(self.config.storage.radar_output_dir.glob("background_radar_*.png"))
        for path in old_backgrounds:
            path.unlink(missing_ok=True)
        archives = sorted(self.config.storage.radar_data_dir.glob("*.hdf"))
        for path in archives[:-limit]:
            path.unlink(missing_ok=True)

    def _prune_extended_outputs(self) -> None:
        limit = self.config.storage.max_tracked_files
        if limit <= 0:
            return
        # Use standard overlay files to determine which timestamps to keep
        overlays = sorted(self.config.storage.radar_output_dir.glob("radar_*_overlay.png"))
        keep_overlays = overlays[-limit:]
        keep_stubs = {
            f"{ov.stem.split('_')[1]}_{ov.stem.split('_')[2]}" for ov in keep_overlays if len(ov.stem.split('_')) >= 3
        }
        all_overlays = sorted(self.config.storage.extended_output_dir.glob("radar_*.png"))
        for overlay in all_overlays:
            parts = overlay.stem.split("_")
            if len(parts) >= 3 and f"{parts[1]}_{parts[2]}" not in keep_stubs:
                overlay.unlink(missing_ok=True)

    def run_cycle(self) -> bool:
        processed_new, latest_timestamp, latest_ready = self._ensure_radar_backlog()

        # Process forecast if we have new radar data, regardless of whether the latest overlay is ready
        # The forecast processing will handle the timestamp matching internally
        if latest_timestamp:
            tar_path = self._download_forecast_tar()
            if tar_path:
                # Extract timestamp from forecast TAR filename - this is the source of truth for offsets
                # We MUST use the forecast generation timestamp, not the radar timestamp, because:
                # 1. Forecast files contain future timestamps based on when the forecast was generated
                # 2. If radar fetching fails/delays, using radar timestamp would create wrong offsets
                # 3. The forecast TAR filename timestamp is the correct reference point for offset calculations
                forecast_timestamp = extract_forecast_timestamp(tar_path.name)
                if forecast_timestamp:
                    self._process_forecast(forecast_timestamp, tar_path)
                    LOGGER.info("Processing forecast using generation timestamp: %s", forecast_timestamp)
                else:
                    LOGGER.warning("Could not extract timestamp from forecast TAR: %s", tar_path.name)

        # Extended overlays are low priority and run after standard + forecast processing.
        self._ensure_extended_backlog()

        if latest_ready and latest_timestamp:
            self.next_publish = self._calculate_next_expected(latest_timestamp)

        return processed_new

    def step(self, now: datetime) -> None:
        """Advance scheduler state by a single timestep.

        The loop ticks every second. As soon as the wall clock crosses the
        next five-minute boundary we flip into "quick" mode: an immediate fetch
        attempt runs, followed by retries every ``quick_check_interval`` seconds
        until we either observe new radar data or exhaust the configured limit.
        Leaving quick mode updates ``next_publish`` so the next boundary is
        tracked automatically.
        """
        if now >= self.next_publish and not self.quick_mode:
            LOGGER.info("Boundary reached (%s); entering quick polling", self.next_publish.strftime("%H:%M"))
            self.quick_mode = True
            self.quick_attempts = 0
            self.quick_last_attempt = None
            LOGGER.debug("Quick check attempt %d", self.quick_attempts + 1)
            processed = self.run_cycle()
            self.quick_last_attempt = now
            self.quick_attempts += 1
            if processed:
                LOGGER.info("New radar detected; leaving quick mode")
                self.quick_mode = False
                self.next_publish = self._calculate_next_expected(now)
                return

        if self.quick_mode:
            should_attempt = False
            if self.quick_last_attempt is None:
                should_attempt = True
            else:
                elapsed = (now - self.quick_last_attempt).total_seconds()
                if elapsed >= self.config.timing.quick_check_interval:
                    should_attempt = True

            if should_attempt:
                LOGGER.debug("Quick check attempt %d", self.quick_attempts + 1)
                processed = self.run_cycle()
                self.quick_last_attempt = now
                self.quick_attempts += 1
                if processed:
                    LOGGER.info("New radar detected; leaving quick mode")
                    self.quick_mode = False
                    self.next_publish = self._calculate_next_expected(now)
                elif self.quick_attempts >= self.config.timing.quick_check_limit:
                    LOGGER.warning("Quick polling limit reached without new data")
                    self.quick_mode = False
                    self.next_publish = self._calculate_next_expected(now)
        else:
            processed = self.run_cycle()
            if processed:
                self.next_publish = self._calculate_next_expected(now)

    def run_forever(self) -> None:
        LOGGER.info("Starting scheduler loop")

        while True:
            now = datetime.utcnow()
            self.step(now)
            time.sleep(1)


