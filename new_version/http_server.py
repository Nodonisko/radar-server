"""Minimal static file server for local development."""

from __future__ import annotations

import contextlib
import http.server
import logging
import urllib.parse
from pathlib import PurePosixPath

from .config import CONFIG

LOGGER = logging.getLogger(__name__)


class RadarStaticRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Serve files from the radar output directories."""

    MOUNT_POINTS = {
        "output": CONFIG.storage.radar_output_dir,
        "output_forecast": CONFIG.storage.forecast_output_dir,
        "output_extended": CONFIG.storage.extended_output_dir,
    }

    def translate_path(self, path: str) -> str:  # pragma: no cover - thin wrapper
        parsed = urllib.parse.urlparse(path)
        parts = [part for part in PurePosixPath(parsed.path).parts if part not in {"", "/"}]

        if not parts:
            return str(CONFIG.storage.radar_output_dir / "__not_found__")

        mount, remainder = parts[0], parts[1:]
        root = self.MOUNT_POINTS.get(mount)
        if root is None:
            return str(CONFIG.storage.radar_output_dir / "__not_found__")

        safe_path = root.joinpath(*remainder)
        return str(safe_path)


class RadarStaticServer:
    """Lightweight HTTP server exposing processed assets locally."""

    def __init__(self) -> None:
        self.host = CONFIG.dev_server.host
        self.port = CONFIG.dev_server.port
        self._httpd: http.server.ThreadingHTTPServer | None = None

    def start(self) -> None:
        if self._httpd is not None:
            raise RuntimeError("Static server already running")

        for path in RadarStaticRequestHandler.MOUNT_POINTS.values():
            path.mkdir(parents=True, exist_ok=True)

        handler = RadarStaticRequestHandler
        self._httpd = http.server.ThreadingHTTPServer((self.host, self.port), handler)
        self._httpd.daemon_threads = True
        self._httpd.allow_reuse_address = True
        LOGGER.info(
            "Serving radar outputs at http://%s:%d (mounts: %s)",
            self.host,
            self.port,
            ", ".join(sorted(RadarStaticRequestHandler.MOUNT_POINTS)),
        )

    def serve_forever(self) -> None:
        if self._httpd is None:
            raise RuntimeError("Static server not started")

        with contextlib.suppress(KeyboardInterrupt):
            self._httpd.serve_forever()

    def shutdown(self) -> None:
        if self._httpd is None:
            return
        LOGGER.info("Stopping static server")
        self._httpd.shutdown()
        self._httpd.server_close()
        self._httpd = None

