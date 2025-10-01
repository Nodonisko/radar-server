"""CLI entrypoint for the rewritten radar processor."""

from __future__ import annotations

import logging
import platform
import sys
import threading

from .config import CONFIG
from .http_server import RadarStaticServer
from .network import force_ipv4_connections
from .scheduler import RadarScheduler


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def _maybe_start_dev_server() -> RadarStaticServer | None:
    should_start = True
    if CONFIG.dev_server.enabled_only_on_macos and platform.system() != "Darwin":
        should_start = False

    if not should_start:
        logging.getLogger(__name__).info("Skipping static server (non-macOS host)")
        return None

    server = RadarStaticServer()
    try:
        server.start()
    except OSError as exc:
        logging.getLogger(__name__).warning("Static server unavailable: %s", exc)
        return None

    thread = threading.Thread(target=server.serve_forever, name="radar-static-server", daemon=True)
    thread.start()
    return server


def main() -> None:
    configure_logging()
    CONFIG.ensure_directories()
    force_ipv4_connections()

    dev_server = _maybe_start_dev_server()

    scheduler = RadarScheduler()
    try:
        scheduler.run_forever()
    finally:
        if dev_server is not None:
            dev_server.shutdown()


if __name__ == "__main__":
    main()


