"""Networking primitives for radar data fetching."""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Iterable, Optional

import requests
from requests import RequestException

LOGGER = logging.getLogger(__name__)
TIMEOUT = 30
MAX_RETRIES = 4
RETRY_DELAY = 2.0

from .network import force_ipv4_connections
force_ipv4_connections()


def _request_with_retry(url: str, stream: bool = False) -> Optional[requests.Response]:
    last_error: RequestException | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response
        except RequestException as exc:
            last_error = exc
            LOGGER.warning("Request failed for %s (attempt %d/%d): %s", url, attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_DELAY * attempt)
    if last_error:
        LOGGER.error("Giving up on %s after %d attempts: %s", url, MAX_RETRIES, last_error)
    return None


def list_remote_files(base_url: str) -> list[str]:
    LOGGER.debug("Listing remote files from %s", base_url)
    response = _request_with_retry(base_url)
    if response is None:
        return []

    entries: list[str] = []
    href_pattern = re.compile(r'href="([^"]+)"', re.IGNORECASE)
    allowed_suffixes = (".hdf", ".tar")

    for line in response.text.splitlines():
        match = href_pattern.search(line)
        if not match:
            continue
        candidate = match.group(1)
        filename = candidate.rsplit("/", maxsplit=1)[-1]
        if filename.lower().endswith(allowed_suffixes):
            entries.append(filename)
    entries.sort(reverse=True)
    return entries


def download_file(base_url: str, filename: str, destination: Path) -> Optional[Path]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = base_url + filename
    LOGGER.info("Downloading %s", url)
    response = _request_with_retry(url, stream=True)
    if response is None:
        if destination.exists():
            destination.unlink(missing_ok=True)
        return None

    with destination.open("wb") as fp:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                fp.write(chunk)
    return destination


def download_tar(base_url: str, filename: str, destination: Path) -> Optional[Path]:
    return download_file(base_url, filename, destination)


def iter_latest(entries: Iterable[str], limit: int) -> list[str]:
    return list(entries)[:limit]


