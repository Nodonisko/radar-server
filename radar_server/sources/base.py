"""Shared primitives for the input fetching layer.

Records, errors, and low-level HTTP helpers used by every per-source module in
this package and by the :mod:`radar_server.fetching` orchestrator. Keeping these
here (rather than in ``fetching.py``) lets the source modules depend on the
shared pieces without importing the orchestrator, avoiding a circular import.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from requests import RequestException

from ..config import InputConfig

LOGGER = logging.getLogger(__name__)

TIMEOUT_SECONDS = 30
MAX_RETRIES = 4
RETRY_DELAY_SECONDS = 2.0
CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class RemoteInputFile:
    input: InputConfig
    timestamp: datetime
    url: str
    filename: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class LocalInputFile:
    input: InputConfig
    timestamp: datetime
    path: Path
    remote: RemoteInputFile
    downloaded: bool


class FetchError(RuntimeError):
    """Base error raised by the input fetching layer."""


class RateLimitedError(FetchError):
    """The upstream service rejected the request due to rate limiting."""


class SkippableFetchError(FetchError):
    """A single remote file is unavailable but the rest of the batch is fine.

    Raised when an expected ARCO frame chunk is missing (a rare radar gap):
    the sync skips just that frame instead of failing the whole input.
    """


def request(method: str, url: str, **kwargs: Any) -> requests.Response:
    """Issue an HTTP request with bounded retries and rate-limit detection."""

    last_error: RequestException | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, timeout=TIMEOUT_SECONDS, **kwargs)
            if response.status_code == 204:
                return response
            if response.status_code == 429:
                raise RateLimitedError(f"rate limited by remote service for {url!r}")
            response.raise_for_status()
            return response
        except FetchError:
            raise
        except RequestException as exc:
            last_error = exc
            LOGGER.warning("Request failed for %s (attempt %d/%d): %s", url, attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise FetchError(f"giving up on {url!r} after {MAX_RETRIES} attempts") from last_error


def request_json(
    url: str,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    auth: tuple[str, str] | None = None,
) -> dict[str, Any]:
    request_headers = {"accept": "application/json", **(headers or {})}
    kwargs: dict[str, Any] = {"params": params, "headers": request_headers}
    if auth is not None:
        kwargs["auth"] = auth
    response = request("GET", url, **kwargs)
    if response.status_code == 204 or not response.content:
        return {}
    return response.json()


def filename_from_url(url: str) -> str:
    return Path(urlsplit(url).path).name


def with_query_params(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def dedupe_remote_files(files: list[RemoteInputFile]) -> list[RemoteInputFile]:
    by_url: dict[str, RemoteInputFile] = {}
    for item in files:
        by_url[item.url] = item
    return list(by_url.values())


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
