"""Fetch configured radar inputs into local timestamped files.

This module stops at the input boundary: it discovers remote files for an
``InputConfig`` and materializes them locally. A later orchestration layer can
index the returned records and decide which products are ready to render.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from requests import RequestException

from .config import (
    GeoBounds,
    HttpDirectorySource,
    InputConfig,
    OrdApiSource,
    OrdItemsQuery,
    OrdLocationQuery,
)

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


@dataclass(frozen=True)
class InputSyncResult:
    input: InputConfig
    files: tuple[LocalInputFile, ...] = ()
    error: FetchError | None = None


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)


def discover_remote_files(
    input_config: InputConfig,
    *,
    now: datetime | None = None,
    limit: int | None = None,
) -> list[RemoteInputFile]:
    """Discover remote files for one configured input without downloading them."""

    if not input_config.enabled:
        return []

    source = input_config.source
    if isinstance(source, HttpDirectorySource):
        files = _discover_http_directory(input_config, source)
    elif isinstance(source, OrdApiSource):
        files = _discover_ord(input_config, source, now=now, limit=limit)
    else:
        raise TypeError(f"unsupported source config: {source!r}")

    files.sort(key=lambda item: item.timestamp, reverse=True)
    if limit is not None:
        return files[:limit]
    return files


def sync_input(
    input_config: InputConfig,
    *,
    now: datetime | None = None,
    limit: int | None = None,
) -> list[LocalInputFile]:
    """Discover and download missing files for one input."""

    remotes = discover_remote_files(input_config, now=now, limit=limit)
    return [download_remote_file(remote) for remote in remotes]


def sync_inputs(
    input_configs: list[InputConfig] | tuple[InputConfig, ...],
    *,
    now: datetime | None = None,
    limit_per_input: int | None = None,
) -> list[InputSyncResult]:
    """Sync multiple inputs, isolating failures to the input that failed."""

    results: list[InputSyncResult] = []
    for input_config in input_configs:
        try:
            files = sync_input(input_config, now=now, limit=limit_per_input)
        except FetchError as exc:
            LOGGER.warning("Input %s sync failed: %s", input_config.id, exc)
            results.append(InputSyncResult(input_config, error=exc))
        else:
            results.append(InputSyncResult(input_config, files=tuple(files)))
    return results


def download_remote_file(remote: RemoteInputFile) -> LocalInputFile:
    """Download one remote input file if it is not already present locally."""

    destination = remote.input.local_dir / remote.filename
    if destination.exists() and destination.stat().st_size > 0:
        return LocalInputFile(remote.input, remote.timestamp, destination, remote, downloaded=False)

    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_name(f"{destination.name}.part")
    LOGGER.info("Downloading %s -> %s", remote.url, destination)
    response = _request("GET", remote.url, stream=True)
    with tmp_path.open("wb") as fp:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                fp.write(chunk)
    tmp_path.replace(destination)
    return LocalInputFile(remote.input, remote.timestamp, destination, remote, downloaded=True)


def remote_files_from_ord_payload(
    input_config: InputConfig,
    payload: dict[str, Any],
    *,
    quantity: str,
) -> list[RemoteInputFile]:
    """Extract ODIM download links from an ORD CoverageJSON-style payload."""

    return _remote_files_from_ord_payload(input_config, payload, quantity=quantity)


def _discover_http_directory(input_config: InputConfig, source: HttpDirectorySource) -> list[RemoteInputFile]:
    response = _request("GET", source.base_url)
    parser = _HrefParser()
    parser.feed(response.text)

    files: list[RemoteInputFile] = []
    for href in parser.hrefs:
        url = urljoin(source.base_url, href)
        filename = _filename_from_url(url)
        if not filename.lower().endswith(input_config.file_suffixes):
            continue
        timestamp = input_config.timestamp_from_name(filename)
        if timestamp is None:
            LOGGER.debug("Skipping %s: cannot derive timestamp", filename)
            continue
        files.append(
            RemoteInputFile(
                input=input_config,
                timestamp=timestamp,
                url=url,
                filename=filename,
                metadata={"source": source.id},
            )
        )
    return _dedupe_remote_files(files)


def _discover_ord(
    input_config: InputConfig,
    source: OrdApiSource,
    *,
    now: datetime | None,
    limit: int | None,
) -> list[RemoteInputFile]:
    query = input_config.remote_query
    if isinstance(query, OrdLocationQuery):
        return _discover_ord_location(input_config, source, query, now=now)
    if isinstance(query, OrdItemsQuery):
        return _discover_ord_items(input_config, source, query, now=now, limit=limit)
    raise ValueError(f"ORD input {input_config.id!r} needs an ORD query config")


def _discover_ord_location(
    input_config: InputConfig,
    source: OrdApiSource,
    query: OrdLocationQuery,
    *,
    now: datetime | None,
) -> list[RemoteInputFile]:
    url = f"{source.api_base_url}/collections/observations/locations/{query.location_id}"
    lookback_minutes = max(query.lookback_minutes, _input_lookback_minutes(input_config))
    payload = _request_json(
        url,
        params={
            "datetime": _datetime_window(now, lookback_minutes),
            "standard_name": query.standard_name,
            "format": query.fmt,
            "method": query.method,
        },
        headers=_ord_headers(source),
    )
    return _remote_files_from_ord_payload(input_config, payload, quantity=query.standard_name)


def _discover_ord_items(
    input_config: InputConfig,
    source: OrdApiSource,
    query: OrdItemsQuery,
    *,
    now: datetime | None,
    limit: int | None,
) -> list[RemoteInputFile]:
    lookback_minutes = max(query.lookback_minutes, _input_lookback_minutes(input_config))
    payload = _request_json(
        f"{source.api_base_url}/collections/observations/items",
        params={
            "bbox": _format_bounds(query.bbox),
            "datetime": _datetime_window(now, lookback_minutes),
            "standard-name": query.standard_name,
            "format": query.fmt,
            "method": query.method,
            **({"naming-authority": query.naming_authority} if query.naming_authority else {}),
        },
        headers=_ord_headers(source),
    )
    files: list[RemoteInputFile] = []
    for feature in payload.get("features", []):
        properties = feature.get("properties", {})
        if not _feature_matches_items_query(properties, query):
            continue
        data_url = properties.get("data")
        if not isinstance(data_url, str):
            continue
        try:
            data_payload = _request_json(
                _with_query_params(
                    data_url,
                    {
                        "datetime": _datetime_window(now, lookback_minutes),
                        "format": query.fmt,
                        "method": query.method,
                    },
                ),
                headers=_ord_headers(source),
            )
        except FetchError as exc:
            LOGGER.warning("Skipping ORD detail lookup for %s: %s", data_url, exc)
            continue
        files.extend(_remote_files_from_ord_payload(input_config, data_payload, quantity=query.standard_name))
        if limit is not None and len(files) >= limit:
            break
    return _dedupe_remote_files(files)


def _remote_files_from_ord_payload(
    input_config: InputConfig,
    payload: dict[str, Any],
    *,
    quantity: str,
) -> list[RemoteInputFile]:
    files: list[RemoteInputFile] = []
    for link in _iter_ord_links(payload):
        href = link.get("href")
        if not isinstance(href, str):
            continue
        filename = _filename_from_url(href)
        if not filename.lower().endswith(input_config.file_suffixes):
            continue
        if f"@{quantity}." not in filename and f"@{quantity}@" not in filename:
            continue
        timestamp = input_config.timestamp_from_name(filename)
        if timestamp is None:
            LOGGER.debug("Skipping %s: cannot derive timestamp", filename)
            continue
        files.append(
            RemoteInputFile(
                input=input_config,
                timestamp=timestamp,
                url=href,
                filename=filename,
                metadata={
                    "length": link.get("length"),
                    "type": link.get("type"),
                    "title": link.get("title"),
                },
            )
        )
    return _dedupe_remote_files(files)


def _iter_ord_links(payload: dict[str, Any]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for link in payload.get("links", []):
        if isinstance(link, dict) and link.get("type") == "application/x-odim":
            links.append(link)
    for coverage in payload.get("coverages", []):
        if isinstance(coverage, dict):
            links.extend(_iter_ord_links(coverage))
    return links


def _feature_matches_items_query(properties: dict[str, Any], query: OrdItemsQuery) -> bool:
    if properties.get("standard_name") != query.standard_name:
        return False
    if properties.get("format") != query.fmt:
        return False
    if properties.get("method") != query.method:
        return False
    if query.naming_authority and properties.get("naming_authority") != query.naming_authority:
        return False
    if query.platform_code_prefixes:
        platform_name = str(properties.get("platform_name") or "").strip("[]")
        if not platform_name.startswith(query.platform_code_prefixes):
            return False
    return True


def _request_json(
    url: str,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    request_headers = {"accept": "application/json", **(headers or {})}
    response = _request("GET", url, params=params, headers=request_headers)
    if response.status_code == 204 or not response.content:
        return {}
    return response.json()


def _request(method: str, url: str, **kwargs: Any) -> requests.Response:
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


def _ord_headers(source: OrdApiSource) -> dict[str, str]:
    api_key = source.api_key()
    if not api_key:
        return {}
    return {source.api_key_header: api_key}


def _input_lookback_minutes(input_config: InputConfig) -> int:
    keep_for = input_config.retention.keep_for_seconds
    if keep_for is None:
        return 0
    return max(1, int(keep_for // 60))


def _datetime_window(now: datetime | None, lookback_minutes: int) -> str:
    end = _as_utc(now or datetime.now(timezone.utc))
    start = end - timedelta(minutes=lookback_minutes)
    return f"{_format_ord_time(start)}/{_format_ord_time(end)}"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_ord_time(value: datetime) -> str:
    return value.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_bounds(bounds: GeoBounds) -> str:
    return f"{bounds.west},{bounds.south},{bounds.east},{bounds.north}"


def _filename_from_url(url: str) -> str:
    return Path(urlsplit(url).path).name


def _with_query_params(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _dedupe_remote_files(files: list[RemoteInputFile]) -> list[RemoteInputFile]:
    by_url: dict[str, RemoteInputFile] = {}
    for item in files:
        by_url[item.url] = item
    return list(by_url.values())
