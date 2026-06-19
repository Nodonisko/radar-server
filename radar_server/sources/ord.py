"""Discover radar inputs from the EUMETNET ORD (OPERA) API."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..config import (
    GeoBounds,
    InputConfig,
    OrdApiSource,
    OrdItemsQuery,
    OrdLocationQuery,
)
from .base import (
    FetchError,
    RemoteInputFile,
    as_utc,
    dedupe_remote_files,
    filename_from_url,
    request_json,
    with_query_params,
)

LOGGER = logging.getLogger(__name__)


def discover(
    input_config: InputConfig,
    source: OrdApiSource,
    *,
    now: datetime | None,
    limit: int | None,
) -> list[RemoteInputFile]:
    query = input_config.remote_query
    if isinstance(query, OrdLocationQuery):
        return _discover_location(input_config, source, query, now=now)
    if isinstance(query, OrdItemsQuery):
        return _discover_items(input_config, source, query, now=now, limit=limit)
    raise ValueError(f"ORD input {input_config.id!r} needs an ORD query config")


def remote_files_from_payload(
    input_config: InputConfig,
    payload: dict[str, Any],
    *,
    quantity: str,
) -> list[RemoteInputFile]:
    """Extract ODIM download links from an ORD CoverageJSON-style payload."""

    files: list[RemoteInputFile] = []
    for link in _iter_links(payload):
        href = link.get("href")
        if not isinstance(href, str):
            continue
        filename = filename_from_url(href)
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
    return dedupe_remote_files(files)


def _discover_location(
    input_config: InputConfig,
    source: OrdApiSource,
    query: OrdLocationQuery,
    *,
    now: datetime | None,
) -> list[RemoteInputFile]:
    url = f"{source.api_base_url}/collections/observations/locations/{query.location_id}"
    lookback_minutes = max(query.lookback_minutes, _input_lookback_minutes(input_config))
    payload = request_json(
        url,
        params={
            "datetime": _datetime_window(now, lookback_minutes),
            "standard_name": query.standard_name,
            "format": query.fmt,
            "method": query.method,
        },
        headers=_headers(source),
    )
    return remote_files_from_payload(input_config, payload, quantity=query.standard_name)


def _discover_items(
    input_config: InputConfig,
    source: OrdApiSource,
    query: OrdItemsQuery,
    *,
    now: datetime | None,
    limit: int | None,
) -> list[RemoteInputFile]:
    lookback_minutes = max(query.lookback_minutes, _input_lookback_minutes(input_config))
    payload = request_json(
        f"{source.api_base_url}/collections/observations/items",
        params={
            "bbox": _format_bounds(query.bbox),
            "datetime": _datetime_window(now, lookback_minutes),
            "standard-name": query.standard_name,
            "format": query.fmt,
            "method": query.method,
            **({"naming-authority": query.naming_authority} if query.naming_authority else {}),
        },
        headers=_headers(source),
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
            data_payload = request_json(
                with_query_params(
                    data_url,
                    {
                        "datetime": _datetime_window(now, lookback_minutes),
                        "format": query.fmt,
                        "method": query.method,
                    },
                ),
                headers=_headers(source),
            )
        except FetchError as exc:
            LOGGER.warning("Skipping ORD detail lookup for %s: %s", data_url, exc)
            continue
        files.extend(remote_files_from_payload(input_config, data_payload, quantity=query.standard_name))
        if limit is not None and len(files) >= limit:
            break
    return dedupe_remote_files(files)


def _iter_links(payload: dict[str, Any]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for link in payload.get("links", []):
        if isinstance(link, dict) and link.get("type") == "application/x-odim":
            links.append(link)
    for coverage in payload.get("coverages", []):
        if isinstance(coverage, dict):
            links.extend(_iter_links(coverage))
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


def _headers(source: OrdApiSource) -> dict[str, str]:
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
    end = as_utc(now or datetime.now(timezone.utc))
    start = end - timedelta(minutes=lookback_minutes)
    return f"{_format_time(start)}/{_format_time(end)}"


def _format_time(value: datetime) -> str:
    return value.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_bounds(bounds: GeoBounds) -> str:
    return f"{bounds.west},{bounds.south},{bounds.east},{bounds.north}"
