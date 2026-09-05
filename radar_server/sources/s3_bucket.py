"""Discover radar inputs from a public S3-compatible object listing.

Used for the EUMETNET ORD rolling bucket on CloudFerro, where OPERA composites
are date-partitioned and filename-predictable. Discovery uses anonymous
ListObjectsV2; downloads are plain HTTPS GETs of object URLs.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import quote, urlencode

from ..config import InputConfig, S3BucketSource, S3PrefixQuery
from .base import RemoteInputFile, as_utc, dedupe_remote_files, filename_from_url, request

LOGGER = logging.getLogger(__name__)

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def discover(
    input_config: InputConfig,
    source: S3BucketSource,
    *,
    now: datetime | None,
    limit: int | None,
) -> list[RemoteInputFile]:
    query = input_config.remote_query
    if not isinstance(query, S3PrefixQuery):
        raise ValueError(f"S3 input {input_config.id!r} needs an S3PrefixQuery remote_query")

    reference = as_utc(now or datetime.now(timezone.utc))
    # Discovery window follows input retention (no separate lookback knob).
    keep_for = input_config.retention.keep_for_seconds
    cutoff = None if keep_for is None else reference - timedelta(seconds=keep_for)
    prefix_start = cutoff if cutoff is not None else reference

    files: list[RemoteInputFile] = []
    for day_prefix in _day_prefixes(prefix_start, reference, query.key_infix):
        for key in _list_object_keys(source, day_prefix):
            filename = filename_from_url(key)
            if not filename.lower().endswith(input_config.file_suffixes):
                continue
            if query.name_contains and query.name_contains not in filename:
                continue
            quantity = input_config.quantity
            if f"@{quantity}." not in filename and f"@{quantity}@" not in filename:
                continue
            timestamp = input_config.timestamp_from_name(filename)
            if timestamp is None:
                LOGGER.debug("Skipping %s: cannot derive timestamp", filename)
                continue
            if cutoff is not None and timestamp.replace(tzinfo=timezone.utc) < cutoff:
                continue
            files.append(
                RemoteInputFile(
                    input=input_config,
                    timestamp=timestamp,
                    url=_object_url(source, key),
                    filename=filename,
                    metadata={"source": source.id, "key": key},
                )
            )
            if limit is not None and len(files) >= limit:
                return dedupe_remote_files(files)
    return dedupe_remote_files(files)


def _day_prefixes(start: datetime, end: datetime, key_infix: str) -> list[str]:
    """Yield ``YYYY/MM/DD/{infix}/`` prefixes covering [start, end] UTC days."""

    day = start.astimezone(timezone.utc).date()
    last = end.astimezone(timezone.utc).date()
    infix = key_infix.strip("/")
    prefixes: list[str] = []
    while day <= last:
        prefixes.append(f"{day:%Y/%m/%d}/{infix}/")
        day += timedelta(days=1)
    return prefixes


def _list_object_keys(source: S3BucketSource, prefix: str) -> Iterable[str]:
    continuation: str | None = None
    while True:
        params: dict[str, str] = {
            "list-type": "2",
            "prefix": prefix,
            "max-keys": "1000",
        }
        if continuation:
            params["continuation-token"] = continuation
        url = f"{source.endpoint_url.rstrip('/')}/{source.bucket}?{urlencode(params)}"
        response = request("GET", url)
        root = ET.fromstring(response.content)
        for contents in root.findall("s3:Contents", _S3_NS):
            key = contents.findtext("s3:Key", default="", namespaces=_S3_NS)
            if key:
                yield key
        truncated = root.findtext("s3:IsTruncated", default="false", namespaces=_S3_NS)
        if truncated.lower() != "true":
            return
        continuation = root.findtext("s3:NextContinuationToken", default="", namespaces=_S3_NS)
        if not continuation:
            return


def _object_url(source: S3BucketSource, key: str) -> str:
    # Keep ``@`` unencoded to match the public ORD object URLs used elsewhere.
    return f"{source.endpoint_url.rstrip('/')}/{source.bucket}/{quote(key, safe='/@')}"
