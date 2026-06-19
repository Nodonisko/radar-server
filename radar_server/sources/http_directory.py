"""Discover radar inputs from a plain HTTP directory listing."""

from __future__ import annotations

import logging
from html.parser import HTMLParser
from urllib.parse import urljoin

from ..config import HttpDirectorySource, InputConfig
from .base import RemoteInputFile, dedupe_remote_files, filename_from_url, request

LOGGER = logging.getLogger(__name__)


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


def discover(input_config: InputConfig, source: HttpDirectorySource) -> list[RemoteInputFile]:
    response = request("GET", source.base_url)
    parser = _HrefParser()
    parser.feed(response.text)

    files: list[RemoteInputFile] = []
    for href in parser.hrefs:
        url = urljoin(source.base_url, href)
        filename = filename_from_url(url)
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
    return dedupe_remote_files(files)
