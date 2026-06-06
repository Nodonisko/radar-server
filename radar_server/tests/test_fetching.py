from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from radar_server.config import RetentionPolicy, cz_maxz
from radar_server.fetching import RemoteInputFile, sync_input


def test_sync_input_filters_discovered_files_outside_retention(monkeypatch, tmp_path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path, retention=RetentionPolicy(keep_for_seconds=7200))
    now = datetime(2026, 6, 5, 21, 5)
    old = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 5, 18, 55),
        url="https://example.test/old.hdf",
        filename="old.hdf",
        metadata={},
    )
    recent = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 5, 19, 5),
        url="https://example.test/recent.hdf",
        filename="recent.hdf",
        metadata={},
    )
    downloaded = []

    monkeypatch.setattr("radar_server.fetching.discover_remote_files", lambda *args, **kwargs: [old, recent])

    def fake_download(remote):
        downloaded.append(remote)
        return remote

    monkeypatch.setattr("radar_server.fetching.download_remote_file", fake_download)

    sync_input(input_config, now=now)

    assert downloaded == [recent]
