from __future__ import annotations

import json
from datetime import datetime

from radar_server.config import opera_dbzh
from radar_server.fetching import LocalInputFile
from radar_server.mqtt_watcher import MqttWatcher, _input_matches_topic, _subscription_topics, parse_ord_topic
from radar_server.registry import InputRegistry


def test_parse_ord_topic() -> None:
    parsed = parse_ord_topic("ORD/eu.eumetnet/0-20010-0-OPERA/DBZH")

    assert parsed is not None
    assert parsed.naming_authority == "eu.eumetnet"
    assert parsed.wigos_id == "0-20010-0-OPERA"
    assert parsed.quantity == "DBZH"
    assert parse_ord_topic("not/a/valid/topic") is None


def test_input_matches_ord_location_topic() -> None:
    assert _input_matches_topic(opera_dbzh, "ORD/eu.eumetnet/0-20010-0-OPERA/DBZH")
    assert not _input_matches_topic(opera_dbzh, "ORD/eu.eumetnet/0-20010-0-OPERA/RATE")
    assert not _input_matches_topic(opera_dbzh, "ORD/eu.eumetnet/0-20000-0-OTHER/DBZH")


def test_subscription_topics_prefers_explicit_configured_topic() -> None:
    policy = opera_dbzh.source.notifications[0]

    topics = _subscription_topics((opera_dbzh,), policy)

    assert topics == ("ORD/eu.eumetnet/0-20010-0-OPERA/DBZH",)


def test_handle_message_downloads_matching_ord_payload(monkeypatch, tmp_path) -> None:
    timestamp = datetime(2026, 6, 5, 21, 35)
    destination = tmp_path / "OPERA@20260605T2135@0@DBZH.h5"
    registry = InputRegistry()
    watcher = MqttWatcher(inputs=(opera_dbzh,), products=(), registry=registry)
    rendered_calls = []

    def fake_download(remote):
        destination.write_bytes(b"hdf")
        return LocalInputFile(opera_dbzh, timestamp, destination, remote, downloaded=True)

    def fake_render(registry, products):
        rendered_calls.append((registry, tuple(products)))
        return []

    monkeypatch.setattr("radar_server.mqtt_watcher.download_remote_file", fake_download)
    monkeypatch.setattr("radar_server.mqtt_watcher.render_ready_jobs", fake_render)
    monkeypatch.setattr("radar_server.mqtt_watcher.prune_all", lambda **kwargs: None)
    payload = {
        "links": [
            {
                "href": "https://example.test/OPERA@20260605T2135@0@DBZH.h5",
                "type": "application/x-odim",
                "title": "Data download link.",
            }
        ]
    }

    result = watcher.handle_message(
        "ORD/eu.eumetnet/0-20010-0-OPERA/DBZH",
        json.dumps(payload).encode("utf-8"),
    )

    assert result.matched_inputs == (opera_dbzh,)
    assert len(result.downloaded) == 1
    assert registry.timestamps_for(opera_dbzh) == {timestamp}
    assert len(rendered_calls) == 1
