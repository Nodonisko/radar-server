from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime

from radar_server.config import RetentionPolicy, opera_dbzh
from radar_server.mqtt_watcher import MqttWatcher, _input_matches_topic, _subscription_topics, parse_ord_topic


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


def test_handle_message_emits_notification_without_downloading(tmp_path) -> None:
    timestamp = datetime(2026, 6, 5, 21, 35)
    input_config = replace(opera_dbzh, local_dir=tmp_path / "in", retention=RetentionPolicy(keep_for_seconds=None))
    captured = []
    watcher = MqttWatcher(inputs=(input_config,), on_notification=captured.append)
    payload = {
        "links": [
            {
                "href": "https://example.test/OPERA@20260605T2135@0@DBZH.h5",
                "type": "application/x-odim",
                "title": "Data download link.",
            }
        ]
    }

    notifications = watcher.handle_message(
        "ORD/eu.eumetnet/0-20010-0-OPERA/DBZH",
        json.dumps(payload).encode("utf-8"),
    )

    assert len(notifications) == 1
    notification = notifications[0]
    assert notification.input is input_config
    assert [remote.filename for remote in notification.remotes] == ["OPERA@20260605T2135@0@DBZH.h5"]
    assert notification.remotes[0].timestamp == timestamp
    assert captured == [notification]
    assert watcher.last_message_at is not None
    # The network-thread callback must not touch the filesystem.
    assert not input_config.local_dir.exists()


def test_handle_message_ignores_non_matching_topic(tmp_path) -> None:
    input_config = replace(opera_dbzh, local_dir=tmp_path / "in")
    captured = []
    watcher = MqttWatcher(inputs=(input_config,), on_notification=captured.append)

    notifications = watcher.handle_message(
        "ORD/eu.eumetnet/0-20000-0-OTHER/DBZH",
        json.dumps({"links": []}).encode("utf-8"),
    )

    assert notifications == ()
    assert captured == []
