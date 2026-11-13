from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from threading import Event

from radar_server.config import RetentionPolicy, opera_dbzh
from radar_server.mqtt_watcher import MqttWatcher, _input_matches_topic, _subscription_topics, parse_ord_topic


def test_parse_ord_topic() -> None:
    parsed = parse_ord_topic("ORD/eu.eumetnet/0-20010-0-OPERA/DBZH")

    assert parsed is not None
    assert parsed.naming_authority == "eu.eumetnet"
    assert parsed.wigos_id == "0-20010-0-OPERA"
    assert parsed.quantity == "DBZH"
    assert parse_ord_topic("not/a/valid/topic") is None


def test_start_retries_broker_timeouts_in_background(monkeypatch) -> None:
    from paho.mqtt.client import Client

    first_failure = Event()
    retry = Event()
    attempts = []

    def unreachable_broker(client):
        attempts.append(client)
        if len(attempts) == 1:
            first_failure.set()
        else:
            retry.set()
        raise TimeoutError("broker unavailable")

    monkeypatch.setattr(Client, "_create_socket_connection", unreachable_broker)
    watcher = MqttWatcher(inputs=(opera_dbzh,))
    client = watcher.start()
    client.reconnect_delay_set(min_delay=1, max_delay=1)
    try:
        assert first_failure.wait(timeout=2)
        # Even the first connection failure must leave the loop retrying.
        assert retry.wait(timeout=7)
        assert not watcher.connected
        assert watcher.is_stale()
        assert client._thread.is_alive()
    finally:
        watcher.stop()


def test_rejected_connection_does_not_mark_watcher_connected() -> None:
    from paho.mqtt.reasoncodes import ReasonCode
    from paho.mqtt.packettypes import PacketTypes
    from unittest.mock import Mock

    watcher = MqttWatcher(inputs=(opera_dbzh,))
    client = watcher._build_client(watcher.default_policy())
    subscriber = Mock()
    client.on_connect(subscriber, None, None, ReasonCode(PacketTypes.CONNACK, "Not authorized"), None)

    assert not watcher.connected
    assert watcher.is_stale()
    subscriber.subscribe.assert_not_called()


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
