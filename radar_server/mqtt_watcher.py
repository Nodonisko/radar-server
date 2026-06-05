"""MQTT event source for ORD radar notifications.

MQTT is preferred over polling when available: a notification payload already
contains direct ODIM links, so the watcher can download the file and feed it
into the same registry/render path as polling.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Iterable

from .config import CONFIG, InputConfig, NotificationPolicy, OrdApiSource, OrdLocationQuery, ProductConfig
from .fetching import LocalInputFile, download_remote_file, remote_files_from_ord_payload
from .registry import InputRegistry
from .render_jobs import render_ready_jobs
from .rendering.pipeline import RenderResult

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrdTopic:
    naming_authority: str
    wigos_id: str
    quantity: str


@dataclass(frozen=True)
class MqttHandleResult:
    topic: str
    matched_inputs: tuple[InputConfig, ...]
    downloaded: tuple[LocalInputFile, ...]
    rendered: tuple[RenderResult, ...]


class MqttWatcher:
    def __init__(
        self,
        *,
        inputs: Iterable[InputConfig] = CONFIG.inputs,
        products: Iterable[ProductConfig] = CONFIG.products,
        registry: InputRegistry | None = None,
        lock: RLock | None = None,
    ) -> None:
        self.inputs = tuple(inputs)
        self.products = tuple(products)
        self.registry = registry or InputRegistry.from_local_inputs(self.inputs)
        self.lock = lock or RLock()
        self.connected = False
        self.last_message_at: datetime | None = None
        self._client = None

    def handle_message(self, topic: str, payload: bytes | str) -> MqttHandleResult:
        with self.lock:
            self.last_message_at = datetime.utcnow()
            payload_json = _decode_payload(payload)
            matched_inputs = tuple(input_config for input_config in self.inputs if _input_matches_topic(input_config, topic))

            downloaded: list[LocalInputFile] = []
            for input_config in matched_inputs:
                for remote in remote_files_from_ord_payload(input_config, payload_json, quantity=input_config.quantity):
                    downloaded.append(download_remote_file(remote))

            if downloaded:
                self.registry.add(downloaded)
                self.registry.prune(self.inputs)
            rendered = tuple(render_ready_jobs(self.registry, self.products))
        return MqttHandleResult(
            topic=topic,
            matched_inputs=matched_inputs,
            downloaded=tuple(downloaded),
            rendered=rendered,
        )

    def is_stale(self, *, now: datetime | None = None, stale_after_seconds: int = 600) -> bool:
        if not self.connected:
            return True
        if self.last_message_at is None:
            return False
        reference = now or datetime.utcnow()
        return reference - self.last_message_at > timedelta(seconds=stale_after_seconds)

    def start(self, policy: NotificationPolicy | None = None):
        policy = policy or _default_mqtt_policy(self.inputs)
        if policy is None:
            raise ValueError("no MQTT notification policy configured")

        client = self._build_client(policy)
        client.connect(policy.host, policy.port)
        client.loop_start()
        self._client = client
        return client

    def stop(self) -> None:
        if self._client is None:
            return
        self._client.loop_stop()
        self._client.disconnect()
        self._client = None

    def run_forever(self, policy: NotificationPolicy | None = None) -> None:
        policy = policy or _default_mqtt_policy(self.inputs)
        if policy is None:
            raise ValueError("no MQTT notification policy configured")

        client = self._build_client(policy)
        client.connect(policy.host, policy.port)
        self._client = client
        client.loop_forever(retry_first_connection=True)

    def _build_client(self, policy: NotificationPolicy):
        mqtt = _import_paho_mqtt()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
        client.reconnect_delay_set(min_delay=5, max_delay=300)
        if policy.path:
            client.ws_set_options(path=policy.path)
        if policy.tls:
            client.tls_set()
        if policy.username is not None:
            client.username_pw_set(policy.username, policy.password)

        topics = _subscription_topics(self.inputs, policy)

        def on_connect(client, userdata, flags, reason_code, properties):  # noqa: ANN001
            self.connected = True
            LOGGER.info("Connected to MQTT %s:%s (%s)", policy.host, policy.port, reason_code)
            for topic in topics:
                LOGGER.info("Subscribing to %s", topic)
                client.subscribe(topic)

        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):  # noqa: ANN001
            self.connected = False
            LOGGER.warning("Disconnected from MQTT %s:%s (%s)", policy.host, policy.port, reason_code)

        def on_message(client, userdata, message):  # noqa: ANN001
            try:
                result = self.handle_message(message.topic, message.payload)
            except Exception:
                LOGGER.exception("Failed to handle MQTT message on %s", message.topic)
                return
            LOGGER.info(
                "MQTT %s matched %d inputs, downloaded %d files, rendered %d products",
                result.topic,
                len(result.matched_inputs),
                len(result.downloaded),
                len(result.rendered),
            )

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        return client


def parse_ord_topic(topic: str) -> OrdTopic | None:
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != "ORD":
        return None
    return OrdTopic(naming_authority=parts[1], wigos_id=parts[2], quantity=parts[3])


def _decode_payload(payload: bytes | str) -> dict:
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    decoded = json.loads(text)
    if not isinstance(decoded, dict):
        raise ValueError("MQTT payload must decode to a JSON object")
    return decoded


def _input_matches_topic(input_config: InputConfig, topic: str) -> bool:
    parsed = parse_ord_topic(topic)
    if parsed is None:
        return False
    if parsed.quantity != input_config.quantity:
        return False
    if not isinstance(input_config.source, OrdApiSource):
        return False
    query = input_config.remote_query
    if isinstance(query, OrdLocationQuery):
        return parsed.wigos_id == query.location_id
    return False


def _default_mqtt_policy(inputs: tuple[InputConfig, ...]) -> NotificationPolicy | None:
    for input_config in inputs:
        source = input_config.source
        if isinstance(source, OrdApiSource):
            for policy in source.notifications:
                if policy.kind == "mqtt":
                    return policy
    return None


def _subscription_topics(inputs: tuple[InputConfig, ...], policy: NotificationPolicy) -> tuple[str, ...]:
    topics: set[str] = set()
    if policy.topic:
        topics.add(policy.topic)
        return tuple(sorted(topics))

    for input_config in inputs:
        source = input_config.source
        query = input_config.remote_query
        if isinstance(source, OrdApiSource) and isinstance(query, OrdLocationQuery):
            topics.add(f"ORD/+/{{query.location_id}}/{input_config.quantity}".format(query=query))
    return tuple(sorted(topics))


def _import_paho_mqtt():
    try:
        import paho.mqtt.client as mqtt  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("paho-mqtt is required for MQTT support; install requirements.txt") from exc
    return mqtt
