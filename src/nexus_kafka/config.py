"""Load the TOML configuration of the connector."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TemporalConfig:
    address: str = "localhost:7233"
    namespace: str = "default"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "nexus-connector"
    auto_offset_reset: str = "earliest"


@dataclass(frozen=True)
class CallbackConfig:
    task_queue: str = "nexus-connector-callbacks"
    """Task queue of the callback worker inside the connector process."""


@dataclass(frozen=True)
class RouteConfig:
    """Map one input topic to one Nexus operation and an optional output topic."""

    name: str
    input_topic: str
    endpoint: str
    service: str
    operation: str
    schedule_to_close_timeout: timedelta
    output_topic: str | None = None
    """Static output topic. If ``output_topic_header`` is also set, this is the
    fallback topic for records that do not have the header."""
    output_topic_header: str | None = None
    """Name of a Kafka header that holds the output topic of each record."""
    propagate_headers: tuple[str, ...] = ()
    """Input record headers to copy to the output record."""

    def resolve_output_topic(self, headers: Mapping[str, str]) -> str | None:
        """Return the output topic of a record, or None if the route has no output."""
        if self.output_topic_header:
            topic = headers.get(self.output_topic_header)
            if topic:
                return topic
        return self.output_topic


@dataclass(frozen=True)
class ConnectorConfig:
    temporal: TemporalConfig = field(default_factory=TemporalConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    callbacks: CallbackConfig = field(default_factory=CallbackConfig)
    routes: tuple[RouteConfig, ...] = ()

    def routes_for_topic(self, topic: str) -> list[RouteConfig]:
        return [r for r in self.routes if r.input_topic == topic]

    @property
    def input_topics(self) -> list[str]:
        return sorted({r.input_topic for r in self.routes})


_ROUTE_KEYS = {
    "name",
    "input_topic",
    "endpoint",
    "service",
    "operation",
    "schedule_to_close_timeout_seconds",
    "output_topic",
    "output_topic_header",
    "propagate_headers",
}


def _section(raw: dict[str, Any], name: str, cls: type) -> Any:
    values = raw.get(name, {})
    try:
        return cls(**values)
    except TypeError as e:
        raise ValueError(f"Invalid [{name}] section: {e}") from e


def _route(raw: dict[str, Any], index: int) -> RouteConfig:
    unknown = set(raw) - _ROUTE_KEYS
    if unknown:
        raise ValueError(f"Route {index}: unknown keys {sorted(unknown)}")
    missing = {"name", "input_topic", "endpoint", "service", "operation"} - set(raw)
    if missing:
        raise ValueError(f"Route {index}: missing keys {sorted(missing)}")
    return RouteConfig(
        name=raw["name"],
        input_topic=raw["input_topic"],
        endpoint=raw["endpoint"],
        service=raw["service"],
        operation=raw["operation"],
        schedule_to_close_timeout=timedelta(
            seconds=raw.get("schedule_to_close_timeout_seconds", 300)
        ),
        output_topic=raw.get("output_topic"),
        output_topic_header=raw.get("output_topic_header"),
        propagate_headers=tuple(raw.get("propagate_headers", ())),
    )


def load_config(path: str | Path) -> ConnectorConfig:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    routes = tuple(_route(r, i) for i, r in enumerate(raw.get("routes", [])))
    if not routes:
        raise ValueError("Configuration must have at least one [[routes]] entry")
    names = [r.name for r in routes]
    if len(names) != len(set(names)):
        raise ValueError(f"Route names must be unique, got {names}")
    return ConnectorConfig(
        temporal=_section(raw, "temporal", TemporalConfig),
        kafka=_section(raw, "kafka", KafkaConfig),
        callbacks=_section(raw, "callbacks", CallbackConfig),
        routes=routes,
    )
