"""Kafka topic name helpers."""

from __future__ import annotations

from gamepulse.config import Settings, get_settings


def events_topic(settings: Settings | None = None) -> str:
    s = settings or get_settings()
    return s.kafka_topic


def dlq_topic(settings: Settings | None = None) -> str:
    return events_topic(settings) + ".dlq"
