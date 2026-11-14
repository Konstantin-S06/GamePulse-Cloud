"""Kafka producer helpers."""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Producer

from gamepulse.common.logging_setup import get_logger

logger = get_logger(__name__)


def build_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 200000,
            "delivery.timeout.ms": 60000,
        }
    )


@dataclass
class ProduceStats:
    delivered: int = 0
    failed: int = 0
    sent: int = 0


@dataclass
class _DeliveryState:
    stats: ProduceStats = field(default_factory=ProduceStats)


def _delivery_callback(err: Any, _msg: Any, state: _DeliveryState) -> None:
    if err is not None:
        state.stats.failed += 1
    else:
        state.stats.delivered += 1


def produce_stream(
    producer: Producer,
    topic: str,
    messages: Iterable[tuple[str, str]],
    *,
    rate: float = 0.0,
) -> ProduceStats:
    state = _DeliveryState()
    start = time.monotonic()
    count = 0

    for key, value in messages:
        key_b = key.encode("utf-8")
        value_b = value.encode("utf-8")
        attempts = 0
        while True:
            try:
                producer.produce(
                    topic,
                    key=key_b,
                    value=value_b,
                    callback=lambda err, msg, s=state: _delivery_callback(err, msg, s),
                )
                state.stats.sent += 1
                count += 1
                break
            except BufferError:
                attempts += 1
                if attempts > 20:
                    state.stats.failed += 1
                    logger.error("buffer full, dropping message after 20 retries")
                    break
                producer.poll(0.5)

        if count % 1000 == 0:
            producer.poll(0)
            if rate > 0.0:
                elapsed = time.monotonic() - start
                target = state.stats.sent / rate
                if elapsed < target:
                    time.sleep(target - elapsed)

    remaining = producer.flush(60)
    if remaining > 0:
        state.stats.failed += remaining
        logger.error("%s messages still in queue after flush", remaining)

    return state.stats
