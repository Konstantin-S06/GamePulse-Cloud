"""Kafka ingest consumer loop."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from gamepulse.common.logging_setup import get_logger

logger = get_logger(__name__)

TIMESTAMP_CREATE_TIME = 1


@dataclass
class ConsumerStats:
    consumer_id: str
    messages_consumed: int = 0
    rows_written: int = 0
    dlq_count: int = 0
    batches: int = 0
    latency_ms_total: float = 0.0
    latency_ms_max: float = 0.0

    @property
    def latency_ms_avg(self) -> float:
        if self.messages_consumed == 0:
            return 0.0
        return self.latency_ms_total / self.messages_consumed


BronzeWriter = Callable[[list[dict[str, Any]], int, str], None]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _msg_timestamp_ms(msg: Any) -> datetime:
    ts_type, ts_ms = msg.timestamp()
    if ts_type == TIMESTAMP_CREATE_TIME or (ts_ms and ts_ms > 0):
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return _now_utc()


class ConsumerLoop:
    def __init__(
        self,
        consumer: Consumer,
        dlq_producer: Any,
        writer: BronzeWriter,
        *,
        consumer_id: str,
        dlq_topic: str,
        events_topic: str,
        batch_size: int = 10000,
        flush_seconds: float = 5,
        idle_timeout_seconds: float = 15,
    ) -> None:
        self.consumer = consumer
        self.dlq_producer = dlq_producer
        self.writer = writer
        self.consumer_id = consumer_id
        self.dlq_topic = dlq_topic
        self.events_topic = events_topic
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.idle_timeout_seconds = idle_timeout_seconds
        self.buffer: list[dict[str, Any]] = []
        self.batch_seq = 0
        self.stats = ConsumerStats(consumer_id=consumer_id)
        self._last_message_at: float | None = None
        self._last_flush_check = time.monotonic()
        self._current_batch_id: str | None = None

    def _ensure_batch_id(self) -> str:
        if self._current_batch_id is None:
            self._current_batch_id = uuid.uuid4().hex[:12]
        return self._current_batch_id

    def _send_dlq(self, msg: Any, reason: str) -> None:
        headers = [
            ("error_reason", reason.encode("utf-8")),
            ("original_topic", msg.topic().encode("utf-8")),
            ("original_partition", str(msg.partition()).encode("utf-8")),
            ("original_offset", str(msg.offset()).encode("utf-8")),
        ]
        self.dlq_producer.produce(self.dlq_topic, value=msg.value(), headers=headers)
        self.stats.dlq_count += 1

    def _add_row(self, msg: Any, raw_json: str) -> None:
        ingested = _now_utc()
        kafka_ts = _msg_timestamp_ms(msg)
        latency_ms = (ingested - kafka_ts).total_seconds() * 1000.0
        self.stats.latency_ms_total += latency_ms
        if latency_ms > self.stats.latency_ms_max:
            self.stats.latency_ms_max = latency_ms
        self.buffer.append(
            {
                "raw_json": raw_json,
                "kafka_topic": msg.topic(),
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
                "kafka_timestamp": kafka_ts,
                "ingested_at": ingested,
                "consumer_id": self.consumer_id,
                "batch_id": self._ensure_batch_id(),
            }
        )

    def flush(self) -> None:
        if not self.buffer:
            return
        batch_id = self._ensure_batch_id()
        rows = list(self.buffer)
        self.buffer.clear()
        delays = [1, 2, 4]
        last_exc: Exception | None = None
        for delay in delays:
            try:
                self.writer(rows, self.batch_seq, batch_id)
                self.stats.rows_written += len(rows)
                self.stats.batches += 1
                self.batch_seq += 1
                self._current_batch_id = None
                self.dlq_producer.flush()
                self._commit_offsets()
                return
            except Exception as exc:
                last_exc = exc
                logger.warning("bronze write failed, retry in %ss: %s", delay, exc)
                time.sleep(delay)
        assert last_exc is not None
        raise last_exc

    def on_revoke(self, _consumer: Consumer, _partitions: list[TopicPartition]) -> None:
        logger.info("partition revoke for %s, flushing", self.consumer_id)
        self.flush()

    def _commit_offsets(self) -> None:
        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            err = exc.args[0] if exc.args else None
            code = err.code() if err is not None and hasattr(err, "code") else None
            if code == KafkaError._NO_OFFSET:
                return
            raise

    def run(self) -> ConsumerStats:
        self._last_flush_check = time.monotonic()
        # Start the idle clock immediately so consumers with no traffic can exit.
        self._last_message_at = time.monotonic()
        while True:
            msg = self.consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                if (now - self._last_message_at) >= self.idle_timeout_seconds:
                    break
                if self.buffer and (now - self._last_flush_check) >= self.flush_seconds:
                    self.flush()
                    self._last_flush_check = now
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("kafka error: %s", msg.error())
                raise KafkaException(msg.error())

            self._last_message_at = now
            self.stats.messages_consumed += 1

            try:
                text = msg.value().decode("utf-8")
                parsed = json.loads(text)
                if not isinstance(parsed, dict):
                    raise ValueError("not a dict")
                self._add_row(msg, text)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
                self._send_dlq(msg, type(exc).__name__)
                continue

            if len(self.buffer) >= self.batch_size:
                self.flush()
                self._last_flush_check = time.monotonic()

            elif self.buffer and (now - self._last_flush_check) >= self.flush_seconds:
                self.flush()
                self._last_flush_check = now

        self.flush()
        self._commit_offsets()
        self.consumer.close()
        return self.stats


def consumer_config(bootstrap: str, group_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
    }
