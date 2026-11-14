"""Archive Kafka DLQ messages to ingestion quarantine."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from gamepulse.common import metrics
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.ingestion.consumer import consumer_config
from gamepulse.ingestion.ingestion_quarantine import write_ingestion_quarantine
from gamepulse.ingestion.topics import dlq_topic

logger = get_logger(__name__)

GROUP_ID = "gamepulse-dlq-archiver"
BATCH_SIZE = 500
IDLE_TIMEOUT = 15


def _header_value(headers: list[tuple[str, bytes]] | None, key: str) -> str:
    if not headers:
        return ""
    for hkey, val in headers:
        if hkey == key:
            return val.decode("utf-8", errors="replace")
    return ""


def archive_dlq(
    *,
    bootstrap: str,
    topic: str,
    data_root: str,
    run_date: str,
    idle_timeout_seconds: float = IDLE_TIMEOUT,
) -> dict[str, int]:
    consumer = Consumer(consumer_config(bootstrap, GROUP_ID))
    consumer.subscribe([topic])
    buffer: list[dict[str, Any]] = []
    messages = 0
    files = 0
    last_message_at: float | None = None

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                if last_message_at is not None and (now - last_message_at) >= idle_timeout_seconds:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            last_message_at = now
            messages += 1
            raw = (msg.value() or b"").decode("utf-8", errors="replace")
            buffer.append(
                {
                    "raw_value": raw,
                    "error_reason": _header_value(msg.headers(), "error_reason") or "unknown",
                    "original_topic": _header_value(msg.headers(), "original_topic"),
                    "original_partition": int(
                        _header_value(msg.headers(), "original_partition") or msg.partition()
                    ),
                    "original_offset": int(
                        _header_value(msg.headers(), "original_offset") or msg.offset()
                    ),
                    "quarantined_at": datetime.now(timezone.utc),
                }
            )
            if len(buffer) >= BATCH_SIZE:
                write_ingestion_quarantine(data_root, run_date, buffer)
                buffer.clear()
                files += 1
                consumer.commit(asynchronous=False)

        if buffer:
            write_ingestion_quarantine(data_root, run_date, buffer)
            files += 1
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return {"messages": messages, "files": files}


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Archive DLQ to quarantine")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=dlq_topic(settings))
    args = parser.parse_args(argv)

    stopwatch = metrics.Stopwatch()
    stopwatch.start()
    records_in = 0
    records_out = 0
    status = "success"
    extra: dict[str, Any] = {}

    try:
        result = archive_dlq(
            bootstrap=args.bootstrap,
            topic=args.topic,
            data_root=settings.data_root,
            run_date=args.run_date,
        )
        records_in = result["messages"]
        records_out = result["files"]
        extra = result
    except Exception:
        status = "failed"
        logger.exception("dlq archive failed")
        stopwatch.stop()
        metrics.write_stage_metrics(
            settings.data_root,
            "dlq_archive",
            args.run_id,
            args.run_date,
            stopwatch.started_at or datetime.now(timezone.utc),
            stopwatch.finished_at or datetime.now(timezone.utc),
            "failed",
            records_in,
            records_out,
            0,
            {"topic": args.topic},
            extra,
        )
        return 1

    stopwatch.stop()
    metrics.write_stage_metrics(
        settings.data_root,
        "dlq_archive",
        args.run_id,
        args.run_date,
        stopwatch.started_at or datetime.now(timezone.utc),
        stopwatch.finished_at or datetime.now(timezone.utc),
        status,
        records_in,
        records_out,
        0,
        {"topic": args.topic},
        extra,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
