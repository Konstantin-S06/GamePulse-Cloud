"""Run parallel Kafka ingest consumers."""

from __future__ import annotations

import argparse
import multiprocessing as mp
import sys
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer
from confluent_kafka import Consumer as KafkaConsumer

from gamepulse.common import metrics
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.ingestion.bronze_writer import write_bronze_batch
from gamepulse.ingestion.consumer import ConsumerLoop, consumer_config
from gamepulse.ingestion.producer import build_producer
from gamepulse.ingestion.topics import dlq_topic

logger = get_logger(__name__)


def _run_consumer_worker(
    worker_args: dict[str, Any],
    out_queue: mp.Queue,
) -> None:
    consumer_id = worker_args["consumer_id"]
    try:
        consumer = KafkaConsumer(consumer_config(worker_args["bootstrap"], worker_args["group_id"]))
        dlq_producer = build_producer(worker_args["bootstrap"])

        def writer(rows: list[dict[str, Any]], batch_seq: int, batch_id: str) -> None:
            write_bronze_batch(
                worker_args["data_root"],
                worker_args["run_date"],
                consumer_id,
                rows,
                batch_seq,
                batch_id,
            )

        loop = ConsumerLoop(
            consumer,
            dlq_producer,
            writer,
            consumer_id=consumer_id,
            dlq_topic=worker_args["dlq_topic"],
            events_topic=worker_args["topic"],
            batch_size=worker_args["batch_size"],
            flush_seconds=worker_args["flush_seconds"],
            idle_timeout_seconds=worker_args["idle_timeout_seconds"],
        )

        def _on_revoke(c: Consumer, parts: list[Any]) -> None:
            loop.on_revoke(c, parts)

        consumer.subscribe([worker_args["topic"]], on_revoke=_on_revoke)
        stats = loop.run()
        out_queue.put(
            {
                "consumer_id": stats.consumer_id,
                "messages_consumed": stats.messages_consumed,
                "rows_written": stats.rows_written,
                "dlq_count": stats.dlq_count,
                "batches": stats.batches,
                "latency_ms_avg": stats.latency_ms_avg,
                "latency_ms_max": stats.latency_ms_max,
            }
        )
    except Exception as exc:
        logger.exception("consumer %s failed", consumer_id)
        out_queue.put({"error": str(exc), "consumer_id": consumer_id})
        raise


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Ingest Kafka events to Bronze")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--num-consumers", type=int, default=3)
    parser.add_argument("--group-id", default="gamepulse-ingest")
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--flush-seconds", type=int, default=5)
    parser.add_argument("--idle-timeout-seconds", type=int, default=15)
    args = parser.parse_args(argv)

    stopwatch = metrics.Stopwatch()
    stopwatch.start()
    consumer_stats: list[dict[str, Any]] = []
    status = "success"
    records_in = 0
    records_out = 0
    records_rejected = 0
    exit_code = 0

    ctx = mp.get_context("spawn")
    queue: mp.Queue = ctx.Queue()
    processes: list[mp.Process] = []
    base_args = {
        "run_date": args.run_date,
        "data_root": settings.data_root,
        "bootstrap": args.bootstrap,
        "group_id": args.group_id,
        "topic": args.topic,
        "dlq_topic": dlq_topic(settings),
        "batch_size": args.batch_size,
        "flush_seconds": args.flush_seconds,
        "idle_timeout_seconds": args.idle_timeout_seconds,
    }

    try:
        for i in range(args.num_consumers):
            worker_args = {**base_args, "consumer_id": f"c{i}"}
            proc = ctx.Process(target=_run_consumer_worker, args=(worker_args, queue))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()
            if proc.exitcode != 0:
                exit_code = 1
                status = "failed"

        while not queue.empty():
            item = queue.get()
            if "error" in item:
                status = "failed"
                exit_code = 1
            else:
                consumer_stats.append(item)
                records_in += item["messages_consumed"]
                records_out += item["rows_written"]
                records_rejected += item["dlq_count"]
    except Exception:
        status = "failed"
        exit_code = 1
        logger.exception("ingest failed")
    finally:
        stopwatch.stop()
        metrics.write_stage_metrics(
            settings.data_root,
            "ingest",
            args.run_id,
            args.run_date,
            stopwatch.started_at or datetime.now(timezone.utc),
            stopwatch.finished_at or datetime.now(timezone.utc),
            status,
            records_in,
            records_out,
            records_rejected,
            {
                "group_id": args.group_id,
                "num_consumers": args.num_consumers,
                "topic": args.topic,
            },
            {"consumers": consumer_stats},
        )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
