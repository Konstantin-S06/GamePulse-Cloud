"""Produce simulated events to Kafka."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from gamepulse.common import metrics
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.ingestion.producer import build_producer, produce_stream
from gamepulse.simulator.corruption import corrupt_stream
from gamepulse.simulator.generator import generate_events
from gamepulse.simulator.sim_config import SimConfig

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Produce events to Kafka")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--num-matches", type=int, default=100)
    parser.add_argument("--num-players", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--rate", type=float, default=0.0)
    parser.add_argument("--corrupt-rate", type=float, default=0.0)
    parser.add_argument("--corrupt-all-types", action="store_true")
    default_run_id = "local-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser.add_argument("--run-id", default=default_run_id)
    args = parser.parse_args(argv)

    stopwatch = metrics.Stopwatch()
    stopwatch.start()
    status = "success"
    stats_delivered = 0
    stats_failed = 0
    corruption_counts: dict[str, int] = {}

    try:
        config = SimConfig(
            run_date=args.run_date,
            seed=args.seed,
            num_matches=args.num_matches,
            num_players=args.num_players,
        )
        events = generate_events(config)
        stream = corrupt_stream(
            events,
            rate=args.corrupt_rate,
            seed=args.seed,
            all_types=args.corrupt_all_types,
        )
        producer = build_producer(args.bootstrap)
        result = produce_stream(producer, args.topic, stream, rate=args.rate)
        stats_delivered = result.delivered
        stats_failed = result.failed
        report = corrupt_stream.report  # type: ignore[attr-defined]
        corruption_counts = dict(report.counts)
        if stats_failed > 0:
            status = "failed"
    except Exception:
        status = "failed"
        logger.exception("produce failed")
        stopwatch.stop()
        root = settings.data_root
        metrics.write_stage_metrics(
            root,
            "produce",
            args.run_id,
            args.run_date,
            stopwatch.started_at or datetime.now(timezone.utc),
            stopwatch.finished_at or datetime.now(timezone.utc),
            "failed",
            stats_delivered + stats_failed,
            stats_delivered,
            stats_failed,
            {"topic": args.topic, "bootstrap": args.bootstrap, "rate": args.rate},
            {"corruption_counts": corruption_counts},
        )
        return 1

    stopwatch.stop()
    metrics.write_stage_metrics(
        settings.data_root,
        "produce",
        args.run_id,
        args.run_date,
        stopwatch.started_at or datetime.now(timezone.utc),
        stopwatch.finished_at or datetime.now(timezone.utc),
        status,
        stats_delivered + stats_failed,
        stats_delivered,
        stats_failed,
        {"topic": args.topic, "bootstrap": args.bootstrap, "rate": args.rate},
        {"corruption_counts": corruption_counts},
    )
    return 1 if status == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
