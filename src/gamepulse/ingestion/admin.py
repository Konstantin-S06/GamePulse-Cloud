"""Kafka topic administration."""

from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.ingestion.topics import dlq_topic, events_topic

logger = get_logger(__name__)


def create_topic(bootstrap: str, name: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    future = futures[name]
    try:
        future.result()
        logger.info("created topic %s with %s partitions", name, partitions)
    except Exception as exc:
        msg = str(exc)
        if "TOPIC_ALREADY_EXISTS" in msg or "already exists" in msg.lower():
            logger.warning("topic %s already exists", name)
            metadata = admin.list_topics(timeout=10)
            if name in metadata.topics:
                existing = len(metadata.topics[name].partitions)
                if existing != partitions:
                    logger.warning(
                        "topic %s has %s partitions, requested %s",
                        name,
                        existing,
                        partitions,
                    )
            return
        raise


def delete_topic(bootstrap: str, name: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.delete_topics([name])
    future = futures[name]
    try:
        future.result()
        logger.info("deleted topic %s", name)
    except Exception as exc:
        msg = str(exc)
        if "UNKNOWN_TOPIC_OR_PART" in msg or "Unknown topic" in msg:
            logger.warning("topic %s does not exist", name)
            return
        raise


def list_topics(bootstrap: str) -> list[str]:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=10)
    return sorted(metadata.topics.keys())


def _cmd_create(args: argparse.Namespace) -> int:
    settings = get_settings()
    bootstrap = args.bootstrap or settings.kafka_bootstrap_servers
    if args.topic:
        create_topic(bootstrap, args.topic, args.partitions)
        return 0
    create_topic(bootstrap, events_topic(settings), settings.kafka_partitions)
    create_topic(bootstrap, dlq_topic(settings), 1)
    return 0


def _cmd_delete(args: argparse.Namespace) -> int:
    settings = get_settings()
    bootstrap = args.bootstrap or settings.kafka_bootstrap_servers
    if not args.topic:
        logger.error("--topic is required for delete")
        return 1
    delete_topic(bootstrap, args.topic)
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    settings = get_settings()
    bootstrap = args.bootstrap or settings.kafka_bootstrap_servers
    for name in list_topics(bootstrap):
        print(name)
    return 0


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Kafka topic admin")
    sub = parser.add_subparsers(dest="command", required=True)
    p_create = sub.add_parser("create")
    p_create.add_argument("--bootstrap", default=None)
    p_create.add_argument("--topic", default=None)
    p_create.add_argument("--partitions", type=int, default=settings.kafka_partitions)
    p_create.set_defaults(func=_cmd_create)
    p_delete = sub.add_parser("delete")
    p_delete.add_argument("--bootstrap", default=None)
    p_delete.add_argument("--topic", required=True)
    p_delete.set_defaults(func=_cmd_delete)
    p_list = sub.add_parser("list")
    p_list.add_argument("--bootstrap", default=None)
    p_list.set_defaults(func=_cmd_list)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
