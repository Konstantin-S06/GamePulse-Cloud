"""Settings loaded from environment variables on every call."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    data_root: str
    aws_region: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_partitions: int
    spark_master: str
    spark_cores_max: int
    spark_shuffle_partitions: int
    spark_executor_memory: str
    spark_driver_memory: str
    spark_driver_host: str
    max_reject_rate: float
    log_level: str


def get_settings() -> Settings:
    """Read os.environ every call; no caching."""
    return Settings(
        data_root=os.environ.get("DATA_ROOT", "/opt/gamepulse/data/lake"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_topic=os.environ.get("KAFKA_TOPIC", "gamepulse.events.v1"),
        kafka_partitions=int(os.environ.get("KAFKA_PARTITIONS", "6")),
        spark_master=os.environ.get("SPARK_MASTER", "local[2]"),
        spark_cores_max=int(os.environ.get("SPARK_CORES_MAX", "4")),
        spark_shuffle_partitions=int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")),
        spark_executor_memory=os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"),
        spark_driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "1g"),
        spark_driver_host=os.environ.get("SPARK_DRIVER_HOST", "localhost"),
        max_reject_rate=float(os.environ.get("MAX_REJECT_RATE", "0.05")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
