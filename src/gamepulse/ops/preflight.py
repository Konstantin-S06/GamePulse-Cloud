"""Environment checks before pipeline stages."""

from __future__ import annotations

import os
import sys

from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings

logger = get_logger(__name__)


def main() -> int:
    settings = get_settings()
    missing: list[str] = []
    if not settings.data_root:
        missing.append("DATA_ROOT")
    if not os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip():
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    if not os.environ.get("KAFKA_TOPIC", "").strip():
        missing.append("KAFKA_TOPIC")
    if not os.environ.get("SPARK_MASTER", "").strip():
        missing.append("SPARK_MASTER")
    if settings.spark_master.startswith("spark://") and not settings.spark_driver_host:
        missing.append("SPARK_DRIVER_HOST")

    if settings.data_root.startswith("s3://"):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            missing.append("AWS_ACCESS_KEY_ID")
        if not os.environ.get("AWS_SECRET_ACCESS_KEY"):
            missing.append("AWS_SECRET_ACCESS_KEY")
        if not settings.aws_region:
            missing.append("AWS_REGION")

    if missing:
        logger.error("missing required configuration: %s", ", ".join(missing))
        return 1

    logger.info(
        "preflight ok (data_root=%s, kafka=%s, spark_master=%s)",
        settings.data_root,
        settings.kafka_bootstrap_servers,
        settings.spark_master,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
