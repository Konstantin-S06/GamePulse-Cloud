"""Write and read a small object under smoke/ to verify S3 credentials."""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone

from gamepulse.common import storage
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings

logger = get_logger(__name__)


def main() -> int:
    settings = get_settings()
    if not storage.is_s3(settings.data_root):
        logger.error("DATA_ROOT is not an s3:// URL; set DATA_ROOT to your lake bucket")
        return 1
    if not __import__("os").environ.get("AWS_ACCESS_KEY_ID"):
        logger.error("AWS_ACCESS_KEY_ID is not set")
        return 1
    if not __import__("os").environ.get("AWS_SECRET_ACCESS_KEY"):
        logger.error("AWS_SECRET_ACCESS_KEY is not set")
        return 1

    token = uuid.uuid4().hex[:8]
    uri = storage.join_uri(settings.data_root, f"smoke/test-{token}.json")
    payload = {
        "ok": True,
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    storage.write_json(payload, uri)
    back = storage.read_json(uri)
    if back.get("ok") is not True:
        logger.error("round-trip failed for %s", uri)
        return 1
    storage.delete_prefix(uri)
    logger.info("s3 smoke ok: %s", uri)
    return 0


if __name__ == "__main__":
    sys.exit(main())
