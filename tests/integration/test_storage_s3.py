"""S3 storage integration (requires real credentials and DATA_ROOT=s3://...)."""

from __future__ import annotations

import os
import uuid

import pytest

from gamepulse.common import storage
from gamepulse.config import get_settings

pytestmark = pytest.mark.integration


def test_s3_json_roundtrip():
    settings = get_settings()
    if not storage.is_s3(settings.data_root):
        pytest.skip("DATA_ROOT is not s3://")
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        pytest.skip("AWS credentials not set")

    token = uuid.uuid4().hex[:8]
    uri = storage.join_uri(settings.data_root, f"smoke/pytest-{token}.json")
    storage.write_json({"token": token}, uri)
    try:
        assert storage.read_json(uri) == {"token": token}
        assert storage.exists(uri)
    finally:
        storage.delete_prefix(uri)
