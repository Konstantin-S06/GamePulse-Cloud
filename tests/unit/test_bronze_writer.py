from datetime import datetime, timezone

from gamepulse.common import storage
from gamepulse.ingestion.bronze_writer import BRONZE_SCHEMA, write_bronze_batch


def _row(batch_id: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "raw_json": '{"event_id":"x"}',
        "kafka_topic": "gamepulse.events.v1",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "kafka_timestamp": now,
        "ingested_at": now,
        "consumer_id": "c0",
        "batch_id": batch_id,
    }


def test_write_bronze_batch_schema_and_path(tmp_path):
    root = str(tmp_path)
    batch_id = "abc123456789"
    uri = write_bronze_batch(root, "2026-01-05", "c0", [_row(batch_id)], 0, batch_id)
    assert uri.endswith(f"part-c0-00000-{batch_id}.parquet")
    assert storage.exists(uri)
    table = storage.read_parquet(uri)
    assert table.schema.equals(BRONZE_SCHEMA)
    assert table.num_rows == 1
    assert table.column("batch_id")[0].as_py() == batch_id
