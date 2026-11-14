"""Bronze Parquet writer for ingested Kafka messages."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from gamepulse.common import storage

BRONZE_SCHEMA = pa.schema(
    [
        ("raw_json", pa.string()),
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("kafka_timestamp", pa.timestamp("ms", tz="UTC")),
        ("ingested_at", pa.timestamp("ms", tz="UTC")),
        ("consumer_id", pa.string()),
        ("batch_id", pa.string()),
    ]
)


def _ts_ms(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def bronze_uri(
    data_root: str, run_date: str, consumer_id: str, batch_seq: int, batch_id: str
) -> str:
    name = f"part-{consumer_id}-{batch_seq:05d}-{batch_id}.parquet"
    return storage.join_uri(data_root, f"bronze/events/run_date={run_date}", name)


def write_bronze_batch(
    data_root: str,
    run_date: str,
    consumer_id: str,
    rows: list[dict[str, Any]],
    batch_seq: int,
    batch_id: str,
) -> str:
    if not rows:
        raise ValueError("empty batch")
    columns: dict[str, list[Any]] = {f.name: [] for f in BRONZE_SCHEMA}
    for row in rows:
        columns["raw_json"].append(row["raw_json"])
        columns["kafka_topic"].append(row["kafka_topic"])
        columns["kafka_partition"].append(int(row["kafka_partition"]))
        columns["kafka_offset"].append(int(row["kafka_offset"]))
        columns["kafka_timestamp"].append(_ts_ms(row["kafka_timestamp"]))
        columns["ingested_at"].append(_ts_ms(row["ingested_at"]))
        columns["consumer_id"].append(row["consumer_id"])
        columns["batch_id"].append(row["batch_id"])
    table = pa.table(columns, schema=BRONZE_SCHEMA)
    uri = bronze_uri(data_root, run_date, consumer_id, batch_seq, batch_id)
    storage.write_parquet(table, uri)
    return uri
