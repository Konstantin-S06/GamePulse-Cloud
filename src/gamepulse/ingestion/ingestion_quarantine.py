"""Ingestion DLQ quarantine Parquet writer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pyarrow as pa

from gamepulse.common import storage

INGESTION_QUARANTINE_SCHEMA = pa.schema(
    [
        ("raw_value", pa.string()),
        ("error_reason", pa.string()),
        ("original_topic", pa.string()),
        ("original_partition", pa.int32()),
        ("original_offset", pa.int64()),
        ("quarantined_at", pa.timestamp("ms", tz="UTC")),
    ]
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def quarantine_uri(data_root: str, run_date: str) -> str:
    uid = uuid4().hex[:8]
    name = f"part-{uid}.parquet"
    return storage.join_uri(data_root, f"quarantine/ingestion/run_date={run_date}", name)


def write_ingestion_quarantine(
    data_root: str,
    run_date: str,
    rows: list[dict[str, Any]],
) -> str:
    if not rows:
        raise ValueError("empty quarantine batch")
    now = _now_utc()
    columns: dict[str, list[Any]] = {f.name: [] for f in INGESTION_QUARANTINE_SCHEMA}
    for row in rows:
        columns["raw_value"].append(row["raw_value"])
        columns["error_reason"].append(row["error_reason"])
        columns["original_topic"].append(row["original_topic"])
        columns["original_partition"].append(int(row["original_partition"]))
        columns["original_offset"].append(int(row["original_offset"]))
        columns["quarantined_at"].append(row.get("quarantined_at", now))
    table = pa.table(columns, schema=INGESTION_QUARANTINE_SCHEMA)
    uri = quarantine_uri(data_root, run_date)
    storage.write_parquet(table, uri)
    return uri
