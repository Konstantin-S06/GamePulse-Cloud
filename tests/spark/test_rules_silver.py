import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from gamepulse.processing.rules import apply_row_rules
from gamepulse.processing.schemas import ENVELOPE_SCHEMA, PAYLOAD_FIELD_NAMES
from gamepulse.processing.session import build_local_spark
from gamepulse.processing.silver_job import BRONZE_SCHEMA

pytestmark = pytest.mark.spark

RUN_DATE = "2026-01-05"
_TS = datetime(2026, 1, 5, 12, 0, 0, tzinfo=timezone.utc)


def _bronze_row(raw: dict) -> Row:
    return Row(
        raw_json=json.dumps(raw, separators=(",", ":"), sort_keys=True),
        kafka_topic="gamepulse.events.v1",
        kafka_partition=0,
        kafka_offset=1,
        kafka_timestamp=_TS,
        ingested_at=_TS,
        consumer_id="c0",
        batch_id="b0",
    )


@pytest.fixture(scope="module")
def spark():
    session = build_local_spark()
    yield session
    session.stop()


def test_invalid_json_quarantine(spark):
    # Empty raw_json makes from_json return a null struct (Spark PERMISSIVE).
    row = _bronze_row({}).asDict()
    row["raw_json"] = ""
    df = spark.createDataFrame([Row(**row)], schema=BRONZE_SCHEMA)
    df = df.withColumn("_parsed", F.from_json(F.col("raw_json"), ENVELOPE_SCHEMA))
    for name in ["event_id", "event_type", "event_version", "event_ts", "match_id", "player_id"]:
        df = df.withColumn(name, F.col(f"_parsed.{name}"))
    for name in PAYLOAD_FIELD_NAMES:
        df = df.withColumn(name, F.col(f"_parsed.payload.{name}"))
    df = df.withColumn("ts", F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    out = apply_row_rules(df, RUN_DATE)
    row = out.select("rejection_reason").first()
    assert row["rejection_reason"] == "invalid_json"
