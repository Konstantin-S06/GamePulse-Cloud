"""Spark batch job: Bronze to Silver and quarantine."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from gamepulse.common import metrics as metrics_mod
from gamepulse.common import storage
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.processing import rules
from gamepulse.processing.schemas import ENVELOPE_SCHEMA, PAYLOAD_FIELD_NAMES, SILVER_COLUMNS
from gamepulse.processing.session import build_spark

logger = get_logger(__name__)

BRONZE_SCHEMA = StructType(
    [
        StructField("raw_json", StringType(), False),
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("consumer_id", StringType(), False),
        StructField("batch_id", StringType(), False),
    ]
)

ENVELOPE_FIELDS = [
    "event_id",
    "event_type",
    "event_version",
    "event_ts",
    "match_id",
    "player_id",
]


def _validate_bronze_schema(df: DataFrame) -> None:
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected = {f.name: f.dataType.simpleString() for f in BRONZE_SCHEMA.fields}
    if actual != expected:
        raise RuntimeError("bronze schema changed")


def _parse_envelope(df: DataFrame) -> DataFrame:
    out = df.withColumn("_parsed", F.from_json(F.col("raw_json"), ENVELOPE_SCHEMA))
    for name in ENVELOPE_FIELDS:
        out = out.withColumn(name, F.col(f"_parsed.{name}"))
    for name in PAYLOAD_FIELD_NAMES:
        out = out.withColumn(name, F.col(f"_parsed.payload.{name}"))
    return out.withColumn("ts", F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))


def _typed_silver(df: DataFrame) -> DataFrame:
    now = F.current_timestamp()
    return (
        df.withColumn("event_date", F.to_date(F.col("ts")))
        .withColumn("event_version", F.col("event_version").cast("int"))
        .withColumn("event_ts", F.col("ts"))
        .withColumn("skill_rating", F.col("skill_rating").cast("int"))
        .withColumn("cost", F.col("cost").cast("int"))
        .withColumn("amount", F.col("amount").cast("int"))
        .withColumn("duration_seconds", F.col("duration_seconds").cast("int"))
        .withColumn("blue_score", F.col("blue_score").cast("int"))
        .withColumn("red_score", F.col("red_score").cast("int"))
        .withColumn("kills", F.col("kills").cast("int"))
        .withColumn("deaths", F.col("deaths").cast("int"))
        .withColumn("assists", F.col("assists").cast("int"))
        .withColumn("damage_dealt", F.col("damage_dealt").cast("long"))
        .withColumn("damage_received", F.col("damage_received").cast("long"))
        .withColumn("gold_spent", F.col("gold_spent").cast("int"))
        .withColumn("won", F.col("won") == F.lit("true"))
        .withColumn("silver_processed_at", now)
        .withColumn("kafka_partition", F.col("kafka_partition").cast("int"))
    )


def run_silver(spark, run_date: str, run_id: str) -> dict[str, int]:
    settings = get_settings()
    root = settings.data_root
    bronze_prefix = storage.join_uri(root, f"bronze/events/run_date={run_date}")
    if not storage.list_files(bronze_prefix):
        raise RuntimeError("bronze is empty")

    checkpoint_dir = storage.join_uri(root, f"tmp/spark-checkpoint/silver/{run_date}")
    storage.delete_prefix(checkpoint_dir)
    spark.sparkContext.setCheckpointDir(storage.to_spark_uri(checkpoint_dir))

    bronze_path = storage.to_spark_uri(bronze_prefix)
    df = spark.read.parquet(bronze_path)
    _validate_bronze_schema(df)
    records_in = df.count()

    df = _parse_envelope(df)
    df = rules.apply_row_rules(df, run_date)
    # Materialize after the large rule expression tree so later joins stay tractable.
    df = df.localCheckpoint(eager=True)

    ok = df.filter(F.col("rejection_reason").isNull())
    bad = df.filter(F.col("rejection_reason").isNotNull())

    window = Window.partitionBy("event_id").orderBy(
        "ingested_at", "kafka_partition", "kafka_offset"
    )
    ok = ok.withColumn("rn", F.row_number().over(window))
    ok = rules.add_reason(ok, "duplicate_event_id", F.col("rn") == 1)
    ok = ok.drop("rn")
    df = bad.unionByName(ok, allowMissingColumns=True)

    starts = (
        df.filter(F.col("rejection_reason").isNull() & (F.col("event_type") == "match_start"))
        .select("match_id")
        .distinct()
        .withColumn("has_start", F.lit(True))
    )
    df = df.join(starts, on="match_id", how="left")
    df = rules.add_reason(df, "orphan_match", F.col("has_start").isNotNull())
    df = df.drop("has_start")

    members = (
        df.filter(F.col("rejection_reason").isNull() & (F.col("event_type") == "player_join"))
        .select("match_id", "player_id")
        .distinct()
        .withColumn("is_member", F.lit(True))
    )
    df = rules.check_membership(df, members, "player_id")
    df = rules.check_membership(df, members, "target_player_id")
    df = rules.check_membership(df, members, "victim_player_id")

    df.persist(StorageLevel.MEMORY_AND_DISK)
    reason_rows = df.groupBy("rejection_reason").count().collect()
    reason_counts = {
        (row["rejection_reason"] if row["rejection_reason"] is not None else "accepted"): row[
            "count"
        ]
        for row in reason_rows
    }

    silver_prefix = storage.join_uri(root, f"silver/events/event_date={run_date}")
    quarantine_prefix = storage.join_uri(root, f"quarantine/events/run_date={run_date}")
    storage.delete_prefix(silver_prefix)
    storage.delete_prefix(quarantine_prefix)

    good = df.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "_parsed")
    good = _typed_silver(good).drop("ts").select(*SILVER_COLUMNS)
    silver_out = storage.to_spark_uri(storage.join_uri(root, "silver/events"))
    good.repartition(settings.spark_shuffle_partitions).write.mode("overwrite").partitionBy(
        "event_date", "event_type"
    ).parquet(silver_out)

    quarantine = df.filter(F.col("rejection_reason").isNotNull()).select(
        "raw_json",
        "rejection_reason",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        F.current_timestamp().alias("quarantined_at"),
        F.lit(run_date).alias("run_date"),
    )
    quarantine_out = storage.to_spark_uri(storage.join_uri(root, "quarantine/events"))
    quarantine.coalesce(1).write.mode("overwrite").partitionBy("run_date").parquet(quarantine_out)

    records_out = good.count()
    records_rejected = quarantine.count()
    df.unpersist()

    return {
        "records_in": records_in,
        "records_out": records_out,
        "records_rejected": records_rejected,
        "reason_counts": reason_counts,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Silver validation job")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    settings = get_settings()
    run_date = args.run_date
    run_id = args.run_id
    sw = metrics_mod.Stopwatch()
    sw.start()
    spark = None
    records_in = 0
    try:
        spark = build_spark("gamepulse-silver")
        stats = run_silver(spark, run_date, run_id)
        records_in = stats["records_in"]
        sw.stop()
        metrics_mod.write_stage_metrics(
            settings.data_root,
            "silver",
            run_id,
            run_date,
            sw.started_at,
            sw.finished_at,
            "success",
            stats["records_in"],
            stats["records_out"],
            stats["records_rejected"],
            {
                "spark_master": settings.spark_master,
                "spark_cores_max": settings.spark_cores_max,
                "shuffle_partitions": settings.spark_shuffle_partitions,
            },
            {
                "reason_counts": stats["reason_counts"],
                "spark_app_id": spark.sparkContext.applicationId,
            },
        )
        logger.info("silver finished for %s", run_date)
        return 0
    except Exception as exc:
        sw.stop()
        finished = sw.finished_at or datetime.now(timezone.utc)
        started = sw.started_at or finished
        metrics_mod.write_stage_metrics(
            settings.data_root,
            "silver",
            run_id,
            run_date,
            started,
            finished,
            "failed",
            records_in,
            0,
            0,
            {
                "spark_master": settings.spark_master,
                "spark_cores_max": settings.spark_cores_max,
                "shuffle_partitions": settings.spark_shuffle_partitions,
            },
            {"error": str(exc)},
        )
        logger.error("silver failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
