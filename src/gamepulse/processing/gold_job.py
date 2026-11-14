"""Spark batch job: Silver to Gold tables."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gamepulse.common import metrics as metrics_mod
from gamepulse.common import storage
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.processing.session import build_spark
from gamepulse.reference_data import CHARACTERS, ITEMS

logger = get_logger(__name__)


def write_day(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    run_date: str,
    root: str,
    shuffle_partitions: int,
    *,
    coalesce_one: bool = False,
) -> int:
    prefix = storage.join_uri(root, f"gold/{table}/match_date={run_date}")
    storage.delete_prefix(prefix)
    out = storage.to_spark_uri(storage.join_uri(root, f"gold/{table}"))
    writer = df.coalesce(1) if coalesce_one else df.repartition(shuffle_partitions)
    writer.write.mode("overwrite").partitionBy("match_date").parquet(out)
    return df.count()


def _date_key(col: str = "event_date") -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def _write_static_dimensions(spark: SparkSession, root: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    char_path = storage.join_uri(root, "gold/dim_character")
    if not storage.list_files(char_path):
        chars = spark.createDataFrame(
            [
                {"character_id": c["character_id"], "character_name": c["name"], "role": c["role"]}
                for c in CHARACTERS
            ]
        )
        dest = storage.to_spark_uri(char_path)
        chars.coalesce(1).write.mode("overwrite").parquet(dest)
        counts["dim_character"] = chars.count()

    item_path = storage.join_uri(root, "gold/dim_item")
    if not storage.list_files(item_path):
        items = spark.createDataFrame(
            [
                {
                    "item_id": i["item_id"],
                    "item_name": i["name"],
                    "category": i["category"],
                    "cost": i["cost"],
                }
                for i in ITEMS
            ]
        )
        dest = storage.to_spark_uri(item_path)
        items.coalesce(1).write.mode("overwrite").parquet(dest)
        counts["dim_item"] = items.count()

    date_path = storage.join_uri(root, "gold/dim_date")
    if not storage.list_files(date_path):
        seq = spark.sql(
            """
            SELECT explode(sequence(to_date('2025-01-01'), to_date('2030-12-31'), interval 1 day))
              AS date
            """
        )
        dim_date = (
            seq.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
            .withColumn("day_of_week", ((F.dayofweek("date") + 5) % 7) + 1)
            .withColumn("day_name", F.date_format("date", "EEEE"))
            .withColumn("is_weekend", F.col("day_of_week") >= 6)
        )
        dest = storage.to_spark_uri(date_path)
        dim_date.coalesce(1).write.mode("overwrite").parquet(dest)
        counts["dim_date"] = dim_date.count()
    return counts


def run_gold(spark: SparkSession, run_date: str) -> dict[str, int]:
    settings = get_settings()
    root = settings.data_root
    shuffle = settings.spark_shuffle_partitions
    silver_root = storage.join_uri(root, "silver/events")
    silver_uri = storage.to_spark_uri(silver_root)
    day_path = f"{silver_uri}/event_date={run_date}"
    if not storage.list_files(storage.join_uri(silver_root, f"event_date={run_date}")):
        raise RuntimeError("silver is empty for run date")

    silver_day = spark.read.option("basePath", silver_uri).parquet(day_path)
    records_in = silver_day.count()
    table_counts: dict[str, int] = _write_static_dimensions(spark, root)

    join_path = f"{silver_uri}/event_date=*/event_type=player_join"
    player_join_all = spark.read.option("basePath", silver_uri).parquet(join_path)
    dim_player = player_join_all.groupBy("player_id").agg(
        F.min("event_date").alias("first_seen_date"),
        F.max("event_date").alias("last_seen_date"),
        F.count(F.lit(1)).alias("matches_played"),
        F.expr("max_by(skill_rating, event_ts)").alias("latest_skill_rating"),
    )
    storage.delete_prefix(storage.join_uri(root, "gold/dim_player"))
    player_dest = storage.to_spark_uri(storage.join_uri(root, "gold/dim_player"))
    dim_player.coalesce(1).write.mode("overwrite").parquet(player_dest)
    table_counts["dim_player"] = dim_player.count()

    starts = silver_day.filter(F.col("event_type") == "match_start").select(
        "match_id",
        F.col("region"),
        F.col("map_name"),
        F.col("game_mode"),
        F.col("event_ts").alias("start_ts"),
        F.col("event_date").alias("match_date"),
    )
    ends = silver_day.filter(F.col("event_type") == "match_end").select(
        "match_id",
        F.col("event_ts").alias("end_ts"),
        F.col("duration_seconds"),
        F.col("winning_team"),
        F.col("blue_score"),
        F.col("red_score"),
    )
    dim_match = (
        starts.join(ends, "match_id", "left")
        .withColumn("is_complete", F.col("end_ts").isNotNull())
        .select(
            "match_id",
            "match_date",
            "region",
            "map_name",
            "game_mode",
            "start_ts",
            "end_ts",
            "duration_seconds",
            "winning_team",
            "blue_score",
            "red_score",
            "is_complete",
        )
    )
    table_counts["dim_match"] = write_day(
        spark, dim_match, "dim_match", run_date, root, shuffle, coalesce_one=False
    )

    characters = silver_day.filter(F.col("event_type") == "character_select").select(
        "match_id", "player_id", F.col("character_id").alias("actor_character_id")
    )
    events = silver_day.filter(F.col("event_type") != "player_stats")
    fact_match_events = (
        events.join(characters, ["match_id", "player_id"], "left")
        .withColumn("date_key", _date_key())
        .withColumn("match_date", F.col("event_date"))
        .withColumnRenamed("cost", "item_cost")
        .withColumnRenamed("amount", "damage_amount")
        .select(
            "event_id",
            "event_ts",
            "date_key",
            "match_id",
            "player_id",
            "event_type",
            "actor_character_id",
            "item_id",
            "item_cost",
            "target_player_id",
            "victim_player_id",
            "damage_amount",
            "damage_source",
            "match_date",
        )
    )
    table_counts["fact_match_events"] = write_day(
        spark, fact_match_events, "fact_match_events", run_date, root, shuffle
    )

    stats = silver_day.filter(F.col("event_type") == "player_stats").drop("skill_rating")
    joins_day = silver_day.filter(F.col("event_type") == "player_join").select(
        "match_id", "player_id", "skill_rating"
    )
    # dim_match may already carry region/duration; alias join columns to avoid clashes.
    match_attrs = dim_match.select(
        "match_id",
        F.col("region").alias("match_region"),
        F.col("duration_seconds").alias("match_duration_seconds"),
    )
    fact_player_match = (
        stats.join(joins_day, ["match_id", "player_id"], "left")
        .join(match_attrs, "match_id", "left")
        .withColumn("date_key", _date_key())
        .withColumn("match_date", F.col("event_date"))
        .withColumn("region", F.col("match_region"))
        .withColumn("duration_seconds", F.col("match_duration_seconds"))
        .select(
            "match_id",
            "player_id",
            "date_key",
            "character_id",
            "team",
            "won",
            "kills",
            "deaths",
            "assists",
            "damage_dealt",
            "damage_received",
            "gold_spent",
            "skill_rating",
            "region",
            "duration_seconds",
            "match_date",
        )
    )
    table_counts["fact_player_match"] = write_day(
        spark, fact_player_match, "fact_player_match", run_date, root, shuffle
    )

    dm = dim_match
    agg_daily = dm.groupBy("match_date").agg(
        F.count(F.lit(1)).alias("matches"),
        F.sum(F.when(F.col("is_complete"), 1).otherwise(0)).alias("complete_matches"),
    )
    ev = fact_match_events.groupBy("match_date").agg(F.count(F.lit(1)).alias("events"))
    pl = fact_player_match.groupBy("match_date").agg(
        F.countDistinct("player_id").alias("active_players"),
        F.avg("damage_dealt").alias("avg_damage_dealt_per_player"),
    )
    complete_avg = (
        dm.filter(F.col("is_complete"))
        .groupBy("match_date")
        .agg(F.avg("duration_seconds").alias("avg_duration_seconds"))
    )
    elim = (
        fact_match_events.filter(F.col("event_type") == "elimination")
        .groupBy("match_date")
        .agg(F.count(F.lit(1)).alias("total_eliminations"))
    )
    agg_daily_overview = (
        agg_daily.join(ev, "match_date", "left")
        .join(pl, "match_date", "left")
        .join(complete_avg, "match_date", "left")
        .join(elim, "match_date", "left")
    )
    table_counts["agg_daily_overview"] = write_day(
        spark, agg_daily_overview, "agg_daily_overview", run_date, root, shuffle, coalesce_one=True
    )

    agg_character_daily = (
        fact_player_match.groupBy("match_date", "character_id")
        .agg(
            F.count(F.lit(1)).alias("games"),
            F.sum(F.when(F.col("won"), 1).otherwise(0)).alias("wins"),
            F.avg("kills").alias("avg_kills"),
            F.avg("deaths").alias("avg_deaths"),
            F.avg("assists").alias("avg_assists"),
            F.avg("damage_dealt").alias("avg_damage_dealt"),
            F.avg("gold_spent").alias("avg_gold_spent"),
        )
        .withColumn("win_rate", F.col("wins") / F.col("games"))
    )
    table_counts["agg_character_daily"] = write_day(
        spark,
        agg_character_daily,
        "agg_character_daily",
        run_date,
        root,
        shuffle,
        coalesce_one=True,
    )

    records_out = sum(table_counts.values())
    return {
        "records_in": records_in,
        "records_out": records_out,
        "table_counts": table_counts,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gold aggregation job")
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
        spark = build_spark("gamepulse-gold")
        stats = run_gold(spark, run_date)
        records_in = stats["records_in"]
        sw.stop()
        metrics_mod.write_stage_metrics(
            settings.data_root,
            "gold",
            run_id,
            run_date,
            sw.started_at,
            sw.finished_at,
            "success",
            stats["records_in"],
            stats["records_out"],
            0,
            {
                "spark_master": settings.spark_master,
                "spark_cores_max": settings.spark_cores_max,
                "shuffle_partitions": settings.spark_shuffle_partitions,
            },
            {
                "table_counts": stats["table_counts"],
                "spark_app_id": spark.sparkContext.applicationId,
            },
        )
        logger.info("gold finished for %s", run_date)
        return 0
    except Exception as exc:
        sw.stop()
        finished = sw.finished_at or datetime.now(timezone.utc)
        started = sw.started_at or finished
        metrics_mod.write_stage_metrics(
            settings.data_root,
            "gold",
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
        logger.error("gold failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
