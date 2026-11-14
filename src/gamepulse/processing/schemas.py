"""Spark schemas for event parsing."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

PAYLOAD_FIELD_NAMES = [
    "map_name",
    "game_mode",
    "region",
    "team",
    "skill_rating",
    "character_id",
    "item_id",
    "cost",
    "target_player_id",
    "amount",
    "damage_source",
    "victim_player_id",
    "reason",
    "winning_team",
    "duration_seconds",
    "blue_score",
    "red_score",
    "kills",
    "deaths",
    "assists",
    "damage_dealt",
    "damage_received",
    "gold_spent",
    "won",
]

PAYLOAD_SCHEMA = StructType([StructField(name, StringType(), True) for name in PAYLOAD_FIELD_NAMES])

ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("match_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
    ]
)

# Spec 4.10 Silver output columns (partition columns last).
SILVER_COLUMNS = [
    "event_id",
    "event_ts",
    "match_id",
    "player_id",
    "event_version",
    "map_name",
    "game_mode",
    "region",
    "team",
    "skill_rating",
    "character_id",
    "item_id",
    "cost",
    "target_player_id",
    "amount",
    "damage_source",
    "victim_player_id",
    "reason",
    "winning_team",
    "duration_seconds",
    "blue_score",
    "red_score",
    "kills",
    "deaths",
    "assists",
    "damage_dealt",
    "damage_received",
    "gold_spent",
    "won",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
    "silver_processed_at",
    "event_date",
    "event_type",
]
