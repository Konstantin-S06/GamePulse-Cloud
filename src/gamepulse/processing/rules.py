"""Silver validation rules using Spark column expressions only."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gamepulse.reference_data import (
    CHARACTER_IDS,
    DAMAGE_SOURCES,
    EVENT_SPECS,
    EVENT_TYPES,
    EVENT_VERSION,
    GAME_MODE,
    ITEM_IDS,
    LEAVE_REASONS,
    MAPS,
    MATCH_ID_PATTERN,
    PLAYER_ID_PATTERN,
    RANGES,
    REGIONS,
    TEAMS,
)

INT_RANGE_FIELDS = {
    "skill_rating": "int",
    "cost": "int",
    "amount": "int",
    "duration_seconds": "int",
    "blue_score": "int",
    "red_score": "int",
    "kills": "int",
    "deaths": "int",
    "assists": "int",
    "damage_dealt": "long",
    "damage_received": "long",
    "gold_spent": "int",
}


def add_reason(df: DataFrame, reason: str, valid_expr: Column) -> DataFrame:
    bad = ~F.coalesce(valid_expr, F.lit(False))
    return df.withColumn(
        "rejection_reason",
        F.when(F.col("rejection_reason").isNull() & bad, F.lit(reason)).otherwise(
            F.col("rejection_reason")
        ),
    )


def per_type_valid(condition_for_type: Callable[[str], Column]) -> Column:
    expr = F.lit(True)
    for event_type in EVENT_TYPES:
        expr = expr & F.when(
            F.col("event_type") == event_type, condition_for_type(event_type)
        ).otherwise(F.lit(True))
    return expr


def _non_empty(col: str) -> Column:
    return F.col(col).isNotNull() & (F.trim(F.col(col)) != "")


def _payload_fields_ok(event_type: str) -> Column:
    expr = F.lit(True)
    for field_name in EVENT_SPECS[event_type]["payload"]:
        expr = expr & _non_empty(field_name)
    return expr


def _numeric_ok(event_type: str) -> Column:
    expr = F.lit(True)
    for field_name, py_type in EVENT_SPECS[event_type]["payload"].items():
        if py_type is bool:
            if field_name == "won":
                expr = expr & F.col("won").isin("true", "false")
            continue
        if py_type is not int:
            continue
        cast_type = INT_RANGE_FIELDS.get(field_name, "int")
        lo, hi = RANGES[field_name]
        casted = F.col(field_name).cast(cast_type)
        expr = expr & casted.isNotNull() & casted.between(lo, hi)
    return expr


def _reference_ok(event_type: str) -> Column:
    expr = F.lit(True)
    payload = EVENT_SPECS[event_type]["payload"]
    if "character_id" in payload:
        expr = expr & F.col("character_id").isin(CHARACTER_IDS)
    if "item_id" in payload:
        expr = expr & F.col("item_id").isin(ITEM_IDS)
    if "team" in payload:
        expr = expr & F.col("team").isin(TEAMS)
    if "winning_team" in payload:
        expr = expr & F.col("winning_team").isin(TEAMS)
    if "region" in payload:
        expr = expr & F.col("region").isin(REGIONS)
    if "map_name" in payload:
        expr = expr & F.col("map_name").isin(MAPS)
    if "game_mode" in payload:
        expr = expr & (F.col("game_mode") == GAME_MODE)
    if "damage_source" in payload:
        expr = expr & F.col("damage_source").isin(DAMAGE_SOURCES)
    if "reason" in payload:
        expr = expr & F.col("reason").isin(LEAVE_REASONS)
    if "target_player_id" in payload:
        expr = expr & F.col("target_player_id").rlike(PLAYER_ID_PATTERN)
    if "victim_player_id" in payload:
        expr = expr & F.col("victim_player_id").rlike(PLAYER_ID_PATTERN)
    return expr


def apply_row_rules(df: DataFrame, run_date: str) -> DataFrame:
    """Apply validation rules 1-11 in order."""
    df = df.withColumn("rejection_reason", F.lit(None).cast("string"))

    df = add_reason(df, "invalid_json", F.col("_parsed").isNotNull())

    required_top = (
        _non_empty("event_id")
        & _non_empty("event_type")
        & _non_empty("event_ts")
        & _non_empty("match_id")
    )
    df = add_reason(df, "missing_required_field", required_top)

    df = add_reason(
        df,
        "unsupported_event_version",
        F.col("event_version") == F.lit(str(EVENT_VERSION)),
    )

    df = add_reason(df, "unknown_event_type", F.col("event_type").isin(EVENT_TYPES))

    df = add_reason(df, "invalid_timestamp", F.col("ts").isNotNull())

    df = add_reason(
        df,
        "timestamp_out_of_range",
        F.to_date(F.col("ts")) == F.lit(run_date),
    )

    player_required = per_type_valid(
        lambda t: _non_empty("player_id") if EVENT_SPECS[t]["requires_player"] else F.lit(True)
    )
    df = add_reason(df, "missing_player_id", player_required)

    id_fmt = F.col("match_id").rlike(MATCH_ID_PATTERN) & (
        F.col("player_id").isNull()
        | (F.trim(F.col("player_id")) == "")
        | F.col("player_id").rlike(PLAYER_ID_PATTERN)
    )
    df = add_reason(df, "invalid_id_format", id_fmt)

    df = add_reason(df, "missing_payload_field", per_type_valid(_payload_fields_ok))
    df = add_reason(df, "invalid_numeric_value", per_type_valid(_numeric_ok))
    df = add_reason(df, "invalid_reference_value", per_type_valid(_reference_ok))

    return df


def check_membership(df: DataFrame, members: DataFrame, id_col: str) -> DataFrame:
    joined = df.join(
        members.withColumnRenamed("player_id", id_col),
        on=["match_id", id_col],
        how="left",
    )
    valid = (
        F.col(id_col).isNull()
        | (F.col("event_type") == F.lit("player_join"))
        | F.col("is_member").isNotNull()
    )
    result = add_reason(joined, "unknown_player_in_match", valid)
    return result.drop("is_member")
