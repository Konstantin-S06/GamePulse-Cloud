"""Dashboard helpers: DuckDB views and metrics."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
from duckdb import DuckDBPyConnection

from gamepulse.analytics import duck as duck_mod
from gamepulse.common import metrics as metrics_mod
from gamepulse.config import get_settings


@lru_cache(maxsize=1)
def _connection() -> DuckDBPyConnection:
    settings = get_settings()
    con = duck_mod.connect(settings.data_root)
    duck_mod.create_views(con, settings.data_root)
    return con


def get_data_root() -> str:
    return get_settings().data_root


def query_sql(sql: str):
    return duck_mod.query_df(_connection(), sql)


def load_daily_overview():
    return query_sql(
        "SELECT * FROM v_agg_daily_overview ORDER BY match_date"
    )


def load_character_daily():
    return query_sql(
        """
        SELECT d.match_date, d.character_id, c.character_name, c.role,
               d.games, d.wins, d.win_rate, d.avg_kills, d.avg_damage_dealt
        FROM v_agg_character_daily d
        LEFT JOIN v_dim_character c ON d.character_id = c.character_id
        ORDER BY match_date, character_id
        """
    )


def load_items_summary():
    return query_sql(
        """
        SELECT item_id, item_name, category, cost
        FROM v_dim_item
        ORDER BY item_id
        """
    )


def load_all_metrics() -> list[dict[str, Any]]:
    return metrics_mod.read_all_metrics(get_settings().data_root)


def metrics_for_run_date(run_date: str) -> list[dict[str, Any]]:
    return [m for m in load_all_metrics() if m.get("run_date") == run_date]


def quarantine_event_count() -> int:
    try:
        row = _connection().execute(
            "SELECT COUNT(*) AS n FROM v_quarantine_events"
        ).fetchone()
        return int(row[0]) if row else 0
    except duckdb.Error:
        return 0


def quarantine_ingestion_count() -> int:
    try:
        row = _connection().execute(
            "SELECT COUNT(*) AS n FROM v_quarantine_ingestion"
        ).fetchone()
        return int(row[0]) if row else 0
    except duckdb.Error:
        return 0


def list_benchmark_csvs() -> list[Path]:
    root = _PROJECT_ROOT()
    results = root / "benchmarks" / "results"
    if not results.is_dir():
        return []
    return sorted(results.glob("*.csv"))


def _PROJECT_ROOT() -> Path:
    return Path(__file__).resolve().parents[2]