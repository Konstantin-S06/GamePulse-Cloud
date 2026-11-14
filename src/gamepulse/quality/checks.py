"""Data quality checks via DuckDB views."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from duckdb import DuckDBPyConnection

from gamepulse.ingestion.bronze_writer import BRONZE_SCHEMA
from gamepulse.processing.schemas import SILVER_COLUMNS
from gamepulse.reference_data import CHARACTER_IDS, EVENT_SPECS, EVENT_TYPES, ITEM_IDS

Severity = str

BRONZE_COLUMN_NAMES = [f.name for f in BRONZE_SCHEMA]


@dataclass
class CheckResult:
    name: str
    severity: Severity
    passed: bool
    observed: Any
    expected: Any
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _scalar(con: DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> Any:
    row = con.execute(sql, params or []).fetchone()
    return row[0] if row else None


def _table_columns(con: DuckDBPyConnection, view: str) -> list[str]:
    rows = con.execute(f"DESCRIBE {view}").fetchall()
    return [r[0] for r in rows]


def _run_c01(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    count = _scalar(
        con,
        "SELECT COUNT(*) FROM v_bronze_events WHERE CAST(run_date AS VARCHAR) = ?",
        [run_date],
    )
    count = int(count or 0)
    return CheckResult(
        "C01_bronze_not_empty",
        "critical",
        count > 0,
        count,
        "> 0",
        "bronze row count for run_date",
    )


def _run_c02(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    # Hive partitioning exposes run_date from the path; it is not a Bronze column (4.7).
    try:
        cols = [c for c in _table_columns(con, "v_bronze_events") if c != "run_date"]
    except Exception as exc:
        return CheckResult(
            "C02_bronze_schema_matches",
            "critical",
            False,
            None,
            BRONZE_COLUMN_NAMES,
            str(exc),
        )
    ok = cols == BRONZE_COLUMN_NAMES
    return CheckResult(
        "C02_bronze_schema_matches",
        "critical",
        ok,
        cols,
        BRONZE_COLUMN_NAMES,
        "bronze columns must match spec 4.7",
    )


def _run_c03(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bronze = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_bronze_events WHERE CAST(run_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    silver = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_silver_events WHERE CAST(event_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    quarantine = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_quarantine_events WHERE CAST(run_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    ok = bronze == silver + quarantine
    return CheckResult(
        "C03_row_reconciliation",
        "critical",
        ok,
        {"bronze": bronze, "silver": silver, "quarantine": quarantine},
        "bronze = silver + quarantine",
        "row reconciliation",
    )


def _run_c04(con: DuckDBPyConnection, run_date: str, max_reject_rate: float) -> CheckResult:
    bronze = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_bronze_events WHERE CAST(run_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    quarantine = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_quarantine_events WHERE CAST(run_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    rate = 0.0 if bronze == 0 else quarantine / bronze
    ok = rate <= max_reject_rate
    return CheckResult(
        "C04_reject_rate",
        "critical",
        ok,
        round(rate, 6),
        f"<= {max_reject_rate}",
        "quarantine / bronze",
    )


def _run_c05(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    dupes = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM (
                SELECT event_id FROM v_silver_events
                WHERE CAST(event_date AS VARCHAR) = ?
                GROUP BY event_id HAVING COUNT(*) > 1
            ) d
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C05_silver_event_id_unique",
        "critical",
        dupes == 0,
        dupes,
        0,
        "duplicate event_id count",
    )


def _run_c06(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    required_types = [t for t, spec in EVENT_SPECS.items() if spec["requires_player"]]
    in_list = ", ".join(f"'{t}'" for t in required_types)
    bad = int(
        _scalar(
            con,
            f"""
            SELECT COUNT(*) FROM v_silver_events
            WHERE CAST(event_date AS VARCHAR) = ?
              AND (
                event_id IS NULL OR event_ts IS NULL OR match_id IS NULL OR event_type IS NULL
                OR (event_type IN ({in_list}) AND (player_id IS NULL OR TRIM(player_id) = ''))
              )
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C06_silver_required_not_null",
        "critical",
        bad == 0,
        bad,
        0,
        "null required fields",
    )


def _run_c07(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    types_list = ", ".join(f"'{t}'" for t in EVENT_TYPES)
    bad = int(
        _scalar(
            con,
            f"""
            SELECT COUNT(*) FROM v_silver_events
            WHERE CAST(event_date AS VARCHAR) = ?
              AND event_type NOT IN ({types_list})
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C07_silver_event_types_valid",
        "critical",
        bad == 0,
        bad,
        0,
        "invalid event_type rows",
    )


def _run_c08(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bad = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM v_silver_events
            WHERE CAST(event_date AS VARCHAR) = ?
              AND CAST(event_ts AS DATE) != CAST(? AS DATE)
            """,
            [run_date, run_date],
        )
        or 0
    )
    return CheckResult(
        "C08_silver_ts_in_run_date",
        "critical",
        bad == 0,
        bad,
        0,
        "event_ts outside run_date",
    )


def _run_c09(con: DuckDBPyConnection, _run_date: str) -> CheckResult:
    try:
        cols = _table_columns(con, "v_silver_events")
    except Exception as exc:
        return CheckResult(
            "C09_silver_schema_matches",
            "critical",
            False,
            None,
            SILVER_COLUMNS,
            str(exc),
        )
    ok = cols == SILVER_COLUMNS
    return CheckResult(
        "C09_silver_schema_matches",
        "critical",
        ok,
        cols,
        SILVER_COLUMNS,
        "silver columns must match spec 4.10",
    )


def _run_c10(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bad = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM v_silver_events
            WHERE CAST(event_date AS VARCHAR) = ?
              AND (
                (event_type = 'damage' AND (amount IS NULL OR amount < 1 OR amount > 1000))
                OR (event_type = 'item_purchase' AND (cost IS NULL OR cost < 1 OR cost > 5000))
              )
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C10_numeric_ranges",
        "critical",
        bad == 0,
        bad,
        0,
        "amount/cost out of range",
    )


def _run_c11(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bad = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM v_fact_match_events f
            LEFT JOIN v_dim_match d ON d.match_id = f.match_id
            WHERE CAST(f.match_date AS VARCHAR) = ? AND d.match_id IS NULL
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C11_fact_match_in_dim_match",
        "critical",
        bad == 0,
        bad,
        0,
        "orphan fact match_ids",
    )


def _run_c12(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bad = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM v_fact_match_events f
            LEFT JOIN v_dim_player p ON p.player_id = f.player_id
            WHERE CAST(f.match_date AS VARCHAR) = ?
              AND f.player_id IS NOT NULL
              AND p.player_id IS NULL
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C12_fact_player_in_dim_player",
        "critical",
        bad == 0,
        bad,
        0,
        "unknown player_id in facts",
    )


def _run_c13(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    char_ids = ", ".join(f"'{c}'" for c in CHARACTER_IDS)
    item_ids = ", ".join(f"'{i}'" for i in ITEM_IDS)
    bad_char = int(
        _scalar(
            con,
            f"""
            SELECT COUNT(*) FROM v_fact_match_events
            WHERE CAST(match_date AS VARCHAR) = ?
              AND actor_character_id IS NOT NULL
              AND actor_character_id NOT IN ({char_ids})
            """,
            [run_date],
        )
        or 0
    )
    bad_item = int(
        _scalar(
            con,
            f"""
            SELECT COUNT(*) FROM v_fact_match_events
            WHERE CAST(match_date AS VARCHAR) = ?
              AND item_id IS NOT NULL
              AND item_id NOT IN ({item_ids})
            """,
            [run_date],
        )
        or 0
    )
    bad = bad_char + bad_item
    return CheckResult(
        "C13_fact_refs_dims",
        "critical",
        bad == 0,
        {"bad_character": bad_char, "bad_item": bad_item},
        0,
        "invalid dimension references",
    )


def _run_c14(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    dupes = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM (
                SELECT match_id, player_id FROM v_fact_player_match
                WHERE CAST(match_date AS VARCHAR) = ?
                GROUP BY match_id, player_id HAVING COUNT(*) > 1
            ) d
            """,
            [run_date],
        )
        or 0
    )
    return CheckResult(
        "C14_player_match_unique",
        "critical",
        dupes == 0,
        dupes,
        0,
        "duplicate player_match keys",
    )


def _run_c15(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    dm = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_dim_match WHERE CAST(match_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    fe = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_fact_match_events WHERE CAST(match_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    fp = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_fact_player_match WHERE CAST(match_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    ok = dm > 0 and fe > 0 and fp > 0
    return CheckResult(
        "C15_gold_not_empty",
        "critical",
        ok,
        {"dim_match": dm, "fact_match_events": fe, "fact_player_match": fp},
        "all > 0",
        "gold tables for date",
    )


def _run_c16(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bad = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM (
                SELECT m.match_id
                FROM v_dim_match m
                LEFT JOIN (
                    SELECT match_id, COUNT(*) AS n
                    FROM v_fact_player_match
                    WHERE CAST(match_date AS VARCHAR) = ?
                    GROUP BY match_id
                ) p ON p.match_id = m.match_id
                WHERE CAST(m.match_date AS VARCHAR) = ?
                  AND m.is_complete
                  AND COALESCE(p.n, 0) != 10
            ) x
            """,
            [run_date, run_date],
        )
        or 0
    )
    return CheckResult(
        "C16_complete_matches_have_10_stats",
        "warn",
        bad == 0,
        bad,
        0,
        "complete matches without 10 stats rows",
    )


def _run_c17(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    total = int(
        _scalar(
            con,
            "SELECT COUNT(*) FROM v_dim_match WHERE CAST(match_date AS VARCHAR) = ?",
            [run_date],
        )
        or 0
    )
    incomplete = int(
        _scalar(
            con,
            """
            SELECT COUNT(*) FROM v_dim_match
            WHERE CAST(match_date AS VARCHAR) = ? AND NOT is_complete
            """,
            [run_date],
        )
        or 0
    )
    ratio = 0.0 if total == 0 else incomplete / total
    ok = ratio <= 0.02
    return CheckResult(
        "C17_incomplete_match_ratio",
        "warn",
        ok,
        round(ratio, 6),
        "<= 0.02",
        "incomplete matches ratio",
    )


def _run_c18(con: DuckDBPyConnection, run_date: str) -> CheckResult:
    bad = int(
        _scalar(
            con,
            """
            WITH rebuilt AS (
                SELECT
                    match_id,
                    player_id,
                    SUM(CASE WHEN event_type = 'elimination' THEN 1 ELSE 0 END) AS kills,
                    SUM(CASE WHEN event_type = 'assist' THEN 1 ELSE 0 END) AS assists,
                    SUM(
                        CASE WHEN event_type = 'damage' THEN damage_amount ELSE 0 END
                    ) AS damage_dealt
                FROM v_fact_match_events
                WHERE CAST(match_date AS VARCHAR) = ? AND player_id IS NOT NULL
                GROUP BY match_id, player_id
            ),
            mismatches AS (
                SELECT p.match_id, p.player_id
                FROM v_fact_player_match p
                JOIN rebuilt r ON r.match_id = p.match_id AND r.player_id = p.player_id
                WHERE CAST(p.match_date AS VARCHAR) = ?
                  AND (
                    p.kills != r.kills
                    OR p.assists != r.assists
                    OR p.damage_dealt != r.damage_dealt
                  )
            )
            SELECT COUNT(*) FROM mismatches
            """,
            [run_date, run_date],
        )
        or 0
    )
    return CheckResult(
        "C18_stats_match_events",
        "warn",
        bad == 0,
        bad,
        0,
        "player stats mismatches vs events",
    )


def run_all_checks(
    con: DuckDBPyConnection,
    run_date: str,
    max_reject_rate: float,
) -> list[CheckResult]:
    return [
        _run_c01(con, run_date),
        _run_c02(con, run_date),
        _run_c03(con, run_date),
        _run_c04(con, run_date, max_reject_rate),
        _run_c05(con, run_date),
        _run_c06(con, run_date),
        _run_c07(con, run_date),
        _run_c08(con, run_date),
        _run_c09(con, run_date),
        _run_c10(con, run_date),
        _run_c11(con, run_date),
        _run_c12(con, run_date),
        _run_c13(con, run_date),
        _run_c14(con, run_date),
        _run_c15(con, run_date),
        _run_c16(con, run_date),
        _run_c17(con, run_date),
        _run_c18(con, run_date),
    ]
