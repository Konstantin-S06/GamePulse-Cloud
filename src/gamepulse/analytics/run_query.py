"""Run named analytics SQL queries."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

from gamepulse.analytics.duck import connect, create_views
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings

logger = get_logger(__name__)

_SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "analytics"


def load_query(name: str) -> str:
    path = _SQL_DIR / f"{name}.sql"
    if not path.is_file():
        path = _SQL_DIR / name
        if not path.is_file():
            raise FileNotFoundError(f"query not found: {name}")
    return path.read_text(encoding="utf-8").strip()


def run_named_query(con: duckdb.DuckDBPyConnection, name: str) -> pd.DataFrame:
    sql = load_query(name)
    return con.execute(sql).fetchdf()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run analytics query")
    parser.add_argument("name", help="query file name, e.g. 01_avg_damage_per_match")
    args = parser.parse_args(argv)

    settings = get_settings()
    con = connect(settings.data_root)
    create_views(con, settings.data_root)
    try:
        df = run_named_query(con, args.name)
    except Exception:
        logger.exception("query %s failed", args.name)
        return 1
    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
