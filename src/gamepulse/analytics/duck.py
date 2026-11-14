"""DuckDB connection and view setup."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection

from gamepulse.common.logging_setup import get_logger

logger = get_logger(__name__)

_VIEWS_PATH = Path(__file__).resolve().parents[3] / "sql" / "views.sql"


def connect(root: str) -> DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    if root.startswith("s3://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        region = os.environ.get("AWS_REGION", "us-east-1")
        con.execute(
            f"""
            CREATE OR REPLACE SECRET gp_s3 (
                TYPE S3,
                KEY_ID '{key_id}',
                SECRET '{secret}',
                REGION '{region}'
            )
            """
        )
    return con


def query_df(con: DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """Run SQL and return a pandas DataFrame."""
    return con.execute(sql).fetchdf()


def create_views(con: DuckDBPyConnection, root: str) -> None:
    sql_text = _VIEWS_PATH.read_text(encoding="utf-8")
    root_norm = root.rstrip("/")
    parts: list[str] = []
    buf: list[str] = []
    for line in sql_text.splitlines():
        if line.strip() == "-- split":
            if buf:
                parts.append("\n".join(buf))
                buf = []
            continue
        buf.append(line)
    if buf:
        parts.append("\n".join(buf))
    for part in parts:
        stmt = part.strip().replace("{ROOT}", root_norm)
        _run_statement(con, stmt)


def _run_statement(con: DuckDBPyConnection, stmt: str) -> None:
    if not stmt:
        return
    try:
        con.execute(stmt)
    except Exception as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "could not open" in msg or "io error" in msg:
            logger.warning("skipping view statement (%s)", exc)
            return
        raise
