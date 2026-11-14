"""DuckDB view wiring."""

from __future__ import annotations

import tempfile

import pandas as pd

from gamepulse.analytics.duck import connect, create_views, query_df


def test_create_views_empty_local_lake() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = tmp
        con = connect(root)
        create_views(con, root)
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        assert "v_dim_character" not in tables


def test_query_df_returns_dataframe() -> None:
    con = connect(":memory:")
    df = query_df(con, "SELECT 1 AS n")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["n"]
    assert int(df.iloc[0]["n"]) == 1
