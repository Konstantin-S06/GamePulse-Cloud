"""Daily overview aggregates."""

from __future__ import annotations

import streamlit as st

from dashboard.lib import data

st.header("Daily overview")
st.caption(f"DATA_ROOT: {data.get_data_root()}")

try:
    df = data.load_daily_overview()
except Exception as exc:
    st.warning(f"No Gold overview data yet: {exc}")
    st.stop()

if df.empty:
    st.info("v_agg_daily_overview is empty. Run the pipeline for at least one date.")
    st.stop()

st.dataframe(df, use_container_width=True)
st.line_chart(df.set_index("match_date")[["matches", "events", "active_players"]])