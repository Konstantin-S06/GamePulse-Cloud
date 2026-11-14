"""GamePulse Streamlit entry."""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="GamePulse",
    page_icon="G",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("GamePulse")
st.caption(
    "Synthetic arena data lake dashboard. All metrics and charts use Gold "
    "Parquet under DATA_ROOT."
)
st.info(
    "Data is synthetic and is not real player data. Spark benchmarks on this "
    "project use two worker containers on one machine."
)