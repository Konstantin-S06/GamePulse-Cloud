"""Pipeline metrics and quarantine counts."""

from __future__ import annotations

import streamlit as st

from dashboard.lib import data

st.header("Data quality")
st.caption(f"DATA_ROOT: {data.get_data_root()}")

metrics = data.load_all_metrics()
if not metrics:
    st.info("No metrics JSON files under metrics/.")
else:
    rows = sorted(
        metrics,
        key=lambda m: (
            m.get("run_date", ""),
            m.get("stage", ""),
            m.get("started_at", ""),
        ),
        reverse=True,
    )
    st.dataframe(rows, use_container_width=True)

run_date = st.text_input("Filter metrics by run_date (YYYY-MM-DD)", value="")
if run_date.strip():
    filtered = data.metrics_for_run_date(run_date.strip())
    if filtered:
        st.subheader(f"Runs on {run_date.strip()}")
        st.dataframe(filtered, use_container_width=True)
    else:
        st.write("No metrics for that date.")

col1, col2 = st.columns(2)
col1.metric("Quarantined events", data.quarantine_event_count())
col2.metric("Quarantined ingestion rows", data.quarantine_ingestion_count())