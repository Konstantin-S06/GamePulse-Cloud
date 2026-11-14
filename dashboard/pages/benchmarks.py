"""Benchmark results placeholder."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.lib import data

st.header("Benchmarks")
st.caption(
    "Results are written to benchmarks/results/ by benchmarks/run_benchmark.py. "
    "Spark runs here use two workers on one host, not multiple machines."
)

paths = data.list_benchmark_csvs()
if not paths:
    st.info(
        "No CSV files in benchmarks/results/. Run a benchmark from the "
        "runner container after pipeline data exists."
    )
    st.code(
        "python benchmarks/run_benchmark.py --config "
        "benchmarks/configs/partition_shuffle.yaml "
        "--run-date 2026-01-05 --run-id bench-1",
        language="bash",
    )
    st.stop()

choice = st.selectbox("Result file", [p.name for p in paths])
selected = Path("benchmarks/results") / choice
df = pd.read_csv(selected)
st.dataframe(df, use_container_width=True)
if "duration_seconds" in df.columns and "variant" in df.columns:
    pivot = df.pivot_table(
        index="variant",
        columns="stage",
        values="duration_seconds",
        aggfunc="mean",
    )
    st.subheader("Duration (seconds)")
    st.dataframe(pivot, use_container_width=True)