"""Character and item summaries."""

from __future__ import annotations

import streamlit as st

from dashboard.lib import data

st.header("Gameplay")
st.caption("Character daily stats and item catalog from Gold dimensions.")

tab_char, tab_items = st.tabs(["Characters", "Items"])

with tab_char:
    try:
        chars = data.load_character_daily()
    except Exception as exc:
        st.warning(f"Character aggregates unavailable: {exc}")
        chars = None
    if chars is not None and not chars.empty:
        st.dataframe(chars, use_container_width=True)
        if "win_rate" in chars.columns:
            by_char = (
                chars.groupby("character_name", as_index=False)["win_rate"]
                .mean()
                .sort_values("win_rate", ascending=False)
            )
            st.bar_chart(by_char.set_index("character_name")["win_rate"])
    else:
        st.info("No agg_character_daily rows yet.")

with tab_items:
    try:
        items = data.load_items_summary()
    except Exception as exc:
        st.warning(f"Item dimension unavailable: {exc}")
        items = None
    if items is not None and not items.empty:
        st.dataframe(items, use_container_width=True)
    else:
        st.info("No dim_item rows yet.")