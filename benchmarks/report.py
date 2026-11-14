"""Print a short summary from benchmark CSV results."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
_RESULTS = _ROOT / "benchmarks" / "results"


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize benchmark CSV")
    parser.add_argument(
        "csv",
        nargs="?",
        help="Path to results CSV (default: newest in benchmarks/results)",
    )
    args = parser.parse_args()

    if args.csv:
        path = Path(args.csv)
    else:
        files = sorted(_RESULTS.glob("*.csv"), key=lambda p: p.stat().st_mtime)
        if not files:
            print("No CSV files in benchmarks/results", file=sys.stderr)
            return 1
        path = files[-1]

    if not path.is_file():
        print(f"not found: {path}", file=sys.stderr)
        return 1

    df = pd.read_csv(path)
    print(f"File: {path}")
    print(f"Rows: {len(df)}")
    if df.empty:
        return 0

    group_cols = ["benchmark", "variant", "stage"]
    if all(c in df.columns for c in group_cols):
        summary = (
            df.groupby(group_cols, dropna=False)
            .agg(
                duration_seconds=("duration_seconds", "mean"),
                records_in=("records_in", "max"),
                status=("status", "first"),
            )
            .reset_index()
        )
        print(summary.to_string(index=False))
    else:
        print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())