"""Benchmark report CLI."""

from __future__ import annotations

import csv
import subprocess
import sys
import tempfile
from pathlib import Path


def test_report_prints_summary() -> None:
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "benchmark",
                    "variant",
                    "stage",
                    "duration_seconds",
                    "records_in",
                    "status",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "benchmark": "partition_shuffle",
                    "variant": "shuffle_8",
                    "stage": "silver",
                    "duration_seconds": 12.5,
                    "records_in": 1000,
                    "status": "success",
                }
            )
        proc = subprocess.run(
            [sys.executable, str(root / "benchmarks" / "report.py"), str(path)],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0
        assert "partition_shuffle" in proc.stdout
        assert "12.5" in proc.stdout
