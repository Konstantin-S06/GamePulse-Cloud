"""Run quality checks and write metrics."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from gamepulse.analytics.duck import connect, create_views
from gamepulse.common import metrics
from gamepulse.common.logging_setup import get_logger
from gamepulse.config import get_settings
from gamepulse.quality.checks import CheckResult, run_all_checks

logger = get_logger(__name__)


def _print_table(results: list[CheckResult]) -> None:
    headers = ("name", "severity", "passed", "observed", "expected", "detail")
    rows = [
        (
            r.name,
            r.severity,
            str(r.passed),
            str(r.observed),
            str(r.expected),
            r.detail,
        )
        for r in results
    ]
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    fmt = "  ".join(f"{{:{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*("-" * w for w in widths)))
    for row in rows:
        print(fmt.format(*row))


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Run data quality checks")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    stopwatch = metrics.Stopwatch()
    stopwatch.start()
    results: list[CheckResult] = []
    status = "success"

    try:
        con = connect(settings.data_root)
        create_views(con, settings.data_root)
        results = run_all_checks(con, args.run_date, settings.max_reject_rate)
        _print_table(results)
        failed_critical = [r for r in results if r.severity == "critical" and not r.passed]
        if failed_critical:
            status = "failed"
    except Exception:
        status = "failed"
        logger.exception("quality checks failed")
        stopwatch.stop()
        metrics.write_stage_metrics(
            settings.data_root,
            "quality",
            args.run_id,
            args.run_date,
            stopwatch.started_at or datetime.now(timezone.utc),
            stopwatch.finished_at or datetime.now(timezone.utc),
            "failed",
            0,
            0,
            0,
            {"max_reject_rate": settings.max_reject_rate},
            {"checks": [r.to_dict() for r in results]},
        )
        return 1

    stopwatch.stop()
    passed = sum(1 for r in results if r.passed)
    failed_critical = sum(1 for r in results if r.severity == "critical" and not r.passed)
    metrics.write_stage_metrics(
        settings.data_root,
        "quality",
        args.run_id,
        args.run_date,
        stopwatch.started_at or datetime.now(timezone.utc),
        stopwatch.finished_at or datetime.now(timezone.utc),
        status,
        len(results),
        passed,
        failed_critical,
        {"max_reject_rate": settings.max_reject_rate},
        {"checks": [r.to_dict() for r in results]},
    )
    return 1 if status == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
