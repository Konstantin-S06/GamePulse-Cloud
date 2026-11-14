"""Run silver, gold, and quality for one run date."""

from __future__ import annotations

import argparse
import importlib
import sys

from gamepulse.common.logging_setup import get_logger
from gamepulse.processing import gold_job, silver_job

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run processing pipeline for a date")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    code = silver_job.main(["--run-date", args.run_date, "--run-id", args.run_id])
    if code != 0:
        return code
    code = gold_job.main(["--run-date", args.run_date, "--run-id", args.run_id])
    if code != 0:
        return code

    try:
        quality_cli = importlib.import_module("gamepulse.quality.cli")
        return int(quality_cli.main(["--run-date", args.run_date, "--run-id", args.run_id]))
    except ModuleNotFoundError:
        logger.warning("quality.cli not available; skipping quality stage")
        return 0
    except AttributeError:
        logger.warning("quality.cli has no main(); skipping quality stage")
        return 0


if __name__ == "__main__":
    sys.exit(main())
