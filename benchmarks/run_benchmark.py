"""Run Silver and Gold under benchmark Spark settings."""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from gamepulse.common.logging_setup import get_logger

logger = get_logger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_RESULTS = _ROOT / "benchmarks" / "results"
_FIELDS = [
    "benchmark",
    "variant",
    "stage",
    "run_date",
    "run_id",
    "status",
    "duration_seconds",
    "records_in",
    "records_out",
    "spark_master",
    "spark_shuffle_partitions",
    "spark_cores_max",
    "started_at",
]


def _load_config(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _env_for_variant(base: dict[str, str], variant: dict) -> dict[str, str]:
    env = dict(base)
    mapping = {
        "spark_master": "SPARK_MASTER",
        "spark_shuffle_partitions": "SPARK_SHUFFLE_PARTITIONS",
        "spark_cores_max": "SPARK_CORES_MAX",
        "spark_executor_memory": "SPARK_EXECUTOR_MEMORY",
        "spark_driver_memory": "SPARK_DRIVER_MEMORY",
    }
    for key, env_key in mapping.items():
        if key in variant:
            env[env_key] = str(variant[key])
    return env


def _run_stage(module: str, run_date: str, run_id: str, env: dict[str, str]) -> tuple[int, float]:
    started = datetime.now(timezone.utc)
    proc = subprocess.run(
        [sys.executable, "-m", module, "--run-date", run_date, "--run-id", run_id],
        env=env,
        cwd=_ROOT,
        capture_output=True,
        text=True,
    )
    finished = datetime.now(timezone.utc)
    duration = (finished - started).total_seconds()
    if proc.returncode != 0:
        logger.error("%s failed: %s", module, proc.stderr.strip())
    return proc.returncode, duration


def _read_stage_metrics(
    env: dict[str, str], stage: str, run_date: str, run_id: str
) -> dict | None:
    os.environ.update(env)
    from gamepulse.common import metrics as metrics_mod

    root = env.get("DATA_ROOT", "/opt/gamepulse/data/lake")
    safe = metrics_mod.sanitize_run_id(run_id)
    uri = f"{root.rstrip('/')}/metrics/stage={stage}/run_date={run_date}/{safe}.json"
    from gamepulse.common import storage

    if not storage.exists(uri):
        return None
    return storage.read_json(uri)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Spark benchmark variants")
    parser.add_argument("--config", required=True, help="YAML config path")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.is_file():
        logger.error("config not found: %s", cfg_path)
        return 1
    cfg = _load_config(cfg_path)
    benchmark_name = cfg.get("name", cfg_path.stem)
    variants = cfg.get("variants") or []
    if not variants:
        logger.error("no variants in config")
        return 1

    _RESULTS.mkdir(parents=True, exist_ok=True)
    out_path = _RESULTS / f"{benchmark_name}_{args.run_date}.csv"
    write_header = not out_path.exists()
    base_env = dict(os.environ)

    rows: list[dict] = []
    for variant in variants:
        label = variant.get("label", "default")
        env = _env_for_variant(base_env, variant)
        for stage, module in (
            ("silver", "gamepulse.processing.silver_job"),
            ("gold", "gamepulse.processing.gold_job"),
        ):
            stage_run_id = f"{args.run_id}-{label}-{stage}"
            code, duration = _run_stage(module, args.run_date, stage_run_id, env)
            status = "success" if code == 0 else "failed"
            metrics = _read_stage_metrics(env, stage, args.run_date, stage_run_id)
            row = {
                "benchmark": benchmark_name,
                "variant": label,
                "stage": stage,
                "run_date": args.run_date,
                "run_id": stage_run_id,
                "status": status,
                "duration_seconds": round(
                    metrics["duration_seconds"] if metrics else duration, 3
                ),
                "records_in": metrics.get("records_in", 0) if metrics else 0,
                "records_out": metrics.get("records_out", 0) if metrics else 0,
                "spark_master": env.get("SPARK_MASTER", ""),
                "spark_shuffle_partitions": env.get("SPARK_SHUFFLE_PARTITIONS", ""),
                "spark_cores_max": env.get("SPARK_CORES_MAX", ""),
                "started_at": metrics.get("started_at", "") if metrics else "",
            }
            rows.append(row)

    with out_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    logger.info("wrote %s (%s rows)", out_path, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())