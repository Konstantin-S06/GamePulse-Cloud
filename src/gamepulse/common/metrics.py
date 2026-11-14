"""Stage metrics JSON helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from gamepulse.common import storage


def sanitize_run_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value)


@dataclass
class Stopwatch:
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def start(self) -> None:
        self.started_at = datetime.now(timezone.utc)

    def stop(self) -> None:
        self.finished_at = datetime.now(timezone.utc)

    @property
    def duration_seconds(self) -> float:
        if self.started_at is None or self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()


def write_stage_metrics(
    root: str,
    stage: str,
    run_id: str,
    run_date: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    records_in: int,
    records_out: int,
    records_rejected: int,
    config: dict[str, Any],
    extra: dict[str, Any],
) -> str:
    duration = (finished_at - started_at).total_seconds()
    duration_rounded = round(duration, 3)
    throughput = 0.0 if duration == 0 else records_in / duration
    payload = {
        "stage": stage,
        "run_id": run_id,
        "run_date": run_date,
        "status": status,
        "started_at": started_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z",
        "finished_at": finished_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z",
        "duration_seconds": duration_rounded,
        "records_in": records_in,
        "records_out": records_out,
        "records_rejected": records_rejected,
        "throughput_per_second": throughput,
        "config": config,
        "extra": extra,
    }
    safe_id = sanitize_run_id(run_id)
    uri = storage.join_uri(root, f"metrics/stage={stage}/run_date={run_date}/{safe_id}.json")
    storage.write_json(payload, uri)
    return uri


def read_all_metrics(root: str) -> list[dict[str, Any]]:
    prefix = storage.join_uri(root, "metrics")
    results: list[dict[str, Any]] = []
    for uri in storage.list_files(prefix):
        if uri.endswith(".json"):
            results.append(storage.read_json(uri))
    return results
