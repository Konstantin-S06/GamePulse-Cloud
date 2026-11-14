from datetime import datetime, timedelta, timezone

from gamepulse.common.metrics import read_all_metrics, sanitize_run_id, write_stage_metrics


def test_metrics_roundtrip(tmp_path):
    root = str(tmp_path)
    started = datetime(2026, 1, 5, 12, 0, 0, tzinfo=timezone.utc)
    finished = started + timedelta(seconds=2.5)
    uri = write_stage_metrics(
        root=root,
        stage="produce",
        run_id="local:run+1",
        run_date="2026-01-05",
        started_at=started,
        finished_at=finished,
        status="success",
        records_in=100,
        records_out=100,
        records_rejected=0,
        config={"x": 1},
        extra={"y": 2},
    )
    assert "local_run_1.json" in uri or "local_run" in uri
    assert sanitize_run_id("a:b+c") == "a_b_c"
    rows = read_all_metrics(root)
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "success"
    assert row["duration_seconds"] == 2.5
    assert abs(row["throughput_per_second"] - 40.0) < 1e-9
    assert set(row) >= {
        "stage",
        "run_id",
        "run_date",
        "status",
        "started_at",
        "finished_at",
        "duration_seconds",
        "records_in",
        "records_out",
        "records_rejected",
        "throughput_per_second",
        "config",
        "extra",
    }

    write_stage_metrics(
        root=root,
        stage="silver",
        run_id="fail-1",
        run_date="2026-01-05",
        started_at=started,
        finished_at=started,
        status="failed",
        records_in=0,
        records_out=0,
        records_rejected=0,
        config={},
        extra={},
    )
    all_rows = read_all_metrics(root)
    assert len(all_rows) == 2
    failed = next(r for r in all_rows if r["stage"] == "silver")
    assert failed["status"] == "failed"
    assert failed["throughput_per_second"] == 0
