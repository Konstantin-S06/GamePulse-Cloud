"""Daily GamePulse pipeline DAG."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PY = "/opt/gamepulse-venv/bin/python"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="gamepulse_daily",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"num_matches": 200, "seed": 42, "corrupt_rate": 0.0},
    default_args=default_args,
) as dag:
    preflight = BashOperator(
        task_id="preflight",
        bash_command=f"{PY} -m gamepulse.ops.preflight",
    )
    create_topics = BashOperator(
        task_id="create_topics",
        bash_command=f"{PY} -m gamepulse.ingestion.admin create",
    )
    produce_events = BashOperator(
        task_id="produce_events",
        bash_command=(
            PY + " -m gamepulse.ingestion.produce_cli --run-date {{ ds }} "
            "--run-id '{{ run_id }}' --num-matches {{ params.num_matches }} "
            "--seed {{ params.seed }} --corrupt-rate {{ params.corrupt_rate }}"
        ),
    )
    ingest_events = BashOperator(
        task_id="ingest_events",
        bash_command=(
            PY
            + " -m gamepulse.ingestion.ingest_cli --run-date {{ ds }} "
            "--run-id '{{ run_id }}' --num-consumers 3"
        ),
    )
    archive_dlq = BashOperator(
        task_id="archive_dlq",
        bash_command=(
            PY + " -m gamepulse.ingestion.dlq_archive --run-date {{ ds }} "
            "--run-id '{{ run_id }}'"
        ),
    )
    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            PY + " -m gamepulse.processing.silver_job --run-date {{ ds }} "
            "--run-id '{{ run_id }}'"
        ),
    )
    spark_gold = BashOperator(
        task_id="spark_gold",
        bash_command=(
            PY + " -m gamepulse.processing.gold_job --run-date {{ ds }} "
            "--run-id '{{ run_id }}'"
        ),
    )
    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            PY + " -m gamepulse.quality.cli --run-date {{ ds }} "
            "--run-id '{{ run_id }}'"
        ),
    )

    (
        preflight
        >> create_topics
        >> produce_events
        >> ingest_events
        >> archive_dlq
        >> spark_silver
        >> spark_gold
        >> quality_checks
    )