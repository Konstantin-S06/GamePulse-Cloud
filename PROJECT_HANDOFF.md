# Project handoff

## Current state
Phase: local Docker E2E working (produce → ingest → silver → gold → quality)
Last finished task: add duck.query_df for Streamlit dashboard Gold pages
Next task: H2 AWS terraform + S3; wire IT-idem tests; optional benchmarks
Last updated: 2026-11-14

## Task log
| Task | Status | Commit | Notes |
|---|---|---|---|
| H0 | done | pending | PROJECT_SPEC, rules, handoff |
| T0.1 | done | pending | scaffold |
| T0.2–T0.4 | done | pending | Dockerfile, compose, Makefile, config, logging, CI |
| T1.1–T1.6 | done | pending | simulator + docs/simulator.md; local sample OK |
| T2.1–T2.5 | done | pending | storage, metrics, terraform, s3 smoke, CI terraform job |
| T3–T7 | done | pending | kafka ingest, spark silver/gold, quality, duckdb, airflow DAG |
| T8–T10 | done | pending | benchmarks scaffold, dashboard, docs, README |
| Docker E2E | done | pending | local lake via runner + SPARK_MASTER=local[2] |
| H2 | pending | — | terraform apply + IAM key into .env (human) |

## Decisions made that the spec did not cover
- Final review fixes live in PROJECT_SPEC.md (4.4 clamp, player cap, IAM revoke, 4.18 idempotency).
- Repo root is the project root (no nested gamepulse-cloud folder).
- Local lake Spark uses `SPARK_MASTER=local[2]` and `SPARK_DRIVER_MEMORY=4g` inside the runner. Compose Spark workers do not mount the host lake path, so cluster mode cannot read local bronze.
- Runner service bypasses the Airflow image entrypoint (`entrypoint: ["/bin/bash","-c"]`, `command: ["sleep infinity"]`) so `docker compose exec` works.
- C02 ignores hive partition column `run_date` (path-only per 4.7).
- Silver write selects `SILVER_COLUMNS` from `processing/schemas.py` so C09 matches 4.10.

## Commands that work now
```
make init && make build && make up

# E2E (local lake) — verified 2026-11-14 as local-demo7
docker compose exec -T -e SPARK_MASTER=local[2] -e SPARK_DRIVER_MEMORY=4g -e SPARK_SHUFFLE_PARTITIONS=2 \
  runner /opt/gamepulse-venv/bin/python -u -m gamepulse.processing.silver_job --run-date 2026-01-05 --run-id local-demo7
docker compose exec -T -e SPARK_MASTER=local[2] -e SPARK_DRIVER_MEMORY=4g -e SPARK_SHUFFLE_PARTITIONS=2 \
  runner /opt/gamepulse-venv/bin/python -u -m gamepulse.processing.gold_job --run-date 2026-01-05 --run-id local-demo7
docker compose exec -T -e MAX_REJECT_RATE=0.10 \
  runner /opt/gamepulse-venv/bin/python -u -m gamepulse.quality.cli --run-date 2026-01-05 --run-id local-demo7

make test && make lint
docker compose --profile dashboard up -d
docker compose --profile airflow up -d
make s3-smoke   # after H2
```

## Measured results
- 2026-11-14 Docker runner: `pytest -m "not integration"` — see latest make test / compose exec output in session.
- local-demo7 quality: status=success; 18 checks, 17 passed, 0 critical rejected; C18 warn failed (22 stats mismatches; expected with corruption / sim noise).
- Bronze 2818 → silver 2781 + quarantine 37; reject rate 0.01313.
- Gold: dim_match=5, fact_match_events=2731, fact_player_match=50.

## Known problems
- Re-ingesting the same Kafka batch without clearing bronze doubles rows and spikes C04 via `duplicate_event_id`. Prefer one ingest per demo date, or delete extra bronze parts before silver.
- Compose Spark workers cannot see `/opt/gamepulse/data` lake; use `SPARK_MASTER=local[2]` for local-path jobs.
- IT-idem-silver / IT-idem-gold (spec 4.18) still unwired as dedicated tests.
- Benchmark result CSVs not measured yet.
- H2 / S3 not run on this host.

## Human steps still pending
1. Optional: `docker compose --profile dashboard up -d` and open http://localhost:8501
2. H2: terraform apply, create access key, put keys + `DATA_ROOT=s3://...` in `.env` only, `make down && make up`, `make s3-smoke`
3. End-to-end against S3; Airflow profile; optional 3-day backfill
4. Run benchmarks, commit result CSVs if desired
5. Commit and push this repo when ready

## Environment notes
- Sample run_date: 2026-01-05
- Region default: us-east-1
- Ports: Airflow 8080, Spark 8082–8084, Kafka host 29092, Streamlit 8501
- No secrets in repo

---

## Task prompt deltas (historical)
See PROJECT_SPEC.md 4.4 / 4.18 — already applied in code (player cap, day clamp, delete-then-write).
