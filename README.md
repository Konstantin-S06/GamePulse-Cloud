# GamePulse Cloud

Batch data pipeline for **synthetic** arena match events: Kafka, Parquet lake
layers, Spark validation and Gold aggregates, DuckDB analytics, and a Streamlit
dashboard.

**Disclaimer:** All data is synthetic, produced by the built-in simulator. It is
not real player data and not data from any commercial game.

## Quick start

```bash
make init    # copy .env, set AIRFLOW_UID and FERNET_KEY (Linux/WSL)
make build
make up
```

Run Python and tests inside the runner container:

```bash
make test
make shell
```

Optional profiles:

```bash
docker compose --profile dashboard up -d
docker compose --profile airflow up -d
```

## Architecture (one page)

Events flow: **simulator → Kafka → Bronze → Spark Silver → Spark Gold → DuckDB
views**. Airflow (optional) chains produce, ingest, Silver, Gold, and quality for
one `run_date` per run. Metrics land as JSON under `metrics/`.

The Spark “cluster” is **one master and two workers on a single machine**. It
shows executor scheduling and partitions in the Spark UI; it does not model
multi-host scaling.

See [docs/architecture.md](docs/architecture.md) for diagrams and tables.

## Data location

| Mode | `DATA_ROOT` |
|---|---|
| Local dev / tests | `/opt/gamepulse/data/lake` |
| S3 runs | `s3://<your-bucket>` |

Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` in `.env` for S3.
Never commit `.env`.

## Host ports

| Service | Port |
|---|---|
| Streamlit dashboard | 8501 |
| Airflow UI | 8080 |
| Spark master UI | 8082 |
| Spark worker UIs | 8083, 8084 |
| Kafka (host) | 29092 |

## Dashboard

```bash
docker compose --profile dashboard up -d
```

Open http://localhost:8501. Pages: overview, gameplay, data quality, benchmarks.

## Benchmarks

```bash
python benchmarks/run_benchmark.py --config benchmarks/configs/partition_shuffle.yaml \
  --run-date 2026-01-05 --run-id bench-1
python benchmarks/report.py
```

See [docs/benchmarks.md](docs/benchmarks.md).

## Docs

- [architecture.md](docs/architecture.md)
- [data_dictionary.md](docs/data_dictionary.md)
- [quality_rules.md](docs/quality_rules.md)
- [benchmarks.md](docs/benchmarks.md)
- [decisions.md](docs/decisions.md)
- [cost_and_cleanup.md](docs/cost_and_cleanup.md)
- [simulator.md](docs/simulator.md)

## Cleanup

1. Revoke the IAM access key for `gamepulse-pipeline`.
2. `terraform destroy`
3. `docker compose down -v`
4. Delete local `data/` and remove secrets from `.env`

Details: [docs/cost_and_cleanup.md](docs/cost_and_cleanup.md).

## Requirements

Docker Compose v2, 12 GB RAM for Docker (8 GB possible with reduced Spark memory),
WSL2 on Windows. No host Python or Java required.