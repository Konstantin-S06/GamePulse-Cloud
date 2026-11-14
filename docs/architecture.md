# Architecture

GamePulse Cloud moves synthetic arena events through Kafka, Bronze Parquet,
Spark batch jobs, and Gold tables. DuckDB reads Gold for SQL, quality checks,
and the Streamlit dashboard.

## Data flow

```
simulator -> Kafka -> Bronze (Python consumers)
Bronze -> Spark Silver (+ quarantine)
Silver -> Spark Gold
Gold -> DuckDB views -> dashboard / quality
```

Airflow (optional compose profile) runs one `run_date` per DAG run: preflight,
topics, produce, ingest, DLQ archive, Silver, Gold, quality.

## Components

| Piece | Role |
|---|---|
| Simulator | Seeded JSON events, optional corruption |
| Kafka | Buffered stream; key = match_id (6 partitions) |
| Ingest | Consumer group writes Bronze; bad JSON to DLQ |
| Spark | Validation, dedupe, Gold builds (batch per run_date) |
| S3 or local DATA_ROOT | Bronze, Silver, quarantine, Gold, metrics |
| DuckDB | `read_parquet` views over the lake (no writes) |
| Streamlit | Read-only Gold and metrics |
| Terraform | One bucket, scoped IAM user, budget alert |

## Spark cluster honesty

The compose Spark cluster is one master and two worker containers on a single
host. Executors share that machine's CPU and disk. It shows executor scheduling
and shuffle behaviour in the Spark UI, not multi-machine scaling or WAN costs.
Benchmark conclusions must stay limited to what was measured on this setup.

## Data modes

- **Local:** `DATA_ROOT=/opt/gamepulse/data/lake` for tests and local Spark.
- **S3:** `DATA_ROOT=s3://<bucket>` for real runs and benchmarks on the cluster.

Python uses pyarrow S3; Spark uses `s3a://` via `storage.to_spark_uri`.