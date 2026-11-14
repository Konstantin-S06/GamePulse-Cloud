# Decisions

Short index of trade-offs from PROJECT_SPEC.md Part 1.3. See the spec for detail.

| ID | Decision |
|---|---|
| D1 | Bronze via Python Kafka consumers, not Spark Streaming |
| D2 | Batch jobs per `run_date`; delete-then-write on retry |
| D3 | At-least-once delivery; Silver dedupes by `event_id` |
| D4 | Kafka key = `match_id` for per-match ordering |
| D5 | Spark: one master, two workers on one machine |
| D6 | Single `DATA_ROOT` for local or S3 |
| D7 | pyarrow S3 only (no boto3/s3fs in pipeline) |
| D8 | Project venv in Airflow image at `/opt/gamepulse-venv` |
| D9 | No Python UDFs in Spark |
| D10 | AQE off for predictable shuffle counts |
| D11 | Metrics as JSON under `metrics/` |
| D12 | Terraform: bucket + IAM user; keys created manually |
| D13 | No EMR, EKS, MSK, or Glue |

## Synthetic data

The simulator produces fictional arena matches. Nothing is real player data or
data from any commercial game title.