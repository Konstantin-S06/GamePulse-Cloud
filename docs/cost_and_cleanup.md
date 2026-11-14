# Cost and cleanup

## Expected cost

- S3 Standard: a few cents per GB-month in us-east-1 (check current pricing).
- This project stays in the low GB range if you delete benchmark and smoke prefixes.
- Egress from S3 to the internet is billable after the free tier; prefer querying
  Gold from DuckDB and the dashboard instead of downloading Bronze.

Terraform sets a $10/month budget with alerts at 50%, 80%, and 100%.

## During the project

- Put credentials only in `.env` (gitignored).
- Run Spark benchmarks with `DATA_ROOT` on S3 when using the cluster master.
- After saving benchmark CSVs locally, delete `benchmarks/` objects in the bucket.

## Shutdown

1. `aws iam delete-access-key --user-name gamepulse-pipeline --access-key-id <KEY_ID>`
2. `terraform destroy` (bucket uses `force_destroy`)
3. `docker compose --profile dashboard --profile airflow down -v`
4. Remove `data/` and clear AWS keys from `.env`

Record measured `dir_size_bytes` results here when you run them:

| Prefix | Bytes | Date |
|---|---|---|
| (fill after measurement) | | |