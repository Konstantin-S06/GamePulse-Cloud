# Benchmarks

Benchmarks time Silver and Gold for fixed Spark settings. They answer
questions about shuffle partitions and local vs cluster master on this machine
only.

## Setup limits

Spark in compose is two worker containers on one host. Results do not predict
multi-machine or cloud cluster performance. State that in any write-up.

Adaptive query execution is off so `spark.sql.shuffle.partitions` stays exact.

## Configs

| File | Variants |
|---|---|
| `benchmarks/configs/partition_shuffle.yaml` | shuffle partitions 4, 8, 16 |
| `benchmarks/configs/local_vs_cluster.yaml` | `local[2]` vs `spark://spark-master:7077` |

## Run

From the runner container with Bronze/Silver data for the date (S3 mode +
cluster recommended for the cluster variant):

```bash
python benchmarks/run_benchmark.py \
  --config benchmarks/configs/partition_shuffle.yaml \
  --run-date 2026-01-05 \
  --run-id bench-shuffle-1
```

Output CSV: `benchmarks/results/<benchmark_name>_<run_date>.csv` (committed
locally; not under `tmp`).

## Report

```bash
python benchmarks/report.py
python benchmarks/report.py benchmarks/results/partition_shuffle_2026-01-05.csv
```

## Cleanup

Copy CSVs you need, then delete large prefixes under `benchmarks/` in S3. The
bucket lifecycle rule removes objects under `benchmarks/` after 30 days.