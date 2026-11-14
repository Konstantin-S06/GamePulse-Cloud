# GamePulse Cloud — Project Specification

Parts 1 to 4 are the single source of truth for architecture, layout, setup, and behaviour.
Task prompts refer to this file by section number. Do not change names, columns, constants,
file locations, or numbers unless this document is updated first.

---

## Part 1. Architecture

### 1.1 Data flow

```
simulator (Python, seeded)
   |  JSON strings, message key = match_id
   v
Kafka topic gamepulse.events.v1 (6 partitions) --- unparseable messages ---> gamepulse.events.v1.dlq
   |  consumer group gamepulse-ingest (3 consumer processes)
   v
S3 bronze/events/run_date=D/                 raw JSON + Kafka metadata, Parquet
   |  Spark job: silver (validate, dedupe, standardize)
   v
S3 silver/events/event_date=D/event_type=T/      S3 quarantine/events/run_date=D/
   |  Spark job: gold (dimensions, facts, aggregates)
   v
S3 gold/<table>/
   |  DuckDB views + SQL files
   v
quality checks, analytics queries, Streamlit dashboard
```

Airflow runs these stages in order for one run date D.
Every stage writes a metrics JSON file to S3 under metrics/.

### 1.2 Responsibilities

| Component | Does | Does not do |
|---|---|---|
| Simulator | Produces events as Python dicts, optionally corrupted on purpose | Know about Kafka or S3 |
| Kafka | Partitioned, durable buffer between producer and consumers | Validate content |
| Ingest consumers | Read Kafka in a consumer group, write raw rows to Bronze, send unparseable messages to the dead letter topic | Business validation |
| Spark | All validation, dedupe, typing, joins, aggregation. Reads Bronze, writes Silver, Quarantine, Gold | Talk to Kafka |
| S3 | Stores every layer, the quarantine and the metrics | |
| DuckDB | Queries Gold, runs quality checks, feeds the dashboard | Write data |
| Airflow | Runs the stages in order for one run date, retries, backfills | Move or transform data itself |
| Streamlit | Shows Gold data, metrics, benchmark results | Write data |

### 1.3 Decisions and trade-offs

- **D1.** Bronze is written by Python consumers, not by Spark Structured Streaming. The brief allows an intermediate ingestion layer. Python consumers make consumer groups and parallel consumption easy to show and test, and Spark stays a plain batch job that is easy to rerun. Trade-off: Spark does not read Kafka directly. The README says so.
- **D2.** Spark jobs are batch jobs keyed by `run_date`. A rerun or backfill overwrites exactly one date partition, so every stage can be repeated safely. Before writing, Silver and Gold delete that date's output prefixes (see 4.9, 4.11, 4.18).
- **D3.** Delivery is at-least-once. A consumer commits offsets only after its Bronze file is written. Duplicates can happen (Kafka replay, producer rerun, consumer retry) and are removed in Silver by `event_id`. Idempotent final state means: after any failed stage is retried to success for date D, Silver has unique `event_id`s for D and Gold aggregates for D match a clean single successful run.
- **D4.** One topic, message key = `match_id`. All events of one match land in one partition, so their order is kept. Trade-off: partition load follows match size.
- **D5.** Spark runs as a standalone cluster in Docker: one master and two workers on one machine. That is multi-worker, not multi-machine. Local mode (`local[N]`) is used for unit tests and as one benchmark variant.
- **D6.** The data location is one variable, `DATA_ROOT`. A local path is used for tests and quick work. An `s3://` URL is used for real runs. Python uses pyarrow filesystems, Spark uses `s3a://`.
- **D7.** Python code uses pyarrow's built-in S3 filesystem. No s3fs and no boto3 in the pipeline, so there are no version conflicts.
- **D8.** Project Python code runs in its own virtualenv inside the Airflow image at `/opt/gamepulse-venv`. Airflow's own packages stay untouched. DAG tasks are BashOperator calls into that venv.
- **D9.** No Python UDFs in Spark. Built-in functions only, so workers need no Python and jobs stay fast.
- **D10.** Adaptive query execution is turned off, so the shuffle partition count is the number you set. The partition benchmark needs this.
- **D11.** Metrics are small JSON files in S3 under `metrics/`. No extra database. The dashboard reads them.
- **D12.** Terraform creates the bucket, a scoped IAM user with its policy, and a budget alert. Access keys are created by hand with the AWS CLI, so no secret ends up in Terraform state.
- **D13.** No EMR, EKS, MSK or Glue. They add cost and setup time and the project goals do not need them.

### 1.4 What distributed means here

The Spark cluster is two worker containers on one machine, sharing its CPU and disk. It shows how Spark schedules tasks over executors and how partitioning behaves, and the Spark UI shows it. It does not show scaling across machines or real network cost. The README and every benchmark write-up must say this in plain words and must only draw conclusions from what was measured on this setup.

### 1.5 Changes from the original brief

The technology list is the same as the brief. Added: PostgreSQL (Airflow metadata only), pyarrow (Parquet and S3 access from Python), PyYAML (benchmark config files). Nothing was removed.

---

## Part 2. Repository structure

```
gamepulse-cloud/
  README.md
  PROJECT_SPEC.md              Parts 1 to 4 of this plan
  PROJECT_HANDOFF.md           progress log and decisions
  Makefile                     every common command
  pyproject.toml               ruff and pytest settings only
  docker-compose.yml
  .env.example                 copy to .env, never commit .env
  .gitignore
  .gitattributes               forces LF line endings
  .cursor/rules/gamepulse.mdc  rules Cursor must follow
  .github/workflows/ci.yml
  requirements/                base.txt, dev.txt, dashboard.txt
  docker/
    airflow/Dockerfile         image for runner, airflow-* services
    spark/Dockerfile           image for spark-master and spark workers
    dashboard/Dockerfile
  airflow/dags/gamepulse_daily.py
  terraform/                   versions.tf variables.tf main.tf outputs.tf terraform.tfvars.example README.md
  sql/
    views.sql                  DuckDB view definitions
    analytics/                 01_*.sql to 08_*.sql
  src/gamepulse/
    __init__.py                empty
    config.py                  Settings from environment variables
    reference_data.py          characters, items, maps, event specs
    common/                    logging_setup.py storage.py metrics.py
    simulator/                 sim_config.py players.py match.py generator.py corruption.py cli.py
    ingestion/                 topics.py admin.py producer.py produce_cli.py bronze_writer.py
                               consumer.py ingest_cli.py dlq_archive.py
    processing/                session.py schemas.py rules.py silver_job.py gold_job.py
    quality/                   checks.py cli.py
    analytics/                 duck.py run_query.py
    ops/                       preflight.py s3_smoke_test.py run_pipeline.py
  dashboard/                   app.py, pages/, lib/data.py
  benchmarks/                  configs/*.yaml, run_benchmark.py, report.py, results/
  tests/                       conftest.py, unit/, spark/, integration/
  docs/                        architecture.md, data_dictionary.md, quality_rules.md,
                               benchmarks.md, decisions.md, cost_and_cleanup.md, simulator.md, images/
  data/                        local lake and scratch files, gitignored
```

Rules for the layout: one responsibility per file, no deeper nesting than shown,
`gamepulse/__init__.py` stays empty so importing a submodule never pulls in Spark or Kafka by accident.

---

## Part 3. Setup plan

### 3.1 Host requirements

Docker with Compose v2 and at least 12 GB of RAM given to Docker (if you only have 8 GB, set worker memory to 1g in compose and use `SPARK_EXECUTOR_MEMORY=512m`). 30 GB free disk. macOS, Linux, or Windows with WSL2 (run everything inside WSL2). git, make, Terraform 1.6 or newer, AWS CLI v2, an AWS account, a GitHub repository. You do not need Python or Java on the host.

### 3.2 Pinned versions

| Item | Version | Notes |
|---|---|---|
| Python | 3.11 | inside the images |
| Java | 17 (openjdk-17-jre-headless) | Spark image and Airflow image |
| Apache Spark | 3.5.3 (spark-3.5.3-bin-hadoop3), pyspark 3.5.3 | cluster and driver must match exactly |
| hadoop-aws / aws-java-sdk-bundle | 3.3.4 / 1.12.262 | jar files copied into both Spark installs |
| Kafka | apache/kafka:3.8.0 | KRaft mode, one broker, no ZooKeeper |
| confluent-kafka | 2.5.3 | |
| Airflow | apache/airflow:2.10.5-python3.11 | LocalExecutor |
| PostgreSQL | postgres:15 | Airflow metadata only |
| pyarrow / numpy / pandas | 16.1.0 / 1.26.4 / 2.2.3 | numpy must stay on 1.x |
| duckdb | 1.1.3 | |
| PyYAML | 6.0.2 | |
| pytest / ruff | 8.3.3 / 0.6.9 | |
| streamlit / plotly | 1.39.0 / 5.24.1 | dashboard image only |
| Terraform / AWS provider | 1.6 or newer / ~> 5.70 | |

These are slightly behind the newest releases on purpose. Do not upgrade anything during the project.

`requirements/base.txt` holds pyspark, confluent-kafka, pyarrow, numpy, pandas, duckdb, PyYAML at the versions above. `requirements/dev.txt` is `-r base.txt` plus pytest and ruff. `requirements/dashboard.txt` is streamlit, plotly, duckdb, pandas, pyarrow, numpy, PyYAML.

### 3.3 How the environment works

All project code runs inside the runner container. It is built from `docker/airflow/Dockerfile`, which contains Java, the venv `/opt/gamepulse-venv` with the base and dev requirements, and the S3 jars inside the venv's pyspark jars folder. The repository is mounted at `/opt/gamepulse`, so edits on the host apply immediately. `PYTHONPATH=/opt/gamepulse/src` is set in the image. Makefile targets wrap `docker compose exec -T runner /opt/gamepulse-venv/bin/python ...`.

Services: `kafka`, `spark-master`, `spark-worker-1`, `spark-worker-2`, `runner` start by default. `postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler` are in compose profile `airflow`. `dashboard` is in profile `dashboard`. This keeps RAM use down while you work on earlier phases.

| What | Host port |
|---|---|
| Airflow UI | 8080 |
| Spark master UI | 8082 (container 8080) |
| Spark worker UIs | 8083, 8084 (container 8081) |
| Kafka from the host | 29092 (inside compose it is `kafka:9092`) |
| Streamlit | 8501 |

### 3.4 Two data modes

- **Local mode:** `DATA_ROOT=/opt/gamepulse/data/lake`. For unit tests, development, and Spark `local[N]`.
- **S3 mode:** `DATA_ROOT=s3://<your-bucket>`. For every real run, all benchmarks, the Airflow demo and the screenshots. Use the Spark cluster (`spark://`) with S3 mode only. With a local root on Linux the workers create root-owned files in your data folder.

### 3.5 AWS resources (Terraform creates exactly these)

- One S3 bucket `gamepulse-<suffix>` with public access blocked, SSE-S3 encryption, and a lifecycle rule that deletes objects under `benchmarks/` and `smoke/` after 30 days and aborts incomplete multipart uploads after 7 days.
- One IAM user `gamepulse-pipeline` with one inline policy scoped **only** to that bucket:
  - `s3:ListBucket` and `s3:GetBucketLocation` on the bucket ARN
  - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `bucket/*`
  - No other AWS services, no `*` resource ARNs outside this bucket
- One AWS budget of 10 USD per month with email alerts at 50, 80 and 100 percent.

Nothing else. No EC2, no EMR, no NAT gateway, no managed Kafka. No access key is created by Terraform.

**Credential handling (required):**

1. Create the access key yourself: `aws iam create-access-key --user-name gamepulse-pipeline`.
2. Put `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and `DATA_ROOT=s3://<bucket_name>` only in `.env` (gitignored). Never commit keys, never paste them into chat, docs, logs, or screenshots.
3. Do not put `AWS_SESSION_TOKEN` in `.env` (unused; an empty value can break clients).
4. Prefer temporary keys when your account supports them; otherwise rotate the IAM user key if it may have been exposed.
5. On cleanup, revoke before destroy finishes: `aws iam delete-access-key --user-name gamepulse-pipeline --access-key-id <KEY_ID>`, then `terraform destroy`, then delete local `.env` keys.

### 3.6 Cost control and cleanup

- S3 Standard storage costs a few cents per GB-month in us-east-1 (check current pricing). Total data for this project should stay in the low GB range, so expect cents. Measure with the directory size helper instead of guessing.
- Data going into S3 is free. Data leaving S3 to the internet is charged after the free monthly allowance, so run Spark and the dashboard against Gold only for dashboard use and do not download Bronze.
- Delete `benchmarks/` prefixes after you save the results. The lifecycle rule is the backup.
- Cleanup when finished:
  1. Delete the IAM access key (`aws iam delete-access-key ...`) so credentials stop working immediately.
  2. `terraform destroy` (the bucket has `force_destroy = true`).
  3. `docker compose down -v`.
  4. Delete `data/` and clear secrets from `.env`.
- `docs/cost_and_cleanup.md` records these steps and the real size numbers you measured.

### 3.7 Known pitfalls (Cursor must read this)

1. Spark runs in client mode from the runner or the Airflow scheduler container. Executors must be able to reach the driver, so the session builder sets `spark.driver.host` from `SPARK_DRIVER_HOST` (the compose service name of the container that runs the job) and fixed ports 40000 and 40001.
2. No Python UDFs, no pandas_udf, no `rdd.map` with Python lambdas.
3. Spark null logic: a rule condition that evaluates to null silently passes the row. Every validity check must be wrapped as shown in Spec 4.9 so that unknown means invalid.
4. Never read from and overwrite the same path in one Spark job.
5. `spark.sql.legacy.timeParserPolicy` must be `CORRECTED`, otherwise bad timestamps raise exceptions instead of becoming null.
6. Never call `collect()` or `toPandas()` on event-sized data. Counts come from aggregations.
7. Dynamic partition overwrite is required (`spark.sql.sources.partitionOverwriteMode=dynamic`).
8. numpy stays at 1.26.4, otherwise pyarrow 16.1.0 fails at import.
9. `.gitattributes` has `* text=auto eol=lf` so Makefile and shell lines do not break on Windows checkouts.
10. Do not put empty `AWS_SESSION_TOKEN` in `.env`. The variable is not used.

---

## Part 4. Specification

This part is the single source of truth. Task prompts refer to it by section number.

### 4.1 Names and formats

- **run_date:** string `YYYY-MM-DD`, UTC. One pipeline run handles one `run_date`.
- **match_id:** `m_` + `YYYYMMDD` + `_` + 5 digit index, for example `m_20260105_00012`. Regex `^m_[0-9]{8}_[0-9]{5}$`.
- **player_id:** `p_` + 6 digits, for example `p_000123`. Regex `^p_[0-9]{6}$`. Valid indices are `0` through `999999` inclusive, so at most 1_000_000 players (`p_000000` … `p_999999`).
- **event_id:** `<match_id>-<6 digit seq>`, for example `m_20260105_00012-000017`. Seq starts at 1 for every match, assigned after the match events are sorted.
- **character_id:** `c01` to `c12`. **item_id:** `i01` to `i12`.
- **Timestamps:** ISO 8601 UTC with milliseconds and a trailing `Z`, for example `2026-01-05T14:03:22.123Z`. Spark parse pattern: `yyyy-MM-dd'T'HH:mm:ss.SSSX`.
- **Kafka topics:** events `gamepulse.events.v1`. Dead letter topic is the events topic name plus `.dlq`.
- **Lake prefixes** under `DATA_ROOT`: `bronze/events/`, `silver/events/`, `quarantine/events/`, `quarantine/ingestion/`, `gold/<table>/`, `metrics/stage=<stage>/run_date=<D>/<run_id>.json`.
- **run_id:** any string, sanitized by replacing every character outside `A-Z a-z 0-9 _ . -` with `_`. Airflow passes its own run id.

### 4.2 Reference data (`src/gamepulse/reference_data.py`)

This file holds constants only. It is the one place these values are defined. Simulator, Spark jobs, quality checks and the dashboard all import it.

**Characters** (`CHARACTERS`, list of dicts with `character_id`, `name`, `role`):

| id | name | role | id | name | role |
|---|---|---|---|---|---|
| c01 | Ironclad | tank | c07 | Talon | damage |
| c02 | Bulwark | tank | c08 | Ranger | damage |
| c03 | Vanguard | tank | c09 | Frost | damage |
| c04 | Ember | damage | c10 | Warden | support |
| c05 | Volt | damage | c11 | Lumen | support |
| c06 | Shade | damage | c12 | Pulse | support |

`ROLE_DAMAGE_MULT = {tank: 0.7, damage: 1.3, support: 0.6}`.

**Items** (`ITEMS`, list of dicts with `item_id`, `name`, `category`, `cost`):

| id | name | category | cost | id | name | category | cost |
|---|---|---|---|---|---|---|---|
| i01 | Iron Plate | defense | 300 | i07 | Aegis Shield | defense | 700 |
| i02 | Blast Core | offense | 450 | i08 | Focus Lens | utility | 350 |
| i03 | Swift Boots | utility | 250 | i09 | Titan Heart | defense | 900 |
| i04 | Health Flask | consumable | 100 | i10 | Storm Gauntlet | offense | 1000 |
| i05 | Energy Cell | consumable | 120 | i11 | Scout Beacon | utility | 150 |
| i06 | Razor Edge | offense | 600 | i12 | Vital Charm | defense | 400 |

Other constants: `MAPS = [Obsidian Yard, Sunken Foundry, Glass Canyon]`, `REGIONS = [na, eu, apac]` with weights `[0.40, 0.35, 0.25]` (`REGION_WEIGHTS`), `GAME_MODE = arena_5v5`, `TEAMS = [blue, red]`, `DAMAGE_SOURCES = [basic_attack, ability]`, `LEAVE_REASONS = [match_end, disconnect]`, `EVENT_VERSION = 1`.

Numeric ranges (`RANGES`, inclusive): skill_rating 400 to 3000, cost 1 to 5000, amount 1 to 1000, duration_seconds 60 to 7200, blue_score and red_score 0 to 500, kills, deaths, assists 0 to 500, damage_dealt and damage_received 0 to 5000000, gold_spent 0 to 100000.

### 4.3 Event envelope and payloads

Every message is one compact JSON object (`json.dumps(obj, separators=(',', ':'), sort_keys=True)`) with these keys: `event_id`, `event_type`, `event_version` (always 1), `event_ts`, `match_id`, `player_id` (null for `match_start` and `match_end`), `payload` (object).

`EVENT_SPECS` in `reference_data.py` is a dict keyed by event type. Each value has `requires_player` (bool) and `payload` (dict of field name to type, with types `str`, `int`, `bool`). All payload fields listed are required.

| event_type | requires_player | payload fields |
|---|---|---|
| match_start | no | map_name str, game_mode str, region str |
| player_join | yes | team str, skill_rating int |
| character_select | yes | character_id str, team str |
| item_purchase | yes | item_id str, cost int |
| damage | yes (attacker) | target_player_id str, amount int, damage_source str |
| elimination | yes (killer) | victim_player_id str |
| assist | yes (assister) | victim_player_id str |
| player_leave | yes | reason str |
| match_end | no | winning_team str, duration_seconds int, blue_score int, red_score int |
| player_stats | yes | character_id str, team str, kills int, deaths int, assists int, damage_dealt int, damage_received int, gold_spent int, won bool |

Damage received by a player is not its own event. It is read from `target_player_id` of damage events.

### 4.4 Simulator algorithm (`src/gamepulse/simulator/`)

**SimConfig** (dataclass in `sim_config.py`): `run_date: str` (required), `seed: int = 42`, `num_matches: int = 100`, `num_players: int = 2000`.

Validation (raise `ValueError` otherwise):

- `200 <= num_players <= 1_000_000` (six-digit `player_id` capacity: `p_000000` … `p_999999`)
- `1 <= num_matches <= 82800` (five-digit match index capacity and day schedule window)

**Randomness:** only `random.Random` instances. Never use the global `random` module, set iteration order, or `hash()`. Iterate lists in a fixed order.

**Player pool** (`players.py`, function `build_player_pool(config) -> list[Player]`): `rng = random.Random(f'{seed}-players')`. For `i` in `0..num_players-1`: `player_id = f'p_{i:06d}'`, `region = rng.choices(REGIONS, weights)[0]`, `skill = int(min(2400, max(600, rng.gauss(1500, 300))))`, `preferred = rng.sample(character_ids, 3)`. If any region has fewer than 10 players raise `ValueError`. `Player` is a frozen dataclass with those four fields. The pool depends on seed only, so the same players appear on every day.

**Match generation** (`match.py`, function `generate_match(config, pool, match_index) -> list[dict]`). Use `rng = random.Random(f'{seed}-{run_date}-{match_index}')`.

Constants for the day schedule (define in `match.py` or as module-level constants used only by the generator):

- `DAY_SECONDS = 86400`
- `MAX_DURATION_SECONDS = 1800`
- `MAX_LEAVE_AFTER_END_SECONDS = 20` (player_leave with reason `match_end` uses `end_ts + randint(2, 20)`)
- `MAX_MATCH_SPAN_SECONDS = MAX_DURATION_SECONDS + MAX_LEAVE_AFTER_END_SECONDS` (= 1820)
- `SCHEDULE_WINDOW_SECONDS = DAY_SECONDS - MAX_MATCH_SPAN_SECONDS` (= 84580)

Steps in this order:

1. `interval = SCHEDULE_WINDOW_SECONDS // num_matches`. `day_start_utc` is the UTC midnight of `run_date`. `day_end_utc = day_start_utc + timedelta(days=1)`. `start_ts = day_start_utc + timedelta(seconds=match_index * interval + rng.randint(0, min(interval, 60) - 1))`.
2. `region = rng.choices(REGIONS, weights)[0]`, `map_name = rng.choice(MAPS)`. Players: `rng.sample(players_in_region, 10)` where `players_in_region` is the pool list filtered by region in pool order. First 5 are blue, last 5 are red.
3. For each of the 10 players in list order: `join_ts = start_ts + rng.randint(1, 20)` seconds. `select_ts = join_ts + rng.randint(1, 10)` seconds. Character: with probability 0.7 (`rng.random() < 0.7`) pick `rng.choice(player.preferred)`, otherwise pick `rng.choice(all character ids)`. If that character is already taken by a teammate, pick `rng.choice` from the character ids the team has not taken.
4. `duration = int(min(1800, max(360, rng.gauss(900, 240))))` seconds. `end_ts = start_ts + timedelta(seconds=duration)`. **Same-day guarantee:** after `end_ts` is computed, if `end_ts + timedelta(seconds=MAX_LEAVE_AFTER_END_SECONDS) >= day_end_utc`, set `end_ts = day_end_utc - timedelta(seconds=MAX_LEAVE_AFTER_END_SECONDS + 1)` and `start_ts = end_ts - timedelta(seconds=duration)`. This keeps every generated timestamp (including leave at `end_ts+20` and stats at `end_ts+1`) strictly inside `run_date` UTC. Gameplay window is `[start_ts + 60 s, end_ts - 60 s]`.
5. Winner: `p_blue = 1 / (1 + 10 ** ((red_avg_skill - blue_avg_skill) / 400))`. `winner = blue` if `rng.random() < p_blue` else `red`.
6. `K = max(12, int(duration / 60 * rng.uniform(2.0, 3.5) + 0.5))`. `share = rng.uniform(0.55, 0.75)`. `winner_kills = int(K * share + 0.5)`. `loser_kills = K - winner_kills`.
7. Eliminations, winning team first then losing team. For each elimination: killer is `rng.choices(team_players, weights=[skill * ROLE_DAMAGE_MULT[role] for each])[0]`. Victim is `rng.choice(other_team_players)`. `elim_ts` is a random time in the gameplay window, truncated to milliseconds. `n_assist = rng.choice([0, 1, 1, 2])`, assisters are `rng.sample(killer_teammates, n_assist)`. Events produced: `n_hits = rng.randint(3, 6)` damage events from the killer to the victim at `elim_ts - j * 400 ms` for `j = 1..n_hits`, one damage event per assister at `elim_ts - 200 ms`, one elimination event at `elim_ts`, one assist event per assister at `elim_ts`. Damage amount = `max(1, int(rng.uniform(30, 120) * ROLE_DAMAGE_MULT[attacker_role] + 0.5))` and `damage_source = rng.choice(DAMAGE_SOURCES)`.
8. Pokes: `duration // 12` extra damage events. Attacker is `rng.choice(all 10)`, target is `rng.choice(opposing team)`, time random in the gameplay window, amount = `max(1, int(rng.uniform(10, 60) * mult + 0.5))`.
9. Purchases: for each player `n = rng.randint(3, 8)` events. `item = rng.choices(ITEMS, weights=[1 / cost])[0]`, time random in `[start_ts + 30 s, end_ts - 60 s]`, cost equals the item cost.
10. Leaves: `rng.random() < 0.05` means disconnect: `leave_ts = end_ts - rng.randint(5, 59)` seconds, reason `disconnect`. Otherwise `leave_ts = end_ts + rng.randint(2, 20)` seconds, reason `match_end`.
11. `match_start` at `start_ts`. `match_end` at `end_ts` with `winning_team`, `duration_seconds = duration`, `blue_score` and `red_score` equal to the number of eliminations by each team. `player_stats` for each player at `end_ts + 1 s`.
12. Stats: kills = eliminations as killer, deaths = eliminations as victim, assists = assist events, damage_dealt = sum of damage amounts as attacker, damage_received = sum of damage amounts as target, gold_spent = sum of purchase costs, won = team equals winner. Counts come from the generated events, so stats always match the event stream exactly.
13. Sort all events of the match by `(timestamp, generation index)`. Assign `event_id` with seq starting at 1 in that order. Format timestamps as in 4.1.

**Invariant (must hold for every generated match):** every event's `event_ts` parses to a UTC datetime whose calendar date equals `run_date`. Tests must assert this; the clamp in step 4 is the mechanism that enforces it when a late start would otherwise spill past midnight.

`generator.py`: `generate_events(config) -> Iterator[dict]` builds the pool once, then yields all events of match 0, then match 1, and so on. A match has roughly 400 to 600 events. Do not hard-code this number anywhere. Benchmarks use the real counts.

Limitations to write in `docs/simulator.md`: all data is synthetic and is not real player data, player skill never changes, a player can appear in two overlapping matches, combat is statistical with no map or positions, the data does not come from any real game.

### 4.5 Deliberate corruption (`corruption.py`)

Function `corrupt_stream(events, rate, seed, all_types=False) -> Iterator[tuple[str, str]]` yields `(key, value)`. `key` is the original `match_id`. `value` is the JSON string. It also exposes a `CorruptionReport` object with a `counts` dict (name to int) and a `total_messages` int. With `rate = 0.0` every value is the plain JSON of the event.

Procedure: `rng = random.Random(f'{seed}-corrupt')`. For every event draw `rng.random()` once. The event is corrupted only if the draw is `< rate` and the event type is in the corruptible set. Default corruptible set: `damage`, `item_purchase`, `player_leave`. With `all_types=True` every type is corruptible. The defaults keep other events from depending on corrupted ones, so expected counts are exact. Choose the corruption with `rng.choice(applicable)`, where `applicable` is the list below in this order, filtered by the conditions.

| name | effect | where it ends up |
|---|---|---|
| drop_required_field | remove one of event_id, event_type, event_ts, match_id (chosen by `rng.choice` in that order) | quarantine: missing_required_field |
| bad_timestamp | event_ts set to not-a-timestamp | quarantine: invalid_timestamp |
| future_timestamp | year in event_ts increased by 1 | quarantine: timestamp_out_of_range |
| unknown_event_type | event_type set to player_teleport | quarantine: unknown_event_type |
| duplicate_event | the event is emitted twice, unchanged | second copy: duplicate_event_id |
| invalid_json | the JSON string is cut to half its length | ingestion dead letter topic |
| orphan_match | match_id set to `m_<YYYYMMDD of run date>_99999` | quarantine: orphan_match |
| invalid_numeric | only for damage (amount = -50), item_purchase (cost = -1), player_stats (kills = -1), match_end (duration_seconds = 0) | quarantine: invalid_numeric_value |
| invalid_player_ref | only if player_id is not null: set to p_999999 | quarantine: unknown_player_in_match |
| invalid_reference_value | only for character_select and player_stats (character_id = c99) and item_purchase (item_id = i99) | quarantine: invalid_reference_value |

Expected counts for a run with the default corruptible types, used by tests: rows in Bronze = total messages minus the `invalid_json` count. Rows in Silver quarantine = total corruptions minus the `invalid_json` count. Dead letter messages = the `invalid_json` count.

### 4.6 Kafka (`src/gamepulse/ingestion/`)

Topics: `gamepulse.events.v1` with 6 partitions by default, replication factor 1. The dead letter topic has 1 partition. `admin.py` provides `create_topic(bootstrap, name, partitions)` (ignore already-exists, log a warning if the existing partition count differs), `delete_topic`, `list_topics`, and a CLI with subcommands `create`, `delete`, `list`.

**Producer settings (exact):** `acks=all`, `enable.idempotence=true`, `compression.type=lz4`, `linger.ms=20`, `batch.num.messages=10000`, `queue.buffering.max.messages=200000`, `delivery.timeout.ms=60000`. Key and value are UTF-8 bytes.

Producer loop: call produce per message with a delivery callback that counts delivered and failed. On `BufferError`: `producer.poll(0.5)` and retry the same message, at most 20 times, then count it as failed. Call `producer.poll(0)` every 1000 messages. At the end `producer.flush(60)`; anything still queued counts as failed. With `--rate N` above zero, after every 1000 messages sleep until elapsed >= sent / N. `produce_cli.py` arguments: `--run-date` (required), `--num-matches` 100, `--num-players` 2000, `--seed` 42, `--topic` (env default), `--bootstrap` (env default), `--rate` 0, `--corrupt-rate` 0.0, `--corrupt-all-types` (flag), `--run-id` (default: `local-<UTC timestamp>`). It writes stage metrics (produce) and exits with code 1 if any message failed.

**Consumer settings (exact):** `enable.auto.commit=false`, `auto.offset.reset=earliest`, `session.timeout.ms=30000`, `max.poll.interval.ms=300000`.

Consumer loop (`consumer.py`, one consumer per process; the class takes a consumer object, a dead letter producer and a writer function so tests can inject fakes):

1. `msg = consumer.poll(1.0)`. If None, check the idle timeout and the flush timer. If the message has an error: ignore partition-EOF, otherwise log and raise.
2. Decode as UTF-8 and `json.loads`. If decoding or parsing fails, or the result is not a dict, send the original bytes to the dead letter topic with headers `error_reason`, `original_topic`, `original_partition`, `original_offset`, and count it. Otherwise add a row to the buffer. If the message timestamp type is not available, use the ingest time as `kafka_timestamp`.
3. Flush when the buffer reaches `batch_size` (10000), or when `flush_seconds` (5) have passed and the buffer is not empty. Flushing writes one Bronze file, retrying up to 3 times with waits of 1, 2 and 4 seconds, and raises if all attempts fail (offsets stay uncommitted). After a successful write: `dlq_producer.flush()` then `consumer.commit(asynchronous=False)`.
4. `on_revoke` callback: flush the buffer and commit, so a rebalance does not lose or repeat work.
5. Exit after `idle_timeout_seconds` (15) pass without receiving a message. Flush the buffer, commit, `consumer.close()`.
6. Per consumer stats: `consumer_id`, `messages_consumed`, `rows_written`, `dlq_count`, `batches`, `latency_ms_avg` and `latency_ms_max` (ingested_at minus Kafka message timestamp).

`ingest_cli.py` arguments: `--run-date`, `--run-id`, `--num-consumers` 3, `--group-id` gamepulse-ingest, `--topic`, `--batch-size` 10000, `--flush-seconds` 5, `--idle-timeout-seconds` 15. It starts the consumers with `multiprocessing.get_context('spawn')`, collects their stats through a queue, writes stage metrics (ingest), and exits 1 if any process exits non-zero. `dlq_archive.py` is a single consumer on the dead letter topic (group `gamepulse-dlq-archiver`) writing to `quarantine/ingestion/` as in 4.7, with metrics stage `dlq_archive`.

### 4.7 Bronze and ingestion quarantine

Bronze file path: `bronze/events/run_date=D/part-<consumer_id>-<batch_seq:05d>-<batch_id>.parquet`. `batch_id` is the first 12 hex characters of a uuid4. Compression snappy. The `run_date` value lives in the path only and is not a column.

| Bronze column | Type |
|---|---|
| raw_json | string |
| kafka_topic | string |
| kafka_partition | int32 |
| kafka_offset | int64 |
| kafka_timestamp | timestamp, ms, UTC |
| ingested_at | timestamp, ms, UTC |
| consumer_id | string |
| batch_id | string |

Ingestion quarantine path: `quarantine/ingestion/run_date=D/part-<uuid8>.parquet`, columns `raw_value` string (decoded with `errors=replace`), `error_reason` string, `original_topic` string, `original_partition` int32, `original_offset` int64, `quarantined_at` timestamp ms UTC.

### 4.8 Common modules and Spark session

`src/gamepulse/config.py`: dataclass `Settings` and function `get_settings()` that reads environment variables on every call (no caching, so tests can change them). Variables and defaults are in 4.16. Settings fields are the lowercase names of the variables listed in the Settings column of 4.16 (AWS keys are read from the environment by DuckDB/Spark helpers when needed, not stored on `Settings`).

`src/gamepulse/common/storage.py` (pyarrow filesystems, no s3fs, no boto3). All functions take a full location string that is either a local path or an `s3://bucket/key` URL.

- `is_s3(uri) -> bool`.
- `join_uri(root, *parts) -> str`. Joins with `/`, strips trailing slashes from root.
- `open_fs(uri) -> tuple[FileSystem, str]`. For `s3://` returns `pyarrow.fs.S3FileSystem(region=AWS_REGION)` and the path without the scheme. Otherwise returns `LocalFileSystem()` and the path.
- `write_parquet(table, uri)`: snappy. Creates parent directories only for local paths.
- `read_parquet(uri) -> pyarrow.Table`.
- `write_json(obj, uri)`, `read_json(uri)`.
- `exists(uri) -> bool`, `list_files(prefix_uri) -> list[str]` (full URIs, recursive, empty list if missing).
- `delete_prefix(prefix_uri)`: removes everything under the prefix, does nothing if missing.
- `dir_size_bytes(prefix_uri) -> int`.
- `to_spark_uri(uri) -> str`: `s3://` becomes `s3a://`, a local path stays as it is.

`src/gamepulse/common/logging_setup.py`: `get_logger(name)` with format `%(asctime)s %(levelname)s %(name)s %(message)s` to stdout, level from `LOG_LEVEL` (default INFO).

`src/gamepulse/processing/session.py`: `build_spark(app_name) -> SparkSession` and `build_local_spark() -> SparkSession` (for tests: master `local[2]`, shuffle partitions 2, `spark.ui.enabled=false`). Settings applied by `build_spark`:

| Setting | Value |
|---|---|
| master | SPARK_MASTER (default local[2]) |
| spark.sql.session.timeZone | UTC |
| spark.sql.legacy.timeParserPolicy | CORRECTED |
| spark.sql.shuffle.partitions | SPARK_SHUFFLE_PARTITIONS (default 8) |
| spark.sql.adaptive.enabled | false |
| spark.sql.sources.partitionOverwriteMode | dynamic |
| spark.sql.parquet.compression.codec | snappy |
| spark.ui.showConsoleProgress | false |
| spark.driver.memory | SPARK_DRIVER_MEMORY (default 1g) |
| spark.hadoop.fs.s3a.impl | org.apache.hadoop.fs.s3a.S3AFileSystem |
| spark.hadoop.fs.s3a.aws.credentials.provider | com.amazonaws.auth.EnvironmentVariableCredentialsProvider |
| spark.hadoop.fs.s3a.endpoint | s3.AWS_REGION.amazonaws.com |
| spark.hadoop.fs.s3a.fast.upload | true |
| spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version | 2 |

Only when the master starts with `spark://` also set: `spark.executor.memory` = SPARK_EXECUTOR_MEMORY (default 1g), `spark.executor.cores` = 2, `spark.cores.max` = SPARK_CORES_MAX (default 4), `spark.driver.host` = SPARK_DRIVER_HOST, `spark.driver.bindAddress` = 0.0.0.0, `spark.driver.port` = 40000, `spark.blockManager.port` = 40001. With 2 cores per executor, `SPARK_CORES_MAX=2` runs one executor and 4 runs two (one per worker). The cluster has 2 workers with 2 cores and 2g each.

### 4.9 Silver job (`processing/silver_job.py`, `schemas.py`, `rules.py`)

Command: `python -m gamepulse.processing.silver_job --run-date D --run-id R`. On any exception it writes failed stage metrics and exits 1.

**Step 1, read.** Path `<DATA_ROOT>/bronze/events/run_date=D` (converted with `to_spark_uri`). If there are no files, exit 1 with the message `bronze is empty`. Check the Bronze columns and types against 4.7. If they differ, exit 1 with the message `bronze schema changed`.

**Step 2, parse.** `schemas.py` defines `ENVELOPE_SCHEMA`: a StructType where every field is StringType: `event_id`, `event_type`, `event_version`, `event_ts`, `match_id`, `player_id`, and `payload` as a nested struct with the string fields `map_name`, `game_mode`, `region`, `team`, `skill_rating`, `character_id`, `item_id`, `cost`, `target_player_id`, `amount`, `damage_source`, `victim_player_id`, `reason`, `winning_team`, `duration_seconds`, `blue_score`, `red_score`, `kills`, `deaths`, `assists`, `damage_dealt`, `damage_received`, `gold_spent`, `won`. Use `F.from_json(F.col('raw_json'), ENVELOPE_SCHEMA, {'mode': 'PERMISSIVE'})`. Flatten into plain string columns with the same names as the keys. Keep `raw_json` and the Bronze metadata columns. Add `ts = F.to_timestamp(event_ts, yyyy-MM-dd'T'HH:mm:ss.SSSX)`.

**Step 3, row rules in `rules.py`.** Start with `rejection_reason` as a null string column. Every rule uses this helper, which is the only way rules may be applied:

```python
def add_reason(df, reason, valid_expr):
    bad = ~F.coalesce(valid_expr, F.lit(False))
    return df.withColumn(
        'rejection_reason',
        F.when(F.col('rejection_reason').isNull() & bad, F.lit(reason)).otherwise(F.col('rejection_reason')),
    )
```

`valid_expr` is an expression that is true when the row is fine. A null result counts as invalid. Apply in this order. The first rule that fails sets the reason and later rules skip that row.

| # | reason | valid when |
|---|---|---|
| 1 | invalid_json | the parsed struct is not null |
| 2 | missing_required_field | event_id, event_type, event_ts, match_id are not null and not empty after trim |
| 3 | unsupported_event_version | event_version equals 1 |
| 4 | unknown_event_type | event_type is one of the 10 types in EVENT_SPECS |
| 5 | invalid_timestamp | ts is not null |
| 6 | timestamp_out_of_range | to_date(ts) equals the run_date |
| 7 | missing_player_id | for event types with requires_player, player_id is not null and not empty |
| 8 | invalid_id_format | match_id matches `^m_[0-9]{8}_[0-9]{5}$` and player_id, when not null, matches `^p_[0-9]{6}$` |
| 9 | missing_payload_field | every payload field of the row's event type (from EVENT_SPECS) is not null and not empty |
| 10 | invalid_numeric_value | every int payload field of the row's type casts to a number and is inside RANGES, and won is true or false |
| 11 | invalid_reference_value | character_id in the 12 ids, item_id in the 12 ids, team and winning_team in TEAMS, region in REGIONS, map_name in MAPS, game_mode equals arena_5v5, damage_source in DAMAGE_SOURCES, reason in LEAVE_REASONS, and target_player_id and victim_player_id match the player regex, each checked only for the event types that have that field |

Rules 9 to 11 are built by a helper `per_type_valid(condition_for_type)`: start with `F.lit(True)`, and for each event type combine with `& F.when(F.col('event_type') == t, condition_for_type(t)).otherwise(F.lit(True))`. This helper is the only allowed way to write per-type logic. No Python UDFs anywhere.

**Step 4, duplicates.** Split the frame into rows with a null reason (ok) and the rest (bad). On ok add `rn = row_number() over Window.partitionBy('event_id').orderBy('ingested_at', 'kafka_partition', 'kafka_offset')`, apply `add_reason(ok, 'duplicate_event_id', F.col('rn') == 1)`, drop `rn`, and rebuild the frame with `bad.unionByName(ok)`.

**Step 5, references.** Build `starts` = distinct `match_id` of rows with null reason and type `match_start`, with a literal column `has_start = True`. Left join on `match_id`, then `add_reason(df, 'orphan_match', F.col('has_start').isNotNull())`, drop `has_start`. Build `members` = distinct `(match_id, player_id)` of rows with null reason and type `player_join`, with a literal column `is_member = True`. Write one function `check_membership(df, members, id_col)` that left joins members on `match_id` and `id_col`, applies `add_reason(df, 'unknown_player_in_match', F.col(id_col).isNull() | (F.col('event_type') == 'player_join') | F.col('is_member').isNotNull())` and drops the helper columns. Call it for `player_id`, then `target_player_id`, then `victim_player_id`. Because members is distinct per key, these joins never multiply rows.

**Step 6, persist** the frame with `StorageLevel.MEMORY_AND_DISK`. `reason_counts = df.groupBy('rejection_reason').count().collect()` gives all metrics (the result has at most about 15 rows).

**Step 7, write.** Delete the output prefixes for this date first (`silver/events/event_date=D`, `quarantine/events/run_date=D`) with `storage.delete_prefix`. This delete-then-write is required so a retry after a partial write cannot leave duplicate files for date D. Silver: rows with null reason, typed per 4.10, `repartition(SPARK_SHUFFLE_PARTITIONS)`, `write.mode('overwrite').partitionBy('event_date', 'event_type').parquet(...)`. Quarantine: rows with a reason, columns `raw_json`, `rejection_reason`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at`, `quarantined_at` (current timestamp), `run_date` (date), `coalesce(1)`, `partitionBy('run_date')`.

**Step 8, metrics** (stage silver): `records_in` = Bronze rows, `records_out` = Silver rows, `records_rejected` = quarantine rows. `config` has `spark_master`, `spark_cores_max`, `shuffle_partitions`. `extra` has `reason_counts` (reason to count), `spark_app_id`.

### 4.10 Silver schema

| Column | Type | Column | Type |
|---|---|---|---|
| event_id | string | target_player_id | string |
| event_ts | timestamp | amount | int |
| match_id | string | damage_source | string |
| player_id | string | victim_player_id | string |
| event_version | int | reason | string |
| map_name | string | winning_team | string |
| game_mode | string | duration_seconds | int |
| region | string | blue_score, red_score | int |
| team | string | kills, deaths, assists | int |
| skill_rating | int | damage_dealt, damage_received | long |
| character_id | string | gold_spent | int |
| item_id | string | won | boolean |
| cost | int | kafka_partition | int |
| | | kafka_offset | long |
| | | ingested_at | timestamp |
| | | silver_processed_at | timestamp |
| event_date, event_type | partition columns | | |

Typing: ints use `cast('int')`, damage totals use `cast('long')`, won is `F.col('won') == 'true'`. `event_date = to_date(ts)`.

### 4.11 Gold job (`processing/gold_job.py`)

Command: `python -m gamepulse.processing.gold_job --run-date D --run-id R`. Reads Silver for the date with `spark.read.option('basePath', silver_root).parquet(silver_root + '/event_date=' + D)`. Exit 1 if that path has no files. Date-scoped tables are written with a helper `write_day(df, table, run_date)`: delete `gold/<table>/match_date=D`, then `df.repartition(SPARK_SHUFFLE_PARTITIONS)` (or `coalesce(1)` for dimensions and aggregates) `.write.mode('overwrite').partitionBy('match_date').parquet(...)`. The delete-then-write is required for safe retries (see 4.18).

| Table | Grain | Primary key | Columns | Written |
|---|---|---|---|---|
| dim_character | one row per character | character_id | character_id, character_name, role | only if the path does not exist |
| dim_item | one row per item | item_id | item_id, item_name, category, cost | only if the path does not exist |
| dim_date | one row per calendar day 2025-01-01 to 2030-12-31 | date_key | date_key (int yyyymmdd), date, year, month, day, day_of_week (1 Monday to 7 Sunday), day_name, is_weekend | only if the path does not exist |
| dim_player | one row per player | player_id | player_id, first_seen_date, last_seen_date, matches_played, latest_skill_rating | every run, full overwrite, no partitions |
| dim_match | one row per match | match_id | match_id, match_date, region, map_name, game_mode, start_ts, end_ts, duration_seconds, winning_team, blue_score, red_score, is_complete | every run, by match_date |
| fact_match_events | one row per event except player_stats | event_id | event_id, event_ts, date_key, match_id, player_id, event_type, actor_character_id, item_id, item_cost, target_player_id, victim_player_id, damage_amount, damage_source | every run, by match_date |
| fact_player_match | one row per player per match | match_id + player_id | match_id, player_id, date_key, character_id, team, won, kills, deaths, assists, damage_dealt, damage_received, gold_spent, skill_rating, region, duration_seconds | every run, by match_date |
| agg_daily_overview | one row per date | match_date | match_date, matches, complete_matches, events, active_players, avg_duration_seconds, total_eliminations, avg_damage_dealt_per_player | every run, by match_date |
| agg_character_daily | one row per date and character | match_date + character_id | match_date, character_id, games, wins, win_rate, avg_kills, avg_deaths, avg_assists, avg_damage_dealt, avg_gold_spent | every run, by match_date |

Build rules:

- `dim_character`, `dim_item` come from `reference_data` with `spark.createDataFrame`. `dim_date` comes from `F.sequence` over the date range exploded to rows. `day_of_week = ((F.dayofweek(date) + 5) % 7) + 1`, `day_name = F.date_format(date, 'EEEE')`, `is_weekend = day_of_week >= 6`.
- `dim_player` reads only the `player_join` slice of all of Silver: `parquet(silver_root + '/event_date=*/event_type=player_join')` with `basePath`. It groups by `player_id`: `min(event_date)`, `max(event_date)`, `count(*)`, and `F.expr('max_by(skill_rating, event_ts)')`. This is cheap because `player_join` is about 2 percent of Silver. Full overwrite of this small table is intended.
- `dim_match` is `match_start` rows left joined to `match_end` rows on `match_id`. `is_complete` is true when the `match_end` row exists.
- `fact_match_events` takes Silver rows of the day where `event_type != 'player_stats'`, left joins `character_select` rows on `(match_id, player_id)` to get `actor_character_id`, renames `cost` to `item_cost` and `amount` to `damage_amount`, and sets `date_key` from `event_date`. `match_date` is `event_date`.
- `fact_player_match` takes `player_stats` rows, left joins `player_join` rows on `(match_id, player_id)` for `skill_rating`, and left joins `dim_match` on `match_id` for `region` and `duration_seconds`.
- `agg_daily_overview`: matches and complete_matches from `dim_match`; events from `fact_match_events`; active_players is `countDistinct(player_id)` on `fact_player_match`; avg_duration_seconds over complete matches; total_eliminations counts elimination events; avg_damage_dealt_per_player is the mean of `damage_dealt`.
- `agg_character_daily`: group `fact_player_match` by `match_date`, `character_id`: games = `count(*)`, wins = `sum(cast(won as int))`, win_rate = wins / games, averages of kills, deaths, assists, damage_dealt, gold_spent.

Metrics (stage gold): `records_in` = Silver rows read for the date, `records_out` = rows written to date-scoped tables plus `dim_player`, `extra.table_counts` = rows per table.

### 4.12 Metrics files (`common/metrics.py`)

`write_stage_metrics(root, stage, run_id, run_date, started_at, finished_at, status, records_in, records_out, records_rejected, config, extra)` writes one JSON file to `metrics/stage=<stage>/run_date=<D>/<sanitized run_id>.json` with the keys: `stage`, `run_id`, `run_date`, `status` (`success` or `failed`), `started_at` and `finished_at` (ISO 8601 UTC), `duration_seconds` (float, 3 decimals), `records_in`, `records_out`, `records_rejected`, `throughput_per_second` (`records_in / duration_seconds`, 0 when the duration is 0), `config` (object), `extra` (object). Also provide `sanitize_run_id(s)`, a `Stopwatch` helper (start and stop UTC times and duration) and `read_all_metrics(root) -> list[dict]`. Stages: `produce`, `ingest`, `dlq_archive`, `silver`, `gold`, `quality`. Every stage CLI writes a failed metrics file when it raises and then exits 1.

### 4.13 Quality checks (`quality/checks.py`, `quality/cli.py`)

Each check returns a `CheckResult` with `name`, `severity` (`critical` or `warn`), `passed`, `observed`, `expected`, `detail`. Checks run through DuckDB views (4.15) filtered to the `run_date`. `python -m gamepulse.quality.cli --run-date D --run-id R` runs all checks, prints a table, writes stage metrics `quality` (`records_in` = number of checks, `records_out` = passed checks, `records_rejected` = failed critical checks, `extra.checks` = the full list), and exits 1 if any critical check failed.

| ID | name | severity | rule |
|---|---|---|---|
| C01 | bronze_not_empty | critical | Bronze row count for the date is above 0 |
| C02 | bronze_schema_matches | critical | Bronze columns and types equal the table in 4.7 |
| C03 | row_reconciliation | critical | Bronze rows = Silver rows + event quarantine rows |
| C04 | reject_rate | critical | quarantine rows / Bronze rows is at most MAX_REJECT_RATE |
| C05 | silver_event_id_unique | critical | no repeated event_id in Silver for the date |
| C06 | silver_required_not_null | critical | event_id, event_ts, match_id, event_type never null, player_id never null for types that require it |
| C07 | silver_event_types_valid | critical | every event_type is in EVENT_SPECS |
| C08 | silver_ts_in_run_date | critical | every event_ts date equals the run_date |
| C09 | silver_schema_matches | critical | Silver columns and types equal 4.10 |
| C10 | numeric_ranges | critical | damage amount in 1 to 1000, cost in 1 to 5000 |
| C11 | fact_match_in_dim_match | critical | every match_id in fact_match_events exists in dim_match |
| C12 | fact_player_in_dim_player | critical | every non-null player_id in fact_match_events exists in dim_player |
| C13 | fact_refs_dims | critical | every non-null actor_character_id is in dim_character, every non-null item_id is in dim_item |
| C14 | player_match_unique | critical | no repeated (match_id, player_id) in fact_player_match |
| C15 | gold_not_empty | critical | dim_match, fact_match_events, fact_player_match all have rows for the date |
| C16 | complete_matches_have_10_stats | warn | every complete match has 10 rows in fact_player_match |
| C17 | incomplete_match_ratio | warn | matches without match_end are at most 2 percent of matches |
| C18 | stats_match_events | warn | kills, assists and damage_dealt in fact_player_match equal the counts and sums rebuilt from fact_match_events |

Warnings do not fail the run. They are shown in the dashboard and the quality metrics. With deliberate corruption turned on, C18 may warn, and that is expected.

### 4.14 Airflow DAG (`airflow/dags/gamepulse_daily.py`)

`dag_id=gamepulse_daily`, `schedule='@daily'`, `start_date=datetime(2026, 1, 1)`, `catchup=False`, `max_active_runs=1`, `params={'num_matches': 200, 'seed': 42, 'corrupt_rate': 0.0}`, `default_args={'retries': 2, 'retry_delay': timedelta(minutes=1), 'execution_timeout': timedelta(minutes=60)}`. The file imports only Airflow and the standard library. Eight BashOperator tasks in a straight line. `PY` is `/opt/gamepulse-venv/bin/python`.

| task_id | bash_command |
|---|---|
| preflight | `PY -m gamepulse.ops.preflight` |
| create_topics | `PY -m gamepulse.ingestion.admin create` |
| produce_events | `PY -m gamepulse.ingestion.produce_cli --run-date {{ ds }} --run-id '{{ run_id }}' --num-matches {{ params.num_matches }} --seed {{ params.seed }} --corrupt-rate {{ params.corrupt_rate }}` |
| ingest_events | `PY -m gamepulse.ingestion.ingest_cli --run-date {{ ds }} --run-id '{{ run_id }}' --num-consumers 3` |
| archive_dlq | `PY -m gamepulse.ingestion.dlq_archive --run-date {{ ds }} --run-id '{{ run_id }}'` |
| spark_silver | `PY -m gamepulse.processing.silver_job --run-date {{ ds }} --run-id '{{ run_id }}'` |
| spark_gold | `PY -m gamepulse.processing.gold_job --run-date {{ ds }} --run-id '{{ run_id }}'` |
| quality_checks | `PY -m gamepulse.quality.cli --run-date {{ ds }} --run-id '{{ run_id }}'` |

`admin create` without arguments creates the events topic (6 partitions) and its dead letter topic (1 partition) using the environment defaults. Reruns are safe because Silver and Gold overwrite one date and duplicate Bronze rows are removed in Silver. Backfill command: `airflow dags backfill -s 2026-01-01 -e 2026-01-03 gamepulse_daily` run in the scheduler container.

**Note on produce+ingest reruns:** re-running `produce_events` appends another batch of messages to Kafka. Re-ingesting can add more Bronze files for the same `run_date`. That is expected under at-least-once delivery. Silver dedupe by `event_id` and the delete-then-write pattern keep Silver and Gold correct for date D. Prefer clearing the Kafka topic (or using a fresh `run_date`) when you need a clean produce-only demo; pipeline correctness does not require it.

### 4.15 DuckDB layer and SQL (`analytics/`, `sql/`)

`analytics/duck.py`: `connect(root) -> DuckDBPyConnection` creates an in-memory connection. For an `s3://` root it runs `INSTALL httpfs`, `LOAD httpfs` and `CREATE OR REPLACE SECRET gp_s3 (TYPE S3, KEY_ID '...', SECRET '...', REGION '...')` with values from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`. `create_views(con, root)` reads `sql/views.sql`, splits it on lines that contain only `-- split`, replaces `{ROOT}` with the root string and runs each statement. A statement that fails because no files exist is skipped with a warning. Views are all `SELECT * FROM read_parquet('{ROOT}/<path>/**/*.parquet', hive_partitioning=true, union_by_name=true)` over: `v_bronze_events` (`bronze/events`), `v_silver_events` (`silver/events`), `v_quarantine_events` (`quarantine/events`), `v_quarantine_ingestion` (`quarantine/ingestion`), and one view per Gold table named `v_<table>` (`v_dim_character`, `v_dim_item`, `v_dim_date`, `v_dim_player`, `v_dim_match`, `v_fact_match_events`, `v_fact_player_match`, `v_agg_daily_overview`, `v_agg_character_daily`).

Analytics queries live in `sql/analytics/NN_name.sql`. Each is a single SELECT over the `v_*` views. `analytics/run_query.py` has `load_query(name)`, `run_named_query(con, name) -> DataFrame`, and a CLI `python -m gamepulse.analytics.run_query <name>`. KDA is always `(kills + assists) / GREATEST(deaths, 1)`.

| File | Output columns and logic |
|---|---|
| 01_avg_damage_per_match | match_date, matches, avg_damage_per_match, avg_damage_per_player. From v_fact_player_match: sum damage_dealt per match, then average per date |
| 02_character_win_rates | character_id, character_name, role, games, wins, win_rate. Join v_dim_character. Order by win_rate descending |
| 03_player_performance_trend | player_id, match_date, games, avg_kda, avg_damage_dealt. Only the 10 players with the most games. Order by player_id, match_date |
| 04_top_items | item_id, item_name, category, purchases, total_gold_spent, share_of_purchases, purchase_rank. From v_fact_match_events where event_type is item_purchase, join v_dim_item |
| 05_match_duration_distribution | bucket_start_minutes (floor(duration_seconds / 120) * 2), matches. From v_dim_match where is_complete. Order by bucket |
| 06_elimination_assist_stats | role, player_rows, avg_kills, avg_assists, avg_deaths, kills_per_minute. Kills per minute is the sum of kills divided by the sum of duration_seconds, times 60. Group by role |
| 07_character_comparison | character_id, character_name, games, avg_damage_dealt, avg_damage_received, avg_gold_spent, avg_kda, damage_rank, kda_rank. Ranks with RANK() OVER, highest first |
| 08_daily_event_volume | match_date, event_type, events. Order by match_date, event_type |

### 4.16 Settings and environment variables

| Variable | Default | Used by | On Settings? |
|---|---|---|---|
| DATA_ROOT | /opt/gamepulse/data/lake | everything | yes (`data_root`) |
| AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY | empty | S3 access (not needed in local mode) | no (env only) |
| AWS_REGION | us-east-1 | pyarrow, Spark, DuckDB, Terraform | yes (`aws_region`) |
| KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | producer, consumers, admin | yes |
| KAFKA_TOPIC | gamepulse.events.v1 | producer, consumers, admin | yes |
| KAFKA_PARTITIONS | 6 | admin create | yes |
| SPARK_MASTER | spark://spark-master:7077 in compose, local[2] otherwise | Spark jobs | yes |
| SPARK_CORES_MAX | 4 | Spark jobs | yes |
| SPARK_SHUFFLE_PARTITIONS | 8 | Spark jobs | yes |
| SPARK_EXECUTOR_MEMORY | 1g | Spark jobs | yes |
| SPARK_DRIVER_MEMORY | 1g | Spark jobs | yes |
| SPARK_DRIVER_HOST | set per container in compose (runner, airflow-scheduler); default localhost when unset | Spark jobs | yes |
| MAX_REJECT_RATE | 0.05 | quality checks | yes |
| LOG_LEVEL | INFO | all | yes |
| AIRFLOW_UID | 50000 (Linux: output of `id -u`) | compose | no |
| AIRFLOW_ADMIN_USER, AIRFLOW_ADMIN_PASSWORD | admin, admin | airflow-init (local use only) | no |
| AIRFLOW_FERNET_KEY | generated by `make init` | Airflow | no |

Settings field names are the lowercase variable names for rows marked "yes" above. `.env.example` lists all of them. `.env` is never committed.

### 4.17 Consistency notes (implementation risks already decided)

1. **Same-day timestamps:** enforced by `SCHEDULE_WINDOW_SECONDS` plus the clamp in 4.4 step 4. Silver rule 6 and quality C08 reject any row that still falls outside `run_date`.
2. **Player ID capacity:** `num_players` max is 1_000_000; IDs remain six digits. Do not change the ID format.
3. **Produce is append-only:** Kafka + Bronze may accumulate duplicates across retries. Silver and Gold are the idempotent layers (4.18).
4. **C13 nulls:** left joins in Gold can leave null `actor_character_id` / `item_id`; only non-null values are checked (aligned with C12).
5. **dim_player across days:** full overwrite from all Silver `player_join` partitions is intentional; a single-day Gold run still refreshes lifetime player stats from the whole lake.

### 4.18 Pipeline idempotency and retry

**Required behaviour for date D:**

1. **Ingest → Bronze:** commit Kafka offsets only after a successful Bronze write. A crash before commit may re-deliver messages; Bronze may contain duplicate `event_id`s. Never commit before the Parquet write succeeds.
2. **Silver:** always `delete_prefix` for `silver/events/event_date=D` and `quarantine/events/run_date=D` before writing. Dedupe keeps one row per `event_id` (earliest by `ingested_at`, `kafka_partition`, `kafka_offset`). Final Silver for D must have unique `event_id`s.
3. **Gold:** `write_day` always deletes `gold/<table>/match_date=D` before overwrite for date-scoped tables. `dim_player` is a full overwrite. Re-running Gold for D after a partial failure must not double-count rows in facts or aggregates for D.
4. **Quality:** after a successful retry chain, C03, C05, C14, and C15 must pass for D.

**Integration tests (local `DATA_ROOT`, Spark local mode; mark `integration` or place under `tests/integration/` / `tests/spark/` as the phase task states):**

- **IT-idem-silver:** write a small Bronze fixture for date D that contains deliberate duplicate `event_id`s (simulate re-delivery). Run silver twice. After each run: Silver row count is stable, no duplicate `event_id`, quarantine + Silver reconciles to Bronze (C03 logic).
- **IT-idem-gold:** from a fixed Silver fixture for date D, run gold, then simulate a failed mid-write by writing an extra junk file under `gold/fact_match_events/match_date=D/`, then run gold again. After the second run: fact and aggregate row counts for D equal the first successful run; no duplicate `(match_id, player_id)` in `fact_player_match`; `agg_daily_overview` for D has exactly one row with the same metrics as the clean run.

Wire these tests into the Phase 4 / Phase 5 / Phase 6 tasks that create Silver, Gold, and the end-to-end pipeline (see handoff "Task prompt deltas"). Do not expand architecture beyond delete-then-write + Silver dedupe.

---

## Spec revision log (final review)

| Issue | Change |
|---|---|
| Match timestamps past midnight | 4.4: `SCHEDULE_WINDOW_SECONDS` + explicit end-of-day clamp; invariant + tests |
| Player ID six-digit overflow | 4.1 + 4.4: `num_players <= 1_000_000` |
| AWS credentials | 3.5–3.6: bucket-scoped IAM restated; create/store/revoke steps |
| Retry / duplicates | D2–D3, 4.9 step 7, 4.11, 4.14 note, new 4.18 + IT requirements |
| C13 vs null refs | 4.13 C13 checks non-null refs only |
| Settings vs AWS keys | 4.16 clarifies which vars are on `Settings` |
