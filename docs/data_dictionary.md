# Data dictionary

All IDs and tables match PROJECT_SPEC.md Part 4. Data is synthetic.

## Keys and formats

| Field | Format |
|---|---|
| run_date | `YYYY-MM-DD` UTC |
| match_id | `m_YYYYMMDD_NNNNN` |
| player_id | `p_NNNNNN` (six digits) |
| event_id | `<match_id>-NNNNNN` |
| character_id | `c01` … `c12` |
| item_id | `i01` … `i12` |
| event_ts | ISO 8601 UTC with `Z` |

## Bronze (`bronze/events/run_date=D/`)

Kafka metadata plus `raw_json`. Schema in spec section 4.7.

## Silver (`silver/events/event_date=D/event_type=T/`)

Typed event columns and Kafka lineage. Partition columns: `event_date`, `event_type`.

## Quarantine

- **events:** rejected rows with `rejection_reason`
- **ingestion:** unparseable Kafka messages

## Gold (`gold/<table>/`)

| Table | Grain |
|---|---|
| dim_character, dim_item, dim_date | Reference dimensions |
| dim_player | One row per player (full overwrite) |
| dim_match | One row per match by `match_date` |
| fact_match_events | One row per event (except player_stats) |
| fact_player_match | One row per player per match |
| agg_daily_overview | One row per `match_date` |
| agg_character_daily | One row per date and character |

DuckDB views are named `v_<table>` and read all Parquet under each Gold prefix.

## Metrics (`metrics/stage=<stage>/run_date=D/<run_id>.json`)

Small JSON files per pipeline stage with counts, duration, and stage-specific `extra`.