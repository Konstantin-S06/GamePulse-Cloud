# Quality rules

Checks run in `gamepulse.quality.cli` via DuckDB views for one `run_date`.
Critical failures exit 1; warnings are recorded only.

## Critical (C01–C15)

| ID | Rule |
|---|---|
| C01 | Bronze row count for the date > 0 |
| C02 | Bronze schema matches spec 4.7 |
| C03 | Bronze = Silver + event quarantine |
| C04 | Quarantine rate ≤ `MAX_REJECT_RATE` (default 5%) |
| C05 | Silver `event_id` unique for the date |
| C06 | Required Silver fields not null |
| C07 | `event_type` in `EVENT_SPECS` |
| C08 | `event_ts` date equals `run_date` |
| C09 | Silver schema matches spec 4.10 |
| C10 | Numeric fields in defined ranges |
| C11 | Fact match events ⊆ `dim_match` |
| C12 | Fact player ids ⊆ `dim_player` (non-null) |
| C13 | Character and item refs ⊆ dimensions (non-null) |
| C14 | No duplicate `(match_id, player_id)` in `fact_player_match` |
| C15 | Gold fact tables non-empty for the date |

## Warnings (C16–C18)

| ID | Rule |
|---|---|
| C16 | Complete matches have 10 `fact_player_match` rows |
| C17 | Incomplete matches ≤ 2% of matches |
| C18 | Stats align with rebuilt event counts |

With deliberate corruption, C18 may warn. The dashboard shows warnings in
quality metrics.

## Silver rejection reasons

Silver applies ordered rules in `rules.py` (invalid JSON, duplicates, orphan
match, unknown player, and others). Counts appear in Silver stage metrics
`extra.reason_counts`.