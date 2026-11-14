# Simulator

The GamePulse simulator produces synthetic arena match events for pipeline testing.
All data is synthetic and is not real player data.

## Event types

The stream includes ten event types: match_start, player_join, character_select,
item_purchase, damage, elimination, assist, player_leave, match_end, and player_stats.

## Determinism

A `SimConfig` takes `run_date`, `seed`, `num_matches`, and `num_players`.
The player pool depends only on `seed`. Match contents depend on `seed`, `run_date`,
and `match_index`. The same inputs always yield the same event list.

## Corruption

`corrupt_stream` can inject bad JSON, bad fields, duplicates, and other faults at a
configured rate. By default only damage, item_purchase, and player_leave are eligible.
Use `--corrupt-all-types` to include every type.

## Limitations

All data is synthetic and is not real player data. Player skill never changes. A player
can appear in two overlapping matches. Combat is statistical with no map or positions.
The data does not come from any real game.
