"""Multi-match event stream."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from gamepulse.simulator.match import generate_match
from gamepulse.simulator.players import build_player_pool
from gamepulse.simulator.sim_config import SimConfig


def generate_events(config: SimConfig) -> Iterator[dict[str, Any]]:
    pool = build_player_pool(config)
    for match_index in range(config.num_matches):
        yield from generate_match(config, pool, match_index)
