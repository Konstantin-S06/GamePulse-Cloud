"""Player pool for the simulator."""

from __future__ import annotations

import random
from dataclasses import dataclass

from gamepulse.reference_data import CHARACTER_IDS, REGION_WEIGHTS, REGIONS, format_player_id
from gamepulse.simulator.sim_config import SimConfig


@dataclass(frozen=True)
class Player:
    player_id: str
    region: str
    skill: int
    preferred: tuple[str, ...]


def build_player_pool(config: SimConfig) -> list[Player]:
    rng = random.Random(f"{config.seed}-players")
    pool: list[Player] = []
    for i in range(config.num_players):
        region = rng.choices(REGIONS, weights=REGION_WEIGHTS)[0]
        skill = int(min(2400, max(600, rng.gauss(1500, 300))))
        preferred = tuple(rng.sample(CHARACTER_IDS, 3))
        pool.append(
            Player(
                player_id=format_player_id(i),
                region=region,
                skill=skill,
                preferred=preferred,
            )
        )
    for region in REGIONS:
        if sum(1 for p in pool if p.region == region) < 10:
            raise ValueError(f"region {region} has fewer than 10 players")
    return pool
