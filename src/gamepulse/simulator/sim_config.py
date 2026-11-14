"""Simulator run configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SimConfig:
    run_date: str
    seed: int = 42
    num_matches: int = 100
    num_players: int = 2000

    def __post_init__(self) -> None:
        if not (200 <= self.num_players <= 1_000_000):
            raise ValueError("num_players must be between 200 and 1000000")
        if not (1 <= self.num_matches <= 82800):
            raise ValueError("num_matches must be between 1 and 82800")
