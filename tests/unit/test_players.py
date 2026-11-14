import re

import pytest

from gamepulse.reference_data import PLAYER_ID_PATTERN
from gamepulse.simulator.players import build_player_pool
from gamepulse.simulator.sim_config import SimConfig


def test_same_seed_identical():
    a = build_player_pool(SimConfig(run_date="2026-01-05", seed=42, num_players=300))
    b = build_player_pool(SimConfig(run_date="2026-01-06", seed=42, num_players=300))
    assert a == b


def test_different_seed_differs():
    a = build_player_pool(SimConfig(run_date="2026-01-05", seed=1, num_players=300))
    b = build_player_pool(SimConfig(run_date="2026-01-05", seed=2, num_players=300))
    assert a != b


def test_pool_properties():
    pool = build_player_pool(SimConfig(run_date="2026-01-05", seed=42, num_players=300))
    assert len(pool) == 300
    for p in pool:
        assert 600 <= p.skill <= 2400
        assert len(p.preferred) == 3
        assert len(set(p.preferred)) == 3
        assert re.match(PLAYER_ID_PATTERN, p.player_id)


def test_validation():
    with pytest.raises(ValueError):
        SimConfig(run_date="2026-01-05", num_players=199)
    with pytest.raises(ValueError):
        SimConfig(run_date="2026-01-05", num_players=1_000_001)
    with pytest.raises(ValueError):
        SimConfig(run_date="2026-01-05", num_matches=0)
    with pytest.raises(ValueError):
        SimConfig(run_date="2026-01-05", num_matches=82801)
