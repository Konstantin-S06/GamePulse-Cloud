import logging

from gamepulse.simulator.generator import generate_events
from gamepulse.simulator.players import build_player_pool
from gamepulse.simulator.sim_config import SimConfig

logger = logging.getLogger(__name__)


def test_generator_basics():
    config = SimConfig(run_date="2026-01-05", seed=42, num_matches=5, num_players=300)
    events = list(generate_events(config))
    match_ids = sorted({e["match_id"] for e in events})
    assert match_ids == [f"m_20260105_{i:05d}" for i in range(5)]
    assert len({e["event_id"] for e in events}) == len(events)

    again = list(generate_events(config))
    assert events == again

    other_seed = list(
        generate_events(SimConfig(run_date="2026-01-05", seed=1, num_matches=5, num_players=300))
    )
    assert events != other_seed

    pool_a = build_player_pool(SimConfig(run_date="2026-01-05", seed=42, num_players=300))
    pool_b = build_player_pool(SimConfig(run_date="2026-01-06", seed=42, num_players=300))
    assert pool_a == pool_b
    day_b = list(
        generate_events(SimConfig(run_date="2026-01-06", seed=42, num_matches=5, num_players=300))
    )
    assert {e["match_id"] for e in events} != {e["match_id"] for e in day_b}

    stream = generate_events(config)
    assert hasattr(stream, "__next__")


def test_event_volume():
    config = SimConfig(run_date="2026-01-05", seed=42, num_matches=50, num_players=300)
    count = sum(1 for _ in generate_events(config))
    logger.warning("num_matches=50 event_count=%s", count)
    print(f"num_matches=50 event_count={count}")
    assert count > 10000
