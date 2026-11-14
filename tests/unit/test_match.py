from datetime import datetime

from gamepulse.simulator.match import generate_match
from gamepulse.simulator.players import build_player_pool
from gamepulse.simulator.sim_config import SimConfig
from tests.unit.helpers import validate_event_shape


def _cfg(**kwargs):
    base = dict(run_date="2026-01-05", seed=42, num_matches=10, num_players=300)
    base.update(kwargs)
    return SimConfig(**base)


def test_match_invariants():
    config = _cfg()
    pool = build_player_pool(config)
    events = generate_match(config, pool, 0)

    for event in events:
        validate_event_shape(event)

    ids = [e["event_id"] for e in events]
    assert len(ids) == len(set(ids))
    for i, event in enumerate(events, start=1):
        assert event["event_id"].endswith(f"-{i:06d}")

    timestamps = [e["event_ts"] for e in events]
    assert timestamps == sorted(timestamps)
    for ts in timestamps:
        assert ts.endswith("Z")
        assert len(ts) == 24
        assert ts[0:10] == config.run_date

    joins = [e for e in events if e["event_type"] == "player_join"]
    assert len(joins) == 10
    players = {e["player_id"] for e in joins}
    assert len(players) == 10
    region = next(e["payload"]["region"] for e in events if e["event_type"] == "match_start")
    pool_by_id = {p.player_id: p for p in pool}
    assert all(pool_by_id[pid].region == region for pid in players)

    teams = {}
    for e in joins:
        teams.setdefault(e["payload"]["team"], set()).add(e["player_id"])
    assert len(teams["blue"]) == 5
    assert len(teams["red"]) == 5

    for pid in players:
        assert (
            sum(1 for e in events if e["event_type"] == "player_join" and e["player_id"] == pid)
            == 1
        )
        assert (
            sum(
                1 for e in events if e["event_type"] == "character_select" and e["player_id"] == pid
            )
            == 1
        )
        assert (
            sum(1 for e in events if e["event_type"] == "player_leave" and e["player_id"] == pid)
            == 1
        )
        assert (
            sum(1 for e in events if e["event_type"] == "player_stats" and e["player_id"] == pid)
            == 1
        )

    for team in ("blue", "red"):
        chars = [
            e["payload"]["character_id"]
            for e in events
            if e["event_type"] == "character_select" and e["payload"]["team"] == team
        ]
        assert len(chars) == len(set(chars))

    assisters = [e for e in events if e["event_type"] == "assist"]
    for assist in assisters:
        elims = [
            e
            for e in events
            if e["event_type"] == "elimination"
            and e["event_ts"] == assist["event_ts"]
            and e["payload"]["victim_player_id"] == assist["payload"]["victim_player_id"]
        ]
        assert len(elims) == 1
        assist_team = next(t for t, ps in teams.items() if assist["player_id"] in ps)
        killer_team = next(t for t, ps in teams.items() if elims[0]["player_id"] in ps)
        assert assist_team == killer_team

    match_end = next(e for e in events if e["event_type"] == "match_end")
    blue_elims = sum(
        1 for e in events if e["event_type"] == "elimination" and e["player_id"] in teams["blue"]
    )
    red_elims = sum(
        1 for e in events if e["event_type"] == "elimination" and e["player_id"] in teams["red"]
    )
    assert match_end["payload"]["blue_score"] == blue_elims
    assert match_end["payload"]["red_score"] == red_elims
    winner = match_end["payload"]["winning_team"]
    if winner == "blue":
        assert blue_elims >= red_elims
    else:
        assert red_elims >= blue_elims

    for stats in (e for e in events if e["event_type"] == "player_stats"):
        pid = stats["player_id"]
        payload = stats["payload"]
        assert payload["kills"] == sum(
            1 for e in events if e["event_type"] == "elimination" and e["player_id"] == pid
        )
        assert payload["deaths"] == sum(
            1
            for e in events
            if e["event_type"] == "elimination" and e["payload"]["victim_player_id"] == pid
        )
        assert payload["assists"] == sum(
            1 for e in events if e["event_type"] == "assist" and e["player_id"] == pid
        )
        assert payload["damage_dealt"] == sum(
            e["payload"]["amount"]
            for e in events
            if e["event_type"] == "damage" and e["player_id"] == pid
        )
        assert payload["damage_received"] == sum(
            e["payload"]["amount"]
            for e in events
            if e["event_type"] == "damage" and e["payload"]["target_player_id"] == pid
        )
        assert payload["gold_spent"] == sum(
            e["payload"]["cost"]
            for e in events
            if e["event_type"] == "item_purchase" and e["player_id"] == pid
        )

    start = next(e for e in events if e["event_type"] == "match_start")
    start_dt = datetime.strptime(start["event_ts"], "%Y-%m-%dT%H:%M:%S.%fZ")
    end_dt = datetime.strptime(match_end["event_ts"], "%Y-%m-%dT%H:%M:%S.%fZ")
    assert match_end["payload"]["duration_seconds"] == int((end_dt - start_dt).total_seconds())


def test_determinism_and_run_date():
    config = _cfg()
    pool = build_player_pool(config)
    a = generate_match(config, pool, 0)
    b = generate_match(config, pool, 0)
    assert a == b
    other = _cfg(run_date="2026-01-06")
    c = generate_match(other, pool, 0)
    assert a != c


def test_late_match_stays_on_run_date():
    config = _cfg(num_matches=5)
    pool = build_player_pool(config)
    events = generate_match(config, pool, config.num_matches - 1)
    for event in events:
        assert event["event_ts"][0:10] == config.run_date
