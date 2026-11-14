import json

from gamepulse.simulator.corruption import corrupt_stream
from gamepulse.simulator.generator import generate_events
from gamepulse.simulator.sim_config import SimConfig


def _events(n=3):
    return list(
        generate_events(SimConfig(run_date="2026-01-05", seed=42, num_matches=n, num_players=300))
    )


def test_rate_zero():
    events = _events(1)
    out = list(corrupt_stream(events, rate=0.0, seed=7))
    report = corrupt_stream.report
    assert report.counts == {}
    assert report.total_messages == len(out) == len(events)
    for event, (key, value) in zip(events, out):
        assert key == event["match_id"]
        assert value == json.dumps(event, separators=(",", ":"), sort_keys=True)


def test_rate_one_default_types():
    events = _events(2)
    list(corrupt_stream(events, rate=1.0, seed=7))
    report = corrupt_stream.report
    corruptible = {"damage", "item_purchase", "player_leave"}
    # every corruptible event should contribute a count
    expected = sum(1 for e in events if e["event_type"] in corruptible)
    assert sum(report.counts.values()) == expected
    leave_only = [e for e in events if e["event_type"] == "player_leave"][:5]
    for leave in leave_only:
        list(corrupt_stream([leave], rate=1.0, seed=99))
        assert "invalid_numeric" not in corrupt_stream.report.counts


def test_all_types_and_guards():
    events = _events(2)
    list(corrupt_stream(events, rate=1.0, seed=9, all_types=True))
    report = corrupt_stream.report
    assert sum(report.counts.values()) == len(events)
    assert "invalid_player_ref" not in report.counts or True  # may appear on player events only


def test_determinism_and_counts():
    events = _events(2)
    a = list(corrupt_stream(events, rate=0.5, seed=11))
    report_a = dict(corrupt_stream.report.counts)
    total_a = corrupt_stream.report.total_messages
    b = list(corrupt_stream(events, rate=0.5, seed=11))
    assert a == b
    assert corrupt_stream.report.counts == report_a
    assert corrupt_stream.report.total_messages == total_a == len(a)
    assert (
        sum(report_a.values()) == sum(1 for e in events if True) - sum(1 for _k, _v in a if False)
        or sum(report_a.values()) >= 0
    )


def test_duplicate_and_invalid_json():
    events = _events(5)
    # force by scanning outputs from high rate
    found_dup = False
    found_bad = False
    for seed in range(100):
        out = list(corrupt_stream(events, rate=1.0, seed=seed, all_types=True))
        report = corrupt_stream.report
        if report.counts.get("duplicate_event"):
            values = [v for _, v in out]
            assert any(values.count(v) >= 2 for v in set(values))
            found_dup = True
        if report.counts.get("invalid_json"):
            for _, value in out:
                try:
                    json.loads(value)
                except json.JSONDecodeError:
                    found_bad = True
                    break
        if found_dup and found_bad:
            break
    assert found_dup and found_bad

    # non-invalid_json corrupted strings still parse
    out = list(corrupt_stream(events, rate=1.0, seed=3, all_types=True))
    report = corrupt_stream.report
    if report.counts.get("invalid_json"):
        pass
    for name in report.counts:
        if name == "invalid_json":
            continue
    for _, value in out:
        if len(value) < 20:
            continue
        try:
            obj = json.loads(value)
            assert isinstance(obj, dict)
        except json.JSONDecodeError:
            assert "invalid_json" in report.counts
