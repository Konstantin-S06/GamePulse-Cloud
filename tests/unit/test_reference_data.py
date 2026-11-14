import re
from datetime import datetime, timezone

from gamepulse import reference_data as rd


def test_characters_and_items():
    assert len(rd.CHARACTERS) == 12
    assert len(rd.ITEMS) == 12
    assert [c["character_id"] for c in rd.CHARACTERS] == [f"c{i:02d}" for i in range(1, 13)]
    costs = {i["item_id"]: i["cost"] for i in rd.ITEMS}
    assert costs["i01"] == 300
    assert costs["i10"] == 1000
    lo, hi = rd.RANGES["cost"]
    for item in rd.ITEMS:
        assert lo <= item["cost"] <= hi


def test_event_specs():
    assert set(rd.EVENT_SPECS) == set(rd.EVENT_TYPES)
    assert len(rd.EVENT_TYPES) == 10
    for name, spec in rd.EVENT_SPECS.items():
        if name in {"match_start", "match_end"}:
            assert spec["requires_player"] is False
        else:
            assert spec["requires_player"] is True


def test_region_weights():
    assert abs(sum(rd.REGION_WEIGHTS) - 1.0) < 1e-9


def test_id_helpers():
    mid = rd.format_match_id("2026-01-05", 12)
    assert mid == "m_20260105_00012"
    assert re.match(rd.MATCH_ID_PATTERN, mid)
    pid = rd.format_player_id(123)
    assert pid == "p_000123"
    assert re.match(rd.PLAYER_ID_PATTERN, pid)
    eid = rd.format_event_id(mid, 17)
    assert eid == "m_20260105_00012-000017"


def test_format_ts_keeps_milliseconds():
    dt = datetime(2026, 1, 5, 14, 3, 22, 123456, tzinfo=timezone.utc)
    assert rd.format_ts(dt) == "2026-01-05T14:03:22.123Z"
