from __future__ import annotations

from typing import Any

from gamepulse.reference_data import EVENT_SPECS


def validate_event_shape(event: dict[str, Any]) -> None:
    required = {
        "event_id",
        "event_type",
        "event_version",
        "event_ts",
        "match_id",
        "player_id",
        "payload",
    }
    assert set(event.keys()) == required
    assert event["event_version"] == 1
    event_type = event["event_type"]
    assert event_type in EVENT_SPECS
    spec = EVENT_SPECS[event_type]
    payload = event["payload"]
    assert set(payload.keys()) == set(spec["payload"].keys())
    for field, typ in spec["payload"].items():
        assert isinstance(payload[field], typ), f"{event_type}.{field}"
    if event_type in {"match_start", "match_end"}:
        assert event["player_id"] is None
    else:
        assert event["player_id"] is not None
