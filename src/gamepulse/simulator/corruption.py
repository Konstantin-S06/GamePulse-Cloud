"""Deliberate event corruption for pipeline testing."""

from __future__ import annotations

import copy
import json
import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CorruptionReport:
    counts: dict[str, int] = field(default_factory=dict)
    total_messages: int = 0

    def add(self, name: str) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1


DEFAULT_CORRUPTIBLE = frozenset({"damage", "item_purchase", "player_leave"})


def _json_dumps(event: dict[str, Any]) -> str:
    return json.dumps(event, separators=(",", ":"), sort_keys=True)


def corrupt_stream(
    events: Iterable[dict[str, Any]],
    rate: float,
    seed: int,
    all_types: bool = False,
) -> Iterator[tuple[str, str]]:
    rng = random.Random(f"{seed}-corrupt")
    report = CorruptionReport()
    corrupt_stream.report = report  # type: ignore[attr-defined]

    for event in events:
        key = event["match_id"]
        draw = rng.random()
        event_type = event["event_type"]
        corruptible = True if all_types else event_type in DEFAULT_CORRUPTIBLE

        if rate <= 0.0 or draw >= rate or not corruptible:
            value = _json_dumps(event)
            report.total_messages += 1
            yield key, value
            continue

        applicable: list[str] = []
        applicable.append("drop_required_field")
        applicable.append("bad_timestamp")
        applicable.append("future_timestamp")
        applicable.append("unknown_event_type")
        applicable.append("duplicate_event")
        applicable.append("invalid_json")
        applicable.append("orphan_match")
        if event_type in {"damage", "item_purchase", "player_stats", "match_end"}:
            applicable.append("invalid_numeric")
        if event.get("player_id") is not None:
            applicable.append("invalid_player_ref")
        if event_type in {"character_select", "player_stats", "item_purchase"}:
            applicable.append("invalid_reference_value")

        name = rng.choice(applicable)
        report.add(name)
        mutated = copy.deepcopy(event)

        if name == "drop_required_field":
            field_name = rng.choice(["event_id", "event_type", "event_ts", "match_id"])
            mutated.pop(field_name, None)
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "bad_timestamp":
            mutated["event_ts"] = "not-a-timestamp"
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "future_timestamp":
            ts = mutated["event_ts"]
            mutated["event_ts"] = str(int(ts[0:4]) + 1) + ts[4:]
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "unknown_event_type":
            mutated["event_type"] = "player_teleport"
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "duplicate_event":
            value = _json_dumps(event)
            report.total_messages += 2
            yield key, value
            yield key, value
        elif name == "invalid_json":
            value = _json_dumps(event)
            half = value[: max(1, len(value) // 2)]
            report.total_messages += 1
            yield key, half
        elif name == "orphan_match":
            run_ymd = event["event_ts"][0:10].replace("-", "")
            # event_ts is ISO; date is YYYY-MM-DD at start when valid
            if len(event["event_ts"]) >= 10 and event["event_ts"][4] == "-":
                run_ymd = event["event_ts"][0:10].replace("-", "")
            else:
                # fall back from match_id m_YYYYMMDD_xxxxx
                run_ymd = event["match_id"][2:10]
            mutated["match_id"] = f"m_{run_ymd}_99999"
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "invalid_numeric":
            if event_type == "damage":
                mutated["payload"]["amount"] = -50
            elif event_type == "item_purchase":
                mutated["payload"]["cost"] = -1
            elif event_type == "player_stats":
                mutated["payload"]["kills"] = -1
            elif event_type == "match_end":
                mutated["payload"]["duration_seconds"] = 0
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "invalid_player_ref":
            mutated["player_id"] = "p_999999"
            report.total_messages += 1
            yield key, _json_dumps(mutated)
        elif name == "invalid_reference_value":
            if event_type in {"character_select", "player_stats"}:
                mutated["payload"]["character_id"] = "c99"
            elif event_type == "item_purchase":
                mutated["payload"]["item_id"] = "i99"
            report.total_messages += 1
            yield key, _json_dumps(mutated)


def get_report() -> CorruptionReport:
    return getattr(corrupt_stream, "report", CorruptionReport())
