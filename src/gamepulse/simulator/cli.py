"""Write simulated events to a JSONL file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from gamepulse.common.logging_setup import get_logger
from gamepulse.simulator.corruption import corrupt_stream
from gamepulse.simulator.generator import generate_events
from gamepulse.simulator.sim_config import SimConfig

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GamePulse event simulator")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--num-matches", type=int, default=100)
    parser.add_argument("--num-players", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--corrupt-rate", type=float, default=0.0)
    parser.add_argument("--corrupt-all-types", action="store_true")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    config = SimConfig(
        run_date=args.run_date,
        seed=args.seed,
        num_matches=args.num_matches,
        num_players=args.num_players,
    )
    events = list(generate_events(config))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    messages = list(
        corrupt_stream(
            events,
            rate=args.corrupt_rate,
            seed=args.seed,
            all_types=args.corrupt_all_types,
        )
    )
    report = corrupt_stream.report  # type: ignore[attr-defined]

    with out_path.open("w", encoding="utf-8", newline="\n") as fh:
        for _key, value in messages:
            fh.write(value + "\n")

    print(f"messages={len(messages)}")
    print(f"events={len(events)}")
    print(f"corruption_counts={json.dumps(report.counts, sort_keys=True)}")
    print(f"output={out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
