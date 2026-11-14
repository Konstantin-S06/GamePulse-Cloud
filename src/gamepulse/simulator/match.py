"""Generate all events for a single match."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from gamepulse.reference_data import (
    CHARACTER_BY_ID,
    CHARACTER_IDS,
    DAMAGE_SOURCES,
    EVENT_VERSION,
    GAME_MODE,
    ITEMS,
    MAPS,
    REGION_WEIGHTS,
    REGIONS,
    ROLE_DAMAGE_MULT,
    format_event_id,
    format_match_id,
    format_ts,
)
from gamepulse.simulator.players import Player
from gamepulse.simulator.sim_config import SimConfig

DAY_SECONDS = 86400
MAX_DURATION_SECONDS = 1800
MAX_LEAVE_AFTER_END_SECONDS = 20
MAX_MATCH_SPAN_SECONDS = MAX_DURATION_SECONDS + MAX_LEAVE_AFTER_END_SECONDS
SCHEDULE_WINDOW_SECONDS = DAY_SECONDS - MAX_MATCH_SPAN_SECONDS


def _make_event(
    event_type: str,
    ts: datetime,
    match_id: str,
    player_id: str | None,
    payload: dict[str, Any],
    gen_index: int,
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "event_version": EVENT_VERSION,
        "event_ts": ts,
        "match_id": match_id,
        "player_id": player_id,
        "payload": payload,
        "_gen_index": gen_index,
    }


def generate_match(config: SimConfig, pool: list[Player], match_index: int) -> list[dict[str, Any]]:
    rng = random.Random(f"{config.seed}-{config.run_date}-{match_index}")
    day_start_utc = datetime.strptime(config.run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end_utc = day_start_utc + timedelta(days=1)
    match_id = format_match_id(config.run_date, match_index)

    # Step 1: schedule
    interval = SCHEDULE_WINDOW_SECONDS // config.num_matches
    start_offset = match_index * interval + rng.randint(0, min(interval, 60) - 1)
    start_ts = day_start_utc + timedelta(seconds=start_offset)

    # Step 2: region, map, players
    region = rng.choices(REGIONS, weights=REGION_WEIGHTS)[0]
    map_name = rng.choice(MAPS)
    players_in_region = [p for p in pool if p.region == region]
    selected = rng.sample(players_in_region, 10)
    blue = selected[:5]
    red = selected[5:]
    team_of = {p.player_id: "blue" for p in blue}
    team_of.update({p.player_id: "red" for p in red})
    {p.player_id: p for p in selected}

    events: list[dict[str, Any]] = []
    gen = 0

    def next_gen() -> int:
        nonlocal gen
        g = gen
        gen += 1
        return g

    # Step 3: joins and character selects
    char_of: dict[str, str] = {}
    team_chars: dict[str, set[str]] = {"blue": set(), "red": set()}
    for player in selected:
        join_ts = start_ts + timedelta(seconds=rng.randint(1, 20))
        select_ts = join_ts + timedelta(seconds=rng.randint(1, 10))
        team = team_of[player.player_id]
        if rng.random() < 0.7:
            character_id = rng.choice(list(player.preferred))
        else:
            character_id = rng.choice(CHARACTER_IDS)
        if character_id in team_chars[team]:
            available = [c for c in CHARACTER_IDS if c not in team_chars[team]]
            character_id = rng.choice(available)
        team_chars[team].add(character_id)
        char_of[player.player_id] = character_id
        events.append(
            _make_event(
                "player_join",
                join_ts,
                match_id,
                player.player_id,
                {"team": team, "skill_rating": player.skill},
                next_gen(),
            )
        )
        events.append(
            _make_event(
                "character_select",
                select_ts,
                match_id,
                player.player_id,
                {"character_id": character_id, "team": team},
                next_gen(),
            )
        )

    # Step 4: duration and same-day clamp
    duration = int(min(1800, max(360, rng.gauss(900, 240))))
    end_ts = start_ts + timedelta(seconds=duration)
    if end_ts + timedelta(seconds=MAX_LEAVE_AFTER_END_SECONDS) >= day_end_utc:
        new_end = day_end_utc - timedelta(seconds=MAX_LEAVE_AFTER_END_SECONDS + 1)
        new_start = new_end - timedelta(seconds=duration)
        delta = new_start - start_ts
        for event in events:
            event["event_ts"] = event["event_ts"] + delta
        start_ts = new_start
        end_ts = new_end
    gameplay_start = start_ts + timedelta(seconds=60)
    gameplay_end = end_ts - timedelta(seconds=60)

    def rand_gameplay_ts() -> datetime:
        span_ms = int((gameplay_end - gameplay_start).total_seconds() * 1000)
        offset_ms = rng.randint(0, max(span_ms, 0))
        ts = gameplay_start + timedelta(milliseconds=offset_ms)
        return ts.replace(microsecond=(ts.microsecond // 1000) * 1000)

    # Step 5: winner
    blue_avg = sum(p.skill for p in blue) / 5
    red_avg = sum(p.skill for p in red) / 5
    p_blue = 1 / (1 + 10 ** ((red_avg - blue_avg) / 400))
    winner = "blue" if rng.random() < p_blue else "red"
    winners = blue if winner == "blue" else red
    losers = red if winner == "blue" else blue

    # Step 6: kill counts
    k = max(12, int(duration / 60 * rng.uniform(2.0, 3.5) + 0.5))
    share = rng.uniform(0.55, 0.75)
    winner_kills = int(k * share + 0.5)
    loser_kills = k - winner_kills

    def role_of(player: Player) -> str:
        return CHARACTER_BY_ID[char_of[player.player_id]]["role"]

    def emit_elimination(killer: Player, victim: Player, elim_ts: datetime) -> None:
        killer_role = role_of(killer)
        n_hits = rng.randint(3, 6)
        for j in range(1, n_hits + 1):
            amount = max(1, int(rng.uniform(30, 120) * ROLE_DAMAGE_MULT[killer_role] + 0.5))
            events.append(
                _make_event(
                    "damage",
                    elim_ts - timedelta(milliseconds=j * 400),
                    match_id,
                    killer.player_id,
                    {
                        "target_player_id": victim.player_id,
                        "amount": amount,
                        "damage_source": rng.choice(DAMAGE_SOURCES),
                    },
                    next_gen(),
                )
            )
        teammates = [p for p in (winners if killer in winners else losers) if p is not killer]
        n_assist = rng.choice([0, 1, 1, 2])
        n_assist = min(n_assist, len(teammates))
        assisters = rng.sample(teammates, n_assist) if n_assist else []
        for assister in assisters:
            a_role = role_of(assister)
            amount = max(1, int(rng.uniform(30, 120) * ROLE_DAMAGE_MULT[a_role] + 0.5))
            events.append(
                _make_event(
                    "damage",
                    elim_ts - timedelta(milliseconds=200),
                    match_id,
                    assister.player_id,
                    {
                        "target_player_id": victim.player_id,
                        "amount": amount,
                        "damage_source": rng.choice(DAMAGE_SOURCES),
                    },
                    next_gen(),
                )
            )
        events.append(
            _make_event(
                "elimination",
                elim_ts,
                match_id,
                killer.player_id,
                {"victim_player_id": victim.player_id},
                next_gen(),
            )
        )
        for assister in assisters:
            events.append(
                _make_event(
                    "assist",
                    elim_ts,
                    match_id,
                    assister.player_id,
                    {"victim_player_id": victim.player_id},
                    next_gen(),
                )
            )

    # Step 7: eliminations
    for _ in range(winner_kills):
        weights = [p.skill * ROLE_DAMAGE_MULT[role_of(p)] for p in winners]
        killer = rng.choices(winners, weights=weights)[0]
        victim = rng.choice(losers)
        emit_elimination(killer, victim, rand_gameplay_ts())
    for _ in range(loser_kills):
        weights = [p.skill * ROLE_DAMAGE_MULT[role_of(p)] for p in losers]
        killer = rng.choices(losers, weights=weights)[0]
        victim = rng.choice(winners)
        emit_elimination(killer, victim, rand_gameplay_ts())

    # Step 8: pokes
    for _ in range(duration // 12):
        attacker = rng.choice(selected)
        opposing = losers if attacker in winners else winners
        target = rng.choice(opposing)
        a_role = role_of(attacker)
        amount = max(1, int(rng.uniform(10, 60) * ROLE_DAMAGE_MULT[a_role] + 0.5))
        events.append(
            _make_event(
                "damage",
                rand_gameplay_ts(),
                match_id,
                attacker.player_id,
                {
                    "target_player_id": target.player_id,
                    "amount": amount,
                    "damage_source": rng.choice(DAMAGE_SOURCES),
                },
                next_gen(),
            )
        )

    # Step 9: purchases
    purchase_start = start_ts + timedelta(seconds=30)
    purchase_end = end_ts - timedelta(seconds=60)
    purchase_span_ms = int((purchase_end - purchase_start).total_seconds() * 1000)
    item_weights = [1 / item["cost"] for item in ITEMS]
    for player in selected:
        for _ in range(rng.randint(3, 8)):
            item = rng.choices(ITEMS, weights=item_weights)[0]
            offset_ms = rng.randint(0, max(purchase_span_ms, 0))
            ts = purchase_start + timedelta(milliseconds=offset_ms)
            events.append(
                _make_event(
                    "item_purchase",
                    ts,
                    match_id,
                    player.player_id,
                    {"item_id": item["item_id"], "cost": item["cost"]},
                    next_gen(),
                )
            )

    # Step 10: leaves
    leave_times: dict[str, datetime] = {}
    leave_reasons: dict[str, str] = {}
    for player in selected:
        if rng.random() < 0.05:
            leave_ts = end_ts - timedelta(seconds=rng.randint(5, 59))
            reason = "disconnect"
        else:
            leave_ts = end_ts + timedelta(seconds=rng.randint(2, 20))
            reason = "match_end"
        leave_times[player.player_id] = leave_ts
        leave_reasons[player.player_id] = reason

    # Step 11-12: match_start/end, stats from generated events
    events.append(
        _make_event(
            "match_start",
            start_ts,
            match_id,
            None,
            {"map_name": map_name, "game_mode": GAME_MODE, "region": region},
            next_gen(),
        )
    )

    blue_score = sum(
        1 for e in events if e["event_type"] == "elimination" and team_of[e["player_id"]] == "blue"
    )
    red_score = sum(
        1 for e in events if e["event_type"] == "elimination" and team_of[e["player_id"]] == "red"
    )
    events.append(
        _make_event(
            "match_end",
            end_ts,
            match_id,
            None,
            {
                "winning_team": winner,
                "duration_seconds": duration,
                "blue_score": blue_score,
                "red_score": red_score,
            },
            next_gen(),
        )
    )

    for player in selected:
        events.append(
            _make_event(
                "player_leave",
                leave_times[player.player_id],
                match_id,
                player.player_id,
                {"reason": leave_reasons[player.player_id]},
                next_gen(),
            )
        )

    for player in selected:
        pid = player.player_id
        kills = sum(1 for e in events if e["event_type"] == "elimination" and e["player_id"] == pid)
        deaths = sum(
            1
            for e in events
            if e["event_type"] == "elimination" and e["payload"]["victim_player_id"] == pid
        )
        assists = sum(1 for e in events if e["event_type"] == "assist" and e["player_id"] == pid)
        damage_dealt = sum(
            e["payload"]["amount"]
            for e in events
            if e["event_type"] == "damage" and e["player_id"] == pid
        )
        damage_received = sum(
            e["payload"]["amount"]
            for e in events
            if e["event_type"] == "damage" and e["payload"]["target_player_id"] == pid
        )
        gold_spent = sum(
            e["payload"]["cost"]
            for e in events
            if e["event_type"] == "item_purchase" and e["player_id"] == pid
        )
        events.append(
            _make_event(
                "player_stats",
                end_ts + timedelta(seconds=1),
                match_id,
                pid,
                {
                    "character_id": char_of[pid],
                    "team": team_of[pid],
                    "kills": kills,
                    "deaths": deaths,
                    "assists": assists,
                    "damage_dealt": damage_dealt,
                    "damage_received": damage_received,
                    "gold_spent": gold_spent,
                    "won": team_of[pid] == winner,
                },
                next_gen(),
            )
        )

    # Step 13: sort and assign event_id
    events.sort(key=lambda e: (e["event_ts"], e["_gen_index"]))
    result: list[dict[str, Any]] = []
    for seq, raw in enumerate(events, start=1):
        result.append(
            {
                "event_id": format_event_id(match_id, seq),
                "event_type": raw["event_type"],
                "event_version": raw["event_version"],
                "event_ts": format_ts(raw["event_ts"]),
                "match_id": raw["match_id"],
                "player_id": raw["player_id"],
                "payload": raw["payload"],
            }
        )
    return result
