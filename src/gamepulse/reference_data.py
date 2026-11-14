"""Shared constants and ID helpers for GamePulse."""

from __future__ import annotations

from datetime import datetime, timezone

MATCH_ID_PATTERN = r"^m_[0-9]{8}_[0-9]{5}$"
PLAYER_ID_PATTERN = r"^p_[0-9]{6}$"

CHARACTERS = [
    {"character_id": "c01", "name": "Ironclad", "role": "tank"},
    {"character_id": "c02", "name": "Bulwark", "role": "tank"},
    {"character_id": "c03", "name": "Vanguard", "role": "tank"},
    {"character_id": "c04", "name": "Ember", "role": "damage"},
    {"character_id": "c05", "name": "Volt", "role": "damage"},
    {"character_id": "c06", "name": "Shade", "role": "damage"},
    {"character_id": "c07", "name": "Talon", "role": "damage"},
    {"character_id": "c08", "name": "Ranger", "role": "damage"},
    {"character_id": "c09", "name": "Frost", "role": "damage"},
    {"character_id": "c10", "name": "Warden", "role": "support"},
    {"character_id": "c11", "name": "Lumen", "role": "support"},
    {"character_id": "c12", "name": "Pulse", "role": "support"},
]

ROLE_DAMAGE_MULT = {"tank": 0.7, "damage": 1.3, "support": 0.6}

ITEMS = [
    {"item_id": "i01", "name": "Iron Plate", "category": "defense", "cost": 300},
    {"item_id": "i02", "name": "Blast Core", "category": "offense", "cost": 450},
    {"item_id": "i03", "name": "Swift Boots", "category": "utility", "cost": 250},
    {"item_id": "i04", "name": "Health Flask", "category": "consumable", "cost": 100},
    {"item_id": "i05", "name": "Energy Cell", "category": "consumable", "cost": 120},
    {"item_id": "i06", "name": "Razor Edge", "category": "offense", "cost": 600},
    {"item_id": "i07", "name": "Aegis Shield", "category": "defense", "cost": 700},
    {"item_id": "i08", "name": "Focus Lens", "category": "utility", "cost": 350},
    {"item_id": "i09", "name": "Titan Heart", "category": "defense", "cost": 900},
    {"item_id": "i10", "name": "Storm Gauntlet", "category": "offense", "cost": 1000},
    {"item_id": "i11", "name": "Scout Beacon", "category": "utility", "cost": 150},
    {"item_id": "i12", "name": "Vital Charm", "category": "defense", "cost": 400},
]

MAPS = ["Obsidian Yard", "Sunken Foundry", "Glass Canyon"]
REGIONS = ["na", "eu", "apac"]
REGION_WEIGHTS = [0.40, 0.35, 0.25]
GAME_MODE = "arena_5v5"
TEAMS = ["blue", "red"]
DAMAGE_SOURCES = ["basic_attack", "ability"]
LEAVE_REASONS = ["match_end", "disconnect"]
EVENT_VERSION = 1

RANGES = {
    "skill_rating": (400, 3000),
    "cost": (1, 5000),
    "amount": (1, 1000),
    "duration_seconds": (60, 7200),
    "blue_score": (0, 500),
    "red_score": (0, 500),
    "kills": (0, 500),
    "deaths": (0, 500),
    "assists": (0, 500),
    "damage_dealt": (0, 5000000),
    "damage_received": (0, 5000000),
    "gold_spent": (0, 100000),
}

EVENT_SPECS = {
    "match_start": {
        "requires_player": False,
        "payload": {"map_name": str, "game_mode": str, "region": str},
    },
    "player_join": {
        "requires_player": True,
        "payload": {"team": str, "skill_rating": int},
    },
    "character_select": {
        "requires_player": True,
        "payload": {"character_id": str, "team": str},
    },
    "item_purchase": {
        "requires_player": True,
        "payload": {"item_id": str, "cost": int},
    },
    "damage": {
        "requires_player": True,
        "payload": {"target_player_id": str, "amount": int, "damage_source": str},
    },
    "elimination": {
        "requires_player": True,
        "payload": {"victim_player_id": str},
    },
    "assist": {
        "requires_player": True,
        "payload": {"victim_player_id": str},
    },
    "player_leave": {
        "requires_player": True,
        "payload": {"reason": str},
    },
    "match_end": {
        "requires_player": False,
        "payload": {
            "winning_team": str,
            "duration_seconds": int,
            "blue_score": int,
            "red_score": int,
        },
    },
    "player_stats": {
        "requires_player": True,
        "payload": {
            "character_id": str,
            "team": str,
            "kills": int,
            "deaths": int,
            "assists": int,
            "damage_dealt": int,
            "damage_received": int,
            "gold_spent": int,
            "won": bool,
        },
    },
}

EVENT_TYPES = list(EVENT_SPECS.keys())

CHARACTER_IDS = [c["character_id"] for c in CHARACTERS]
CHARACTER_BY_ID = {c["character_id"]: c for c in CHARACTERS}
ITEM_IDS = [i["item_id"] for i in ITEMS]


def format_match_id(run_date: str, index: int) -> str:
    ymd = run_date.replace("-", "")
    return f"m_{ymd}_{index:05d}"


def format_player_id(i: int) -> str:
    return f"p_{i:06d}"


def format_event_id(match_id: str, seq: int) -> str:
    return f"{match_id}-{seq:06d}"


def format_ts(dt: datetime) -> str:
    """Return millisecond UTC timestamp with trailing Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    ms = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"
