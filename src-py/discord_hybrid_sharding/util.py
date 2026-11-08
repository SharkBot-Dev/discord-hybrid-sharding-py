"""Helpers shared by the manager and cluster client."""
from __future__ import annotations
import json
import os
import secrets
import urllib.error
import urllib.request
from typing import Any, Iterable

def generate_nonce() -> str:
    return secrets.token_urlsafe(18)

def chunk_array(items: Iterable[Any], size: int) -> list[list[Any]]:
    values = list(items)
    if size < 1:
        raise ValueError("chunk size must be at least 1")
    return [values[i:i + size] for i in range(0, len(values), size)]

def shard_id_for_guild_id(guild_id: int | str, total_shards: int = 1) -> int:
    if total_shards < 1:
        raise ValueError("total_shards must be at least 1")
    return (int(guild_id) >> 22) % total_shards

def get_info() -> dict[str, Any]:
    mode = os.getenv("CLUSTER_MANAGER_MODE")
    if mode not in ("process", "worker"):
        raise RuntimeError("NO CHILD/MASTER EXISTS OR SUPPLIED CLUSTER_MANAGER_MODE IS INCORRECT")
    shards = [int(x) for x in os.getenv("SHARD_LIST", "").split(",") if x]
    if not shards:
        raise RuntimeError("SHARD_LIST is missing or empty")
    try: cluster_data = json.loads(os.getenv("DISCORD_HYBRID_SHARDING_DATA", "{}"))
    except json.JSONDecodeError: cluster_data = {}
    return {"SHARD_LIST": shards, "TOTAL_SHARDS": int(os.getenv("TOTAL_SHARDS", "1")),
            "CLUSTER_COUNT": int(os.getenv("CLUSTER_COUNT", "1")), "CLUSTER": int(os.getenv("CLUSTER", "0")),
            "CLUSTER_MANAGER_MODE": mode, "MAINTENANCE": os.getenv("MAINTENANCE") or None,
            "CLUSTER_QUEUE_MODE": os.getenv("CLUSTER_QUEUE_MODE", "auto"),
            "FIRST_SHARD_ID": shards[0], "LAST_SHARD_ID": shards[-1], "CLUSTER_DATA": cluster_data,
            "DISCORD_TOKEN": os.getenv("DISCORD_TOKEN", "")}

def fetch_recommended_shards(token: str, guilds_per_shard: int = 1000) -> int:
    if not token:
        raise ValueError("DISCORD_TOKEN_MISSING")
    request = urllib.request.Request("https://discord.com/api/v10/gateway/bot",
        headers={"Authorization": "Bot " + token.removeprefix("Bot ")})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            raise ValueError("DISCORD_TOKEN_INVALID") from exc
        raise
    return max(1, round(payload["shards"] * (1000 / guilds_per_shard)))
