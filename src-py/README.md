# discord-hybrid-sharding for Python

Python process-mode port of the `discord-hybrid-sharding` manager. Install from this directory with `pip install ./src-py`. It has no mandatory third-party dependencies and can be used with discord.py or another Python Discord client.

## Manager entry point

```python
# manager.py
import asyncio
from discord_hybrid_sharding import ClusterManager

async def main():
    manager = ClusterManager(
        "bot.py",
        {"totalShards": "auto", "shardsPerClusters": 2, "mode": "process", "token": "YOUR_TOKEN"},
    )
    manager.on("clusterCreate", lambda cluster: print(f"Started cluster {cluster.id}"))
    manager.on("clusterReady", lambda cluster: print(f"Cluster {cluster.id} ready"))
    await manager.spawn()

if __name__ == "__main__":
    asyncio.run(main())
```

## Cluster entry point

```python
# bot.py
import asyncio
import discord
from discord_hybrid_sharding import ClusterClient, get_info

async def main():
    info = get_info()
    client = discord.AutoShardedClient(
        intents=discord.Intents.default(),
        shard_ids=info["SHARD_LIST"],
        shard_count=info["TOTAL_SHARDS"],
    )
    cluster = ClusterClient(client)
    @client.event
    async def on_ready():
        await cluster.trigger_ready()
    await client.start(info["DISCORD_TOKEN"])

if __name__ == "__main__":
    asyncio.run(main())
```

`ClusterManager` accepts `totalShards`, `totalClusters`, `shardsPerClusters`, `shardList`, `clusterList`, `token`, `respawn`, `restarts`, `queue`, `spawnOptions`, and `clusterData` options (Python dictionaries use the same camelCase keys as the original package). Shard assignments are exported via `get_info()` and environment variables.

## API

- `ClusterManager.spawn`, `broadcast`, `broadcast_eval`, `fetch_client_values`, `eval_on_manager`, `respawn_all`, `kill_all`, `create_cluster`, and `extend` are async where they perform IPC or process operations.
- `ClusterClient.trigger_ready`, `send`, `request`, `broadcast_eval`, `eval_on_manager`, `fetch_client_values`, `respawn_all`, `trigger_maintenance`, and `spawn_next_cluster` are async.
- Cluster manager and clients expose `.on(event, callback)`, `.once`, `.off`, and `.emit` event methods.
- `HeartbeatManager`, `ReClusterManager`, and `AutoResharderManager` are available as optional extensions.
- `shard_id_for_guild_id`, `chunk_array`, and `fetch_recommended_shards` are exported helpers.

Evaluation strings are Python expressions with `client`, `cluster`, and `context` available in client clusters, and `manager` available to manager evaluation. Python callables are supported for manager-local evaluation only.

## Port scope

The port launches independent Python processes using `multiprocessing` and a duplex IPC pipe. Node worker threads have no direct Python equivalent, so `mode="worker"` is rejected; use `mode="process"`. Bot frameworks remain optional and are initialized by the user's entry point. Discord recommended shard discovery uses the official `/gateway/bot` endpoint.
