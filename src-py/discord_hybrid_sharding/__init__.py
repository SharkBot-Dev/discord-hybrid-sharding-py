"""Python port of discord-hybrid-sharding.

The package is framework agnostic. Use :class:`ClusterManager` to launch a
Python bot entry point and :class:`ClusterClient` inside that entry point.
"""
from .core import Cluster, ClusterClient, ClusterManager
from .plugins import AutoResharderManager, HeartbeatManager, ReClusterManager
from .structures import BaseMessage, IPCMessage, PromiseHandler, Queue
from .util import chunk_array, fetch_recommended_shards, get_info, shard_id_for_guild_id

__all__ = ["Cluster", "ClusterClient", "ClusterManager", "BaseMessage", "IPCMessage",
           "PromiseHandler", "Queue", "HeartbeatManager", "ReClusterManager",
           "AutoResharderManager", "get_info", "chunk_array", "shard_id_for_guild_id",
           "fetch_recommended_shards"]
__version__ = "3.0.1"
