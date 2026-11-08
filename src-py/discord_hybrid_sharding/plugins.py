"""Optional cluster lifecycle plugins."""
from __future__ import annotations
import asyncio
import time

class HeartbeatManager:
    def __init__(self,interval=20,max_missed_heartbeats=5):
        self.interval=interval; self.max_missed_heartbeats=max_missed_heartbeats; self.manager=None; self._tasks={}; self._pending={}
    def build(self,manager):
        self.manager=manager; manager.heartbeat=self
        manager.on("clusterReady",self._ready); return self
    def _ready(self,cluster):
        task=self._tasks.pop(cluster.id,None)
        if task: task.cancel()
        self._tasks[cluster.id]=asyncio.create_task(self._run(cluster))
    async def _run(self,cluster):
        try:
            while cluster.process and cluster.process.is_alive():
                date=time.time_ns(); self._pending.setdefault(cluster.id,{})[date]=time.monotonic()
                await cluster.send({"_type":4,"date":date})
                await asyncio.sleep(self.interval)
                pending=self._pending.setdefault(cluster.id,{})
                cutoff=time.monotonic()-self.interval*self.max_missed_heartbeats
                if any(sent<cutoff for sent in pending.values()):
                    self.manager._debug("[Heartbeat] missed acknowledgements; respawning",cluster.id)
                    await cluster.respawn(delay=0); return
        except (asyncio.CancelledError,RuntimeError,EOFError): return
    def ack(self,cluster_id,date): self._pending.get(cluster_id,{}).pop(date,None)
    def stop(self,cluster=None,reason=None):
        ids=[cluster.id] if cluster else list(self._tasks)
        for cid in ids:
            task=self._tasks.pop(cid,None)
            if task: task.cancel()
            self._pending.pop(cid,None)

class ReClusterManager:
    """Schedule a controlled full restart using updated shard assignments."""
    def __init__(self,**options): self.options=options; self.manager=None; self.on_progress=False
    def build(self,manager): self.manager=manager; manager.recluster=self; return self
    async def start(self,**options):
        if self.on_progress: raise RuntimeError("Reclustering is already in progress")
        if not self.manager: raise RuntimeError("Manager is missing on ReClusterManager")
        self.on_progress=True
        try:
            settings={**self.options,**options}
            requested=settings.get("total_shards")
            if requested not in (None,"auto",-1): self.manager.total_shards=requested
            if requested in ("auto",-1): self.manager.total_shards=-1
            if settings.get("total_clusters") is not None: self.manager.total_clusters=settings["total_clusters"]
            if settings.get("shards_per_clusters") is not None: self.manager.shards_per_clusters=settings["shards_per_clusters"]
            if settings.get("shard_list") is not None: self.manager.shard_list=list(settings["shard_list"])
            if self.manager.total_shards == -1:
                if not self.manager.token: raise ValueError("A token is required to discover total_shards")
                from .util import fetch_recommended_shards
                self.manager.total_shards=await asyncio.to_thread(fetch_recommended_shards,self.manager.token)
            # Python processes are restarted with freshly calculated assignments.
            shard_list=self.manager.shard_list or list(range(self.manager.total_shards))
            count=settings.get("total_clusters") or self.manager.total_clusters
            per=settings.get("shards_per_clusters") or self.manager.shards_per_clusters
            if per: count=(len(shard_list)+per-1)//per
            if count in (None,-1,"auto"): count=1
            size=per or (len(shard_list)+count-1)//count
            from .util import chunk_array
            chunks=settings.get("shard_cluster_list") or chunk_array(shard_list,size)
            await self.manager.kill_all("recluster")
            self.manager.clusters.clear(); self.manager.shard_list=shard_list
            self.manager.total_clusters=len(chunks); self.manager.shards_per_clusters=per
            self.manager.spawn_options.update(amount=self.manager.total_shards,
                delay=settings.get("delay",self.manager.spawn_options["delay"]),timeout=settings.get("timeout",-1))
            self.manager.shard_list=[shard for part in chunks for shard in part]
            return await self.manager.spawn(amount=self.manager.total_shards,
                delay=settings.get("delay",self.manager.spawn_options["delay"]),timeout=settings.get("timeout",-1))
        finally: self.on_progress=False

class AutoResharderManager:
    """Periodically checks Discord's recommended shard count and triggers a restart."""
    def __init__(self,interval=3600,**options): self.interval=interval; self.options=options; self.manager=None; self._task=None
    def build(self,manager):
        if not manager.token: raise ValueError("AutoResharderManager requires a Discord token")
        self.manager=manager; manager.autoresharder=self
        self._task=asyncio.get_event_loop().create_task(self._run()); return self
    async def _run(self):
        try:
            while True:
                await asyncio.sleep(self.interval)
                count=await asyncio.to_thread(__import__("discord_hybrid_sharding.util",fromlist=["fetch_recommended_shards"]).fetch_recommended_shards,self.manager.token)
                if count != self.manager.total_shards:
                    if not self.manager.recluster: self.manager.extend(ReClusterManager())
                    await self.manager.recluster.start(total_shards=count)
        except asyncio.CancelledError: return
    def stop(self):
        if self._task: self._task.cancel()
