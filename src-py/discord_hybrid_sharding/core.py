"""Async process manager and framework-neutral cluster client."""
from __future__ import annotations
import asyncio
import multiprocessing as mp
import os
import runpy
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any
from .structures import BaseMessage, EventEmitter, IPCMessage, PromiseHandler, Queue
from .util import chunk_array, fetch_recommended_shards, generate_nonce, get_info

def _cluster_entry(path, env, conn, data):
    os.environ.update({k: str(v) for k,v in env.items() if v is not None})
    os.environ["DISCORD_HYBRID_SHARDING_DATA"] = __import__('json').dumps(data)
    from . import core
    core._child_connection = conn
    try: runpy.run_path(path, run_name="__main__")
    except BaseException:
        traceback.print_exc(); raise

_child_connection = None

class Cluster(EventEmitter):
    def __init__(self, manager, cluster_id, shard_list, total_shards):
        super().__init__(); self.manager=manager; self.id=cluster_id; self.shard_list=list(shard_list); self.total_shards=total_shards
        self.process=None; self.connection=None; self.ready=False; self._ready_event=None; self._listener=None; self._stopping=False; self._send_lock=threading.Lock()
        self.restarts={"current":0,"max":manager.restarts["max"],"interval":manager.restarts["interval"]}
    async def spawn(self, timeout=-1):
        if self.process and self.process.is_alive(): raise RuntimeError(f"CLUSTER ALREADY SPAWNED | ClusterId: {self.id}")
        self._stopping=False
        ctx=mp.get_context("spawn"); parent,child=ctx.Pipe(duplex=True); self.connection=parent
        env={"SHARD_LIST":",".join(map(str,self.shard_list)),"TOTAL_SHARDS":self.total_shards,"CLUSTER_MANAGER":"true",
             "CLUSTER":self.id,"CLUSTER_COUNT":self.manager.total_clusters,"CLUSTER_MANAGER_MODE":"process",
             "CLUSTER_QUEUE_MODE":"auto" if self.manager.queue.options["auto"] else "manual",
             "DISCORD_TOKEN":self.manager.token or "","MAINTENANCE":self.manager._maintenance.get(self.id,"")}
        self._ready_event=threading.Event(); self._event_loop=self.manager.loop
        self.process=ctx.Process(target=_cluster_entry,args=(self.manager.file,env,child,self.manager.cluster_data),name=f"Cluster-{self.id}")
        self.process.start(); child.close(); self.emit("spawn",self.process)
        self._listener=threading.Thread(target=self._read_messages,daemon=True); self._listener.start()
        if timeout != -1:
            if not await asyncio.to_thread(self._ready_event.wait, timeout/1000): raise TimeoutError(f"CLUSTERING_READY_TIMEOUT | ClusterId: {self.id}")
        return self.process
    def _read_messages(self):
        try:
            while self.connection and self.process and self.process.is_alive():
                if not self.connection.poll(.25): continue
                message=self.connection.recv()
                if isinstance(message,dict): self._handle_message(message)
                if self.process and not self.process.is_alive(): break
        except (EOFError, OSError): pass
        finally:
            if self.process:
                self.process.join(timeout=0); self.ready=False; self.emit("death",self,self.process)
                if not self._stopping and self.manager.respawn and self.restarts["current"] < self.restarts["max"]:
                    self.restarts["current"] += 1
                    try: asyncio.run_coroutine_threadsafe(self.respawn(delay=1000),self.manager.loop)
                    except RuntimeError: pass
    def _handle_message(self,msg):
        kind=msg.get("_type")
        if kind==4:
            self.send({"_type":5,"date":msg.get("date")})
            return
        if kind==5:
            if self.manager.heartbeat: self.manager.heartbeat.ack(self.id,msg.get("date"))
            return
        if kind==15:
            self.ready=True; self._ready_event.set(); self.manager.emit("clusterReady",self); return
        if kind in (8,9):
            if kind==8: asyncio.run_coroutine_threadsafe(self.respawn(),self.manager.loop)
            else: asyncio.run_coroutine_threadsafe(self.manager.respawn_all(**msg.get("options",{})),self.manager.loop)
            return
        if kind==14:
            asyncio.run_coroutine_threadsafe(self.manager.queue.next(),self.manager.loop); return
        if kind in (7,17,19,21): self.manager.promise.resolve(msg); return
        if kind==6:
            async def broadcast_child():
                try:
                    requested=msg.get("options",{}).get("cluster")
                    if requested is None: targets=list(self.manager.clusters.values())
                    elif isinstance(requested,list): targets=[self.manager.clusters[i] for i in requested]
                    else: targets=[self.manager.clusters[requested]]
                    values=await asyncio.gather(*(c.eval(msg.get("_eval",""),msg.get("context"),msg.get("options",{}).get("timeout")) for c in targets))
                    await self.send({"_type":7,"nonce":msg.get("nonce"),"_result":values[0] if requested is not None and not isinstance(requested,list) else values})
                except Exception as exc: await self.send({"_type":7,"nonce":msg.get("nonce"),"_error":{"name":type(exc).__name__,"message":str(exc)}})
            asyncio.run_coroutine_threadsafe(broadcast_child(),self.manager.loop); return
        if kind==18:
            async def manager_eval():
                try:
                    result=eval(msg.get("_eval",""),{"manager":self.manager,"__builtins__":{}})
                    if asyncio.iscoroutine(result): result=await result
                    await self.send({"_type":19,"nonce":msg.get("nonce"),"_result":result})
                except Exception as exc: await self.send({"_type":19,"nonce":msg.get("nonce"),"_error":{"name":type(exc).__name__,"message":str(exc)}})
            asyncio.run_coroutine_threadsafe(manager_eval(),self.manager.loop); return
        if kind==1:
            request=IPCMessage(self,msg); self.manager.emit("clientRequest",request); self.emit("message",request); return
        if kind==10:
            if msg.get("maintenance"): self.manager._maintenance[self.id]=msg["maintenance"]
            else: self.manager._maintenance.pop(self.id,None)
            return
        if kind==13:
            reason=msg.get("maintenance")
            for cluster in self.manager.clusters.values():
                asyncio.run_coroutine_threadsafe(cluster.send({"_type":11 if reason else 12,"maintenance":reason}),self.manager.loop)
            return
        self.emit("message",IPCMessage(self,msg))
    async def send(self,message):
        if not self.connection: raise RuntimeError(f"CLUSTERING_NO_CHILD_EXISTS | ClusterId: {self.id}")
        await asyncio.to_thread(self._send,BaseMessage(message).to_json()); return self
    def _send(self,message):
        with self._send_lock: self.connection.send(message)
    async def request(self,message):
        msg=dict(message); msg.update(_type=1,nonce=msg.get("nonce",generate_nonce())); task=asyncio.create_task(self.manager.promise.create(msg,msg.get("options")))
        await self.send(msg); return await task
    async def eval(self,script,context=None,timeout=None):
        msg={"nonce":generate_nonce(),"_eval":script,"context":context,"options":{"timeout":timeout},"_type":16}
        task=asyncio.create_task(self.manager.promise.create(msg,msg["options"])); await self.send(msg); return await task
    async def kill(self,reason=None,force=True):
        self._stopping=True
        if self.process and self.process.is_alive():
            self.process.terminate(); await asyncio.to_thread(self.process.join,5)
        if self.connection: self.connection.close()
        self.process=None; self.connection=None; self.ready=False; self.manager._debug(f"[KILL] {reason or 'Cluster stopped'}",self.id)
    async def respawn(self,delay=0,timeout=-1):
        await self.kill("respawn")
        if delay: await asyncio.sleep(delay/1000)
        return await self.spawn(timeout)
    def trigger_maintenance(self,reason=None):
        return self.send({"_type":11 if reason else 12,"maintenance":reason})

class ClusterManager(EventEmitter):
    def __init__(self,file,options=None):
        super().__init__(); options=options or {}; self.file=str(Path(file).resolve())
        if not Path(self.file).is_file(): raise FileNotFoundError(self.file)
        if options.get("keepAlive"): raise ValueError("keepAlive is replaced by HeartbeatManager")
        self.respawn=options.get("respawn",True); self.restarts={"max":3,"interval":3600000,**(options.get("restarts") or {})}
        self.cluster_data=options.get("clusterData") or {}; self.total_shards=options.get("totalShards",-1); self.total_clusters=options.get("totalClusters",-1)
        self.shards_per_clusters=options.get("shardsPerClusters"); self.mode=options.get("mode","process")
        if self.mode not in ("process","worker"): raise ValueError('Cluster mode must be "worker" or "process"')
        if self.mode == "worker": raise NotImplementedError("Python worker-thread mode is not supported; use mode='process'")
        if self.shards_per_clusters is not None and self.shards_per_clusters<1: raise ValueError("shardsPerClusters must be positive")
        self.shard_list=list(dict.fromkeys(options.get("shardList") or [])); self.cluster_list=options.get("clusterList") or []
        self.token=(options.get("token") or os.getenv("DISCORD_TOKEN") or "").removeprefix("Bot ") or None
        self.clusters={}; self.shard_cluster_list=[]; self.cluster_data.update({}); self._maintenance={}; self._plugins=[]
        self.spawn_options={"delay":7000,"timeout":-1,"amount":self.total_shards,**(options.get("spawnOptions") or {})}
        self.queue=Queue({"auto":(options.get("queue") or {}).get("auto",True),"timeout":self.spawn_options["delay"]})
        self.promise=PromiseHandler(); self.loop=None
    def _debug(self,message,cluster=None): self.emit("debug",f"[Cluster {cluster}] {message}" if cluster is not None else message)
    def extend(self,plugin): plugin.build(self); self._plugins.append(plugin); return self
    def create_cluster(self,cluster_id,shards,total_shards):
        cluster=Cluster(self,cluster_id,shards,total_shards); cluster._event_loop=self.loop; self.clusters[cluster_id]=cluster; self.emit("clusterCreate",cluster); return cluster
    async def spawn(self,amount=None,delay=None,timeout=None):
        self.loop=asyncio.get_running_loop(); self._event_loop=self.loop; amount=self.spawn_options["amount"] if amount is None else amount
        delay=self.spawn_options["delay"] if delay is None else delay; timeout=self.spawn_options["timeout"] if timeout is None else timeout
        if amount in (-1,"auto",None):
            if not self.token: raise ValueError("A token is required when totalShards is auto")
            amount=await asyncio.to_thread(fetch_recommended_shards,self.token)
        if not isinstance(amount,int) or amount<1: raise ValueError("totalShards must be a positive integer")
        self.total_shards=amount
        if not self.shard_list: self.shard_list=list(range(amount))
        if min(self.shard_list,default=0)<0 or max(self.shard_list,default=-1)>=amount: raise ValueError("Shard IDs must be smaller than totalShards")
        count=self.total_clusters if self.total_clusters not in (-1,"auto",None) else (os.cpu_count() or 1)
        if self.shards_per_clusters: count=(len(self.shard_list)+self.shards_per_clusters-1)//self.shards_per_clusters
        size=self.shards_per_clusters or (len(self.shard_list)+count-1)//count
        self.shard_cluster_list=chunk_array(self.shard_list,size); self.total_clusters=len(self.shard_cluster_list)
        self.spawn_options={"delay":delay,"timeout":timeout,"amount":amount}
        for i,shards in enumerate(self.shard_cluster_list):
            cid=self.cluster_list[i] if i<len(self.cluster_list) else i; cluster=self.create_cluster(cid,shards,amount)
            async def run_cluster(target=cluster, ready_timeout=timeout): return await target.spawn(ready_timeout)
            self.queue.add(run_cluster,timeout=delay*len(shards))
        return await self.queue.start()
    async def broadcast(self,message): return await asyncio.gather(*(c.send(message) for c in self.clusters.values()))
    async def broadcast_eval(self,script,options=None):
        options=options or {}; ids=options.get("cluster")
        if options.get("guildId") is not None: options["shard"] = __import__("discord_hybrid_sharding.util",fromlist=["shard_id_for_guild_id"]).shard_id_for_guild_id(options["guildId"],self.total_shards)
        if options.get("shard") is not None: ids=next((c.id for c in self.clusters.values() if options["shard"] in c.shard_list),None)
        targets=[self.clusters[i] for i in ids] if isinstance(ids,list) else ([self.clusters[ids]] if ids is not None else list(self.clusters.values()))
        if not targets: raise RuntimeError("CLUSTERING_NO_CLUSTERS_FOUND")
        return await asyncio.gather(*(c.eval(script,options.get("context"),options.get("timeout")) for c in targets))
    async def fetch_client_values(self,prop,cluster=None): return await self.broadcast_eval(prop,{"cluster":cluster})
    async def eval_on_manager(self,script,options=None):
        try:
            result=script(self) if callable(script) else eval(script,{"manager":self,"__builtins__":__builtins__})
            if asyncio.iscoroutine(result): result=await result
            return {"_result":result,"_error":None}
        except Exception as exc: return {"_result":None,"_error":{"name":type(exc).__name__,"message":str(exc)}}
    async def respawn_all(self,cluster_delay=5000,respawn_delay=5500,timeout=-1):
        results=[]
        for cluster in self.clusters.values(): results.append(await cluster.respawn(delay=respawn_delay,timeout=timeout)); await asyncio.sleep(cluster_delay/1000)
        return results
    async def kill_all(self,reason="manager shutdown"):
        await asyncio.gather(*(c.kill(reason) for c in self.clusters.values()))
    async def trigger_maintenance(self,reason=None):
        return await asyncio.gather(*(c.trigger_maintenance(reason) for c in self.clusters.values()))

class ClusterClient(EventEmitter):
    """IPC client constructed in a cluster entry point, optionally bound to a bot client."""
    def __init__(self,client=None):
        super().__init__(); self.client=client; self.info=get_info(); self.mode=self.info["CLUSTER_MANAGER_MODE"]
        self.shard_list=self.info["SHARD_LIST"]; self.id=self.info["CLUSTER"]; self.count=self.info["CLUSTER_COUNT"]
        self.maintenance=self.info.get("MAINTENANCE") or False; self.queue={"mode":self.info["CLUSTER_QUEUE_MODE"]}
        self.promise=PromiseHandler(); self.ready=False; self._loop=None; self._recv_thread=None
        if _child_connection is None: raise RuntimeError("ClusterClient must be created inside a ClusterManager child process")
        self._connection=_child_connection
        self._send_lock=threading.Lock()
        self._event_loop=self._loop
        self._recv_thread=threading.Thread(target=self._read_messages,daemon=True); self._recv_thread.start()
    @property
    def shards(self):
        value=getattr(getattr(self.client,"ws",None),"shards",None)
        if value is None: raise AttributeError("Websocket shards property is missing on your Discord Client")
        return value
    async def send(self,message):
        await asyncio.to_thread(self._send,BaseMessage(message).to_json()); return self
    def _send(self,message):
        with self._send_lock: self._connection.send(message)
    def _read_messages(self):
        while True:
            try:
                if not self._connection.poll(.25): continue
                msg=self._connection.recv()
                if self._loop: asyncio.run_coroutine_threadsafe(self._handle_message(msg),self._loop)
            except (EOFError,OSError): return
    async def _handle_message(self,msg):
        kind=msg.get("_type")
        if kind in (7,17,19,21,3): self.promise.resolve(msg); return
        if kind==16:
            try:
                result=eval(msg.get("_eval",""),{"client":self.client,"context":msg.get("context"),"cluster":self,"__builtins__":__builtins__})
                if asyncio.iscoroutine(result): result=await result
                await self.send({"_type":17,"nonce":msg.get("nonce"),"_result":result})
            except Exception as exc: await self.send({"_type":17,"nonce":msg.get("nonce"),"_error":{"name":type(exc).__name__,"message":str(exc)}})
            return
        if kind==11: self.maintenance=msg.get("maintenance") or True; return
        if kind==12: self.maintenance=False; self.trigger_cluster_ready(); return
        if kind==4: await self.send({"_type":5,"date":msg.get("date")}); return
        if kind==1: self.emit("message",IPCMessage(self,msg)); return
        self.emit("message",IPCMessage(self,msg))
    def trigger_ready(self):
        self._loop=asyncio.get_running_loop(); self.ready=True
        return self.send({"_type":15})
    def trigger_cluster_ready(self): self.emit("ready",self); return True
    async def request(self,message):
        msg=dict(message); msg.update(_type=1,nonce=msg.get("nonce",generate_nonce())); task=asyncio.create_task(self.promise.create(msg,msg.get("options"))); await self.send(msg); return await task
    async def broadcast_eval(self,script,options=None):
        opts=options or {}; msg={"nonce":generate_nonce(),"_eval":script,"context":opts.get("context"),"options":opts,"_type":opts.get("_type",6)}
        task=asyncio.create_task(self.promise.create(msg,opts)); await self.send(msg); return await task
    async def eval_on_manager(self,script,options=None):
        opts={"_type":18,**(options or {})}
        if callable(script): raise TypeError("eval_on_manager accepts a Python expression string")
        return await self.broadcast_eval(script,opts)
    async def fetch_client_values(self,prop,cluster=None): return await self.broadcast_eval(prop,{"cluster":cluster})
    async def respawn_all(self,**options): return await self.send({"_type":9,"options":options})
    async def trigger_maintenance(self,reason,all_clusters=False):
        self.maintenance=reason or False; return await self.send({"_type":13 if all_clusters else 10,"maintenance":reason})
    async def spawn_next_cluster(self):
        if self.queue["mode"]=="auto": raise RuntimeError("Next Cluster can only be spawned when the queue is manual")
        return await self.send({"_type":14})
