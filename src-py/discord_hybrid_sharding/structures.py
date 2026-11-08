"""Messaging, promises, events and spawn queue primitives."""
from __future__ import annotations
import asyncio
import inspect
import time
from collections import defaultdict
from typing import Any, Callable
from .util import generate_nonce

class EventEmitter:
    def __init__(self): self._listeners: dict[str, list[Callable[..., Any]]] = defaultdict(list); self._event_loop=None
    def on(self, event: str, callback: Callable[..., Any]): self._listeners[event].append(callback); return self
    def once(self, event: str, callback: Callable[..., Any]):
        def wrapped(*args): self.off(event, wrapped); return callback(*args)
        self.on(event, wrapped); return self
    def off(self, event: str, callback: Callable[..., Any]):
        self._listeners[event] = [x for x in self._listeners[event] if x != callback]; return self
    def emit(self, event: str, *args):
        for callback in tuple(self._listeners[event]):
            result = callback(*args)
            if inspect.isawaitable(result):
                try: asyncio.get_running_loop().create_task(result)
                except RuntimeError:
                    if self._event_loop and self._event_loop.is_running(): asyncio.run_coroutine_threadsafe(result,self._event_loop)
                    else: result.close()
        return bool(self._listeners[event])

class BaseMessage(dict):
    def __init__(self, message=None, **kwargs):
        super().__init__(message or {}, **kwargs)
        self.setdefault("nonce", generate_nonce()); self.setdefault("_type", 2)
    def to_json(self): return dict(self)

class IPCMessage(BaseMessage):
    def __init__(self, instance, message): super().__init__(message); self.instance = instance; self.raw = dict(self)
    async def send(self, message): return await self.instance.send({**message, "_type": 2})
    async def request(self, message): return await self.instance.request({**message, "_type": 1, "nonce": self["nonce"]})
    async def reply(self, message): return await self.instance.send({**message, "_type": 3, "nonce": self["nonce"], "_result": message})
    def get_type(self): return self.get("_type", 2)

class PromiseHandler:
    def __init__(self): self._pending: dict[str, asyncio.Future] = {}
    async def create(self, message, options=None):
        nonce = message.setdefault("nonce", generate_nonce()); future = asyncio.get_running_loop().create_future(); self._pending[nonce] = future
        timeout = (options or message.get("options") or {}).get("timeout")
        try: return await asyncio.wait_for(future, timeout / 1000 if timeout else None)
        finally: self._pending.pop(nonce, None)
    def resolve(self, message):
        future = self._pending.get(message.get("nonce"))
        if future and not future.done():
            if message.get("_error"): future.set_exception(RuntimeError(message["_error"].get("message", str(message["_error"]))))
            else: future.set_result(message.get("_result"))

class Queue:
    def __init__(self, options=None): self.options={"auto": True, "timeout": 7000, **(options or {})}; self.queue=[]; self.paused=False
    def add(self, run, *args, timeout=None): self.queue.append((run,args,self.options["timeout"] if timeout is None else timeout)); return self
    async def start(self):
        if not self.options["auto"]: return self
        while self.queue:
            if self.paused: await asyncio.sleep(.1); continue
            run,args,delay=self.queue.pop(0); value=run(*args)
            if inspect.isawaitable(value): await value
            if delay: await asyncio.sleep(delay/1000)
        return self
    async def next(self):
        if not self.paused and self.queue:
            run,args,_=self.queue.pop(0); result=run(*args)
            if inspect.isawaitable(result): return await result
            return result
    def stop(self): self.paused=True; return self
    def resume(self): self.paused=False; return self
