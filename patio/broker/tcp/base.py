from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Dict, Set, TypeVar

from patio import Registry
from patio.broker.tcp.protocol import Header, PacketTypes, Protocol
from patio.compat import Queue
from patio.executor import AbstractExecutor


T = TypeVar("T")


@dataclass(frozen=True)
class RPCEvent:
    header: Header
    payload: Any


class RPCBase(ABC):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    protocol: Protocol
    executor: AbstractExecutor
    registry: Registry

    __slots__ = (
        "__tasks", "__events", "__outbound", "__results",
        "reader", "writer", "protocol", "executor", "registry",
    )

    @cached_property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        executor: AbstractExecutor,
        registry: Registry,
        key: bytes = b"",
    ):
        self.reader = reader
        self.writer = writer
        self.protocol = Protocol(key=key)
        self.executor = executor
        self.registry = registry

        self.__tasks: Set[asyncio.Task] = set()
        self.__events: Queue[RPCEvent] = Queue(self.executor.max_workers)
        self.__outbound: Queue[bytes] = Queue(self.executor.max_workers)
        self.__results: Dict[int, asyncio.Future] = {}

    @staticmethod
    async def __packet_get(reader: asyncio.StreamReader) -> RPCEvent:
        header = Header.unpack(await reader.read(Header.SIZE))
        payload = await reader.read(header.size)
        return RPCEvent(header=header, payload=payload)

    async def handle_request(self, event: RPCEvent) -> None:
        func, args, kwargs = event.payload
        serial = event.header.serial

        try:
            await self.__outbound.put(
                self.protocol.pack_response(await func(*args, **kwargs), serial),
            )
        except Exception as e:
            await self.__outbound.put(
                self.protocol.pack_error(e, event.header.serial),
            )

    async def handle_response(self, event: RPCEvent) -> None:
        future = self.__results.pop(event.header.serial)
        if future.done():
            return

        if event.header.type == PacketTypes.RESPONSE:
            future.set_result(event.payload)
            return
        future.set_exception(event.payload)

    async def call(self, func, *args, **kwargs) -> Any:
        pass

    async def _worker(self) -> None:
        while True:
            event = await self.__events.get()
            if event.header.type == PacketTypes.REQUEST:
                await self.handle_request(event)
            elif event.header.type in (PacketTypes.RESPONSE, PacketTypes.ERROR):
                await self.handle_response(event)

    async def _sender(self) -> None:
        while True:
            self.writer.write(await self.__outbound.get())

    async def start(self) -> None:
        await self.authorize()

        self.__tasks.add(self.loop.create_task(self._sender()))
        for _ in range(self.max_workers):
            self.__tasks.add(self.loop.create_task(self._worker()))

    async def join(self):
        try:
            await self.writer.wait_closed()
        finally:
            await self.close()

    async def close(self) -> None:
        tasks = self.__tasks
        del self.__tasks

        waiters: Set[asyncio.Task] = set()

        for task in tasks:
            if task.done():
                continue
            task.cancel()
            waiters.add(task)

        if waiters:
            await asyncio.gather(*waiters, return_exceptions=True)

    @abstractmethod
    async def authorize(self) -> None:
        pass
