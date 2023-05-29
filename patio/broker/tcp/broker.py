from __future__ import annotations

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from functools import cached_property
from types import MappingProxyType
from typing import (
    Any, Callable, Coroutine, Deque, Dict, List, Mapping, Optional, Set,
    TypeVar, Union,
)

from patio import Registry, TaskFunctionType
from patio.broker import AbstractBroker, TimeoutType, serializer
from patio.broker.tcp.protocol import CallRequest, Header, PacketTypes, Protocol
from patio.compat import Queue
from patio.executor import AbstractExecutor


T = TypeVar("T")
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RPCEvent:
    header: Header
    payload: bytes


class PacketHandler(ABC):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    protocol: Protocol
    executor: AbstractExecutor
    registry: Registry

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        executor: AbstractExecutor,
        protocol: Protocol,
    ):
        self.reader = reader
        self.writer = writer
        self.protocol = protocol
        self.executor = executor
        self.__client_info = self.writer.get_extra_info("peername")

        self.__events: Queue[RPCEvent] = Queue(self.executor.max_workers)
        self.__results: Dict[int, asyncio.Future] = {}

        self.__method_map: Mapping[
            PacketTypes, Callable[[RPCEvent], Coroutine[Any, Any, Any]],
        ] = MappingProxyType({
            PacketTypes.REQUEST: self.handle_request,
            PacketTypes.RESPONSE: self.handle_response,
            PacketTypes.ERROR: self.handle_error,
        })

    @cached_property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    async def get_event(self) -> RPCEvent:
        header = Header.unpack(await self.reader.readexactly(Header.SIZE))
        payload = await self.reader.readexactly(header.size)
        return RPCEvent(header=header, payload=payload)

    async def handle_request(self, event: RPCEvent) -> None:
        serial = event.header.serial
        try:
            request: CallRequest = serializer.unpack(event.payload)
        except ValueError:
            self.protocol.pack(
                RuntimeError("Failed to load response"),
                PacketTypes.ERROR,
                serial=serial,
            )
            return

        try:
            result = await asyncio.wait_for(
                self.executor.execute(
                    request.func, *request.args, **request.kwargs
                ),
                timeout=request.timeout,
            )
            self.writer.write(
                self.protocol.pack(result, PacketTypes.RESPONSE, serial=serial),
            )
        except Exception as e:
            self.writer.write(
                self.protocol.pack(
                    e, PacketTypes.ERROR, serial=event.header.serial,
                ),
            )

    async def handle_response(self, event: RPCEvent) -> None:
        future = self.__results.pop(event.header.serial)
        if future.done():
            return
        try:
            payload = serializer.unpack(event.payload)
        except ValueError as e:
            future.set_exception(e)
            return

        future.set_result(payload)

    async def handle_error(self, event: RPCEvent) -> None:
        future = self.__results.pop(event.header.serial)
        if future.done():
            return

        try:
            payload = serializer.unpack(event.payload)
        except ValueError as e:
            future.set_exception(e)
            return

        future.set_exception(payload)

    async def step(self) -> None:
        event = await self.get_event()
        method = self.__method_map[event.header.type]
        await method(event)

    async def make_request(self, request: CallRequest) -> Any:
        serial = self.protocol.get_serial()
        future = self.loop.create_future()
        self.__results[serial] = future
        self.writer.write(
            self.protocol.pack(request, PacketTypes.REQUEST, serial=serial),
        )
        return await future

    @abstractmethod
    async def authorize(self) -> bool:
        raise NotImplementedError

    async def close(self) -> None:
        if not self.writer.can_write_eof():
            return
        self.writer.close()
        await self.writer.wait_closed()

    async def process_events(self) -> None:
        while True:
            event = await self.__events.get()
            method = self.__method_map[event.header.type]
            await method(event)
            self.__events.task_done()

    async def start_processing(self):
        workers = []

        for _ in range(self.executor.max_workers):
            workers.append(self.loop.create_task(self.process_events()))

        try:
            while True:
                try:
                    await self.__events.put(await self.get_event())
                except (ConnectionError, asyncio.IncompleteReadError):
                    address, port = self.__client_info[:2]

                    if ":" in address:
                        address = f"[{address}]"

                    log.info(
                        "Client connection tcp://%s:%d had been closed",
                        address, port,
                    )
                    return
        finally:
            del self.__events

            cancelling = []
            for worker in workers:
                if worker.done():
                    continue
                worker.cancel()
                cancelling.append(worker)
            if cancelling:
                await asyncio.gather(*cancelling, return_exceptions=True)


class ServerPacketHandler(PacketHandler):
    async def authorize(self) -> bool:
        token = uuid.uuid4().bytes
        self.writer.write(self.protocol.pack(token, PacketTypes.AUTH_DIGEST))
        event = await self.get_event()

        if event.header.type != PacketTypes.AUTH_REQUEST:
            return False

        if not self.protocol.authorize_check(event.payload):
            return False

        self.writer.write(self.protocol.pack(None, PacketTypes.AUTH_OK))
        return True


class ClientPacketHandler(PacketHandler):
    async def authorize(self) -> bool:
        event = await self.get_event()
        if event.header.type != PacketTypes.AUTH_DIGEST:
            return False
        token = event.payload
        self.writer.write(self.protocol.authorize_request(token))
        event = await self.get_event()
        return event.header.type == PacketTypes.AUTH_OK


class BrokerBase(AbstractBroker, ABC):
    protocol: Protocol
    registry: Registry

    @cached_property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def __init__(
        self,
        executor: AbstractExecutor,
        address: str,
        port: int = 15383,
        key: bytes = b"",
        reconnect_timeout: TimeoutType = 1,
    ):
        self.__handlers: Set[PacketHandler] = set()
        self.__tasks: Set[asyncio.Task] = set()
        self.address: str = address
        self.port: int = port
        self.protocol = Protocol(key=key)
        self.reconnect_timeout = reconnect_timeout
        super().__init__(executor=executor)

    def create_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
        task = self.loop.create_task(coro)
        self.__tasks.add(task)
        task.add_done_callback(self.__tasks.discard)
        return task

    async def close(self) -> None:
        cancelled: List[asyncio.Task] = []

        for task in self.__tasks:
            if task.done():
                continue
            task.cancel()
            cancelled.append(task)

        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)

        await super().close()

    @abstractmethod
    async def call(
        self,
        func: Union[str, TaskFunctionType],
        *args: Any,
        timeout: Optional[TimeoutType] = None,
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError(func, args, timeout, kwargs)

    async def join(self) -> None:
        async def waiter():
            await self.loop.create_future()
        await self.create_task(waiter())


class Server(BrokerBase):
    def __init__(
        self, executor: AbstractExecutor, address: str,
        port: int = 15383, key: bytes = b"",
    ):
        super().__init__(executor=executor, address=address, port=port, key=key)
        self.server: Optional[asyncio.AbstractServer] = None
        self.clients: Deque[PacketHandler] = deque()
        self.__rotate_lock = asyncio.Lock()

    async def _on_client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ):
        handler = ServerPacketHandler(
            reader=reader, writer=writer,
            executor=self.executor, protocol=self.protocol,
        )

        if not await handler.authorize():
            raise RuntimeError("Unauthorized")

        self.clients.append(handler)

        try:
            await handler.start_processing()
        finally:
            async with self.__rotate_lock:
                self.clients.remove(handler)

    async def start_server(self) -> None:
        self.server = await asyncio.start_server(
            self._on_client_connected, host=self.address, port=self.port,
        )

    async def setup(self) -> None:
        await super().setup()
        await self.start_server()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        await super().close()

    async def call(
        self,
        func: Union[str, TaskFunctionType],
        *args: Any,
        timeout: Optional[TimeoutType] = None,
        **kwargs: Any,
    ) -> Any:
        if not isinstance(func, str):
            raise TypeError("Only strings supports")

        request = CallRequest(
            func=func, args=args, kwargs=kwargs, timeout=timeout,
        )

        async def go():
            async with self.__rotate_lock:
                while not self.clients:
                    log.warning(
                        "No connected clients, try again "
                        "after %.3f seconds.", self.reconnect_timeout,
                    )
                    await asyncio.sleep(self.reconnect_timeout)

                handler = self.clients.popleft()
                self.clients.append(handler)

            return await handler.make_request(request)

        return await asyncio.wait_for(go(), timeout=timeout)


class Client(BrokerBase):
    def __init__(
        self, executor: AbstractExecutor,
        address: str, port: int = 15383, key: bytes = b"",
    ):
        super().__init__(executor=executor, address=address, port=port, key=key)
        self.__connected: Optional[asyncio.Event] = None
        self.__handler: Optional[ClientPacketHandler] = None

    async def get_handler(self) -> ClientPacketHandler:
        await self.__connected.wait()
        while self.__handler is None:
            await self.__connected.wait()
        return self.__handler       # type: ignore

    async def connection_fabric(self):
        self.__connected = asyncio.Event()
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    host=self.address, port=self.port,
                )
                handler = ClientPacketHandler(
                    reader=reader, writer=writer,
                    executor=self.executor, protocol=self.protocol,
                )
                if not await handler.authorize():
                    raise RuntimeError("Unauthorized")

                await handler.start_processing()

                self.__handler = handler
                self.__connected.set()

                log.error(
                    "Connection to tcp://%s:%d closed. "
                    "Reconnecting after %.3f seconds...",
                    self.address, self.port, self.reconnect_timeout,
                )
            except ConnectionError:
                log.error(
                    "Failed to establish connection to tcp://%s:%d closed. "
                    "Reconnecting after %.3f seconds.",
                    self.address, self.port, self.reconnect_timeout,
                )
            finally:
                self.__connected.clear()
                await asyncio.sleep(self.reconnect_timeout)

    async def setup(self) -> None:
        await super().setup()
        self.create_task(self.connection_fabric())

    async def call(
        self,
        func: Union[str, TaskFunctionType],
        *args: Any,
        timeout: Optional[TimeoutType] = None,
        **kwargs: Any,
    ) -> Any:
        if not isinstance(func, str):
            raise TypeError("Only strings supports")

        handler = await self.get_handler()
        return await handler.make_request(
            CallRequest(
                func=func,
                args=args,
                kwargs=kwargs,
                timeout=timeout,
            ),
        )
