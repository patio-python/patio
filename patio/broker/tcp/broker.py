from __future__ import annotations

import asyncio
import logging
import pickle
import ssl
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from functools import cached_property
from types import MappingProxyType
from typing import (
    Any, Callable, Coroutine, Deque, Dict, Mapping, Optional, Set, Tuple,
    TypeVar, Union, final,
)

from patio.broker import AbstractBroker, TimeoutType
from patio.broker.serializer import (
    AbstractSerializer, RestrictedPickleSerializer,
)
from patio.broker.tcp.protocol import Header, PacketTypes, Protocol
from patio.compat import Queue
from patio.executor import AbstractExecutor
from patio.registry import Registry, TaskFunctionType


T = TypeVar("T")
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RPCEvent:
    header: Header
    payload: bytes


@dataclass
class CallRequest:
    func: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    timeout: Optional[TimeoutType]


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
        try:
            request: CallRequest = self.protocol.serializer.unpack(
                event.payload,
            )
            result = await asyncio.wait_for(
                self.executor.execute(
                    request.func, *request.args, **request.kwargs
                ),
                timeout=request.timeout,
            )
            self.writer.write(
                self.protocol.pack(
                    result, PacketTypes.RESPONSE, serial=event.header.serial,
                ),
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
            payload = self.protocol.serializer.unpack(event.payload)
        except (ValueError, pickle.UnpicklingError) as e:
            future.set_exception(e)
            return

        future.set_result(payload)

    async def handle_error(self, event: RPCEvent) -> None:
        future = self.__results.pop(event.header.serial)
        if future.done():
            return

        try:
            payload = self.protocol.serializer.unpack(event.payload)
        except (ValueError, pickle.UnpicklingError) as e:
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

    async def start_processing(self) -> None:
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


class TCPBrokerBase(AbstractBroker, ABC):
    protocol: Protocol
    registry: Registry

    def __init__(
        self,
        executor: AbstractExecutor,
        key: bytes = b"", *,
        ssl_context: Optional[ssl.SSLContext] = None,
        reconnect_timeout: TimeoutType = 1,
        serializer: AbstractSerializer = RestrictedPickleSerializer()
    ):
        self.protocol = Protocol(key=key, serializer=serializer)
        self.reconnect_timeout = reconnect_timeout
        self._ssl_context: Optional[ssl.SSLContext] = ssl_context
        self.__handlers: Deque[PacketHandler] = deque()
        self.__rotate_lock = asyncio.Lock()

        super().__init__(executor=executor)

    async def _get_handler(self) -> PacketHandler:
        handler: PacketHandler

        async with self.__rotate_lock:
            while not self.__handlers:
                log.warning(
                    "No active connections, retrying after %.3f seconds.",
                    self.reconnect_timeout,
                )
                await asyncio.sleep(self.reconnect_timeout)

            handler = self.__handlers.popleft()
            self.__handlers.append(handler)
        return handler

    async def _add_handler(self, handler: PacketHandler) -> None:
        self.__handlers.append(handler)

    async def _remove_handler(self, handler: PacketHandler) -> None:
        async with self.__rotate_lock:
            self.__handlers.remove(handler)

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

        async def go() -> Any:
            handler = await self._get_handler()
            return await handler.make_request(request)

        return await asyncio.wait_for(go(), timeout=timeout)


DEFAULT_PORT = 15383


@final
class TCPServerBroker(TCPBrokerBase):
    def __init__(
        self,
        executor: AbstractExecutor,
        key: bytes = b"", *,
        ssl_context: Optional[ssl.SSLContext] = None,
        reconnect_timeout: TimeoutType = 1,
        serializer: AbstractSerializer = RestrictedPickleSerializer()
    ):
        super().__init__(
            executor=executor, key=key, ssl_context=ssl_context,
            reconnect_timeout=reconnect_timeout, serializer=serializer,
        )
        self.servers: Set[asyncio.AbstractServer] = set()

    async def _on_client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        handler = ServerPacketHandler(
            reader=reader, writer=writer,
            executor=self.executor, protocol=self.protocol,
        )

        async def handle() -> None:
            if not await handler.authorize():
                raise RuntimeError("Unauthorized")

            await self._add_handler(handler)

            try:
                await handler.start_processing()
            finally:
                await self._remove_handler(handler)

        await self.create_task(handle())

    async def listen(
        self, address: str, port: int = DEFAULT_PORT, **kwargs: Any
    ) -> None:
        if "ssl" not in kwargs:
            kwargs["ssl"] = self._ssl_context

        self.servers.add(
            await asyncio.start_server(
                self._on_client_connected,
                host=address,
                port=port,
                **kwargs
            ),
        )

    async def close(self) -> None:
        tasks = [super().close()]
        for server in self.servers:
            server.close()
            tasks.append(server.wait_closed())
        await asyncio.gather(*tasks, return_exceptions=True)


@final
class TCPClientBroker(TCPBrokerBase):
    async def connection_fabric(
        self, address: str, port: int,
        on_connected: asyncio.Event,
        **kwargs: Any
    ) -> None:
        if "ssl" not in kwargs:
            kwargs["ssl"] = self._ssl_context

        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    host=address, port=port, **kwargs
                )
                try:
                    handler = ClientPacketHandler(
                        reader=reader, writer=writer,
                        executor=self.executor, protocol=self.protocol,
                    )
                    if not await handler.authorize():
                        raise RuntimeError("Unauthorized")

                    on_connected.set()
                    await self._add_handler(handler)

                    await handler.start_processing()
                except asyncio.CancelledError:
                    if not writer.is_closing():
                        writer.close()
                        await writer.wait_closed()
                    raise

                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()

                on_connected.clear()
                await self._remove_handler(handler)

                log.error(
                    "Connection to tcp://%s:%d closed. "
                    "Reconnecting after %.3f seconds...",
                    address, port, self.reconnect_timeout,
                )
            except (ConnectionError, asyncio.TimeoutError):
                log.error(
                    "Failed to establish connection to tcp://%s:%d closed. "
                    "Reconnecting after %.3f seconds.",
                    address, port, self.reconnect_timeout,
                )
            finally:
                await asyncio.sleep(self.reconnect_timeout)
            log.info(
                "Connection attempts to tcp://%s:%d has been stopped.",
                address, port,
            )

    async def connect(
        self, address: str, port: int = DEFAULT_PORT, **kwargs: Any
    ) -> None:
        event = asyncio.Event()
        self.create_task(
            self.connection_fabric(address, port, event, **kwargs),
        )
        await event.wait()


__all__ = ("TCPBrokerBase", "TCPClientBroker", "TCPServerBroker")
