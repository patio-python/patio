from __future__ import annotations

import asyncio
import logging
from random import getrandbits
from typing import Any, AsyncContextManager, Optional

from patio.broker.tcp.base import RPCBase
from patio.broker.tcp.protocol import (
    Header, PacketTypes, Protocol, RestrictedUnpickler,
)


class RPCServer(RPCBase, AsyncContextManager):
    __slots__ = "host", "port", "packer", "server"

    async def on_client_connected(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        header: Header
        payload: Any

        token = getrandbits(128).to_bytes(16, "big")
        writer.write(self.packer.authorize_request(token))

        header, payload = await self.packet_get(reader)

        if header.type != PacketTypes.AUTH_REQUEST:
            writer.close()
            await writer.wait_closed()
            return

        writer.write(self.packer.authorize_request(payload))

        header, payload = await self.packet_get(reader)


        while True:
            header, payload = await self.packet_get(reader)


    async def on_request(
        self, writer: asyncio.StreamWriter, header: Header, payload: Any,
    ) -> None:
        pass

    async def on_reponse(
        self, writer: asyncio.StreamWriter, header: Header, payload: Any,
    ):
        pass

    async def on_error(
        self, writer: asyncio.StreamWriter, header: Header, payload: Exception,
    ):
        pass

    async def event_processor(self):
        writer: asyncio.StreamWriter
        header: Header
        payload: Any

        while True:
            writer, (header, payload) = await self.events.get()
            try:
                await self.on_event(writer)
            except Exception as e:
                logging.exception("Failed to handle event: %r", header)

    async def start(self, **kwargs: Any) -> None:
        self.server = await asyncio.start_server(
            self.on_client_connected,
            host=self.host, port=self.port,
            start_serving=True,
            **kwargs
        )
        self.event_processor_task = self.loop.create_task(
            self.event_processor(),
        )

    async def close(self) -> None:
        waiters = []
        if (
            self.event_processor_task is not None and
            not self.event_processor_task.done()
        ):
            self.event_processor_task.cancel()
            waiters.append(self.event_processor_task)

        if self.server:
            self.server.close()
            waiters.append(self.server.wait_closed())

        if not waiters:
            return

        await asyncio.gather(*waiters, return_exceptions=True)

    async def __aenter__(self) -> RPCServer:
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_value: Any, traceback: Any,
    ) -> None:
        await self.close()
        self.server = None

    async def handle_requests(self) -> None:
        while self.server is not None:
            pass

    async def call(self, func, *args, **kwargs) -> Any:
        pass


class RPCClient(RPCBase, AsyncContextManager):
    def __init__(self, host: str, port: int = 23650, key: bytes = b""):
        self.host = host
        self.port = port
        self.packer = Protocol(key=key)
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def connect(self, **kwargs):
        self.reader, self.writer = await asyncio.open_connection(
            host=self.host, port=self.port, **kwargs
        )

    async def close(self) -> None:
        if not self.writer:
            return

        if not self.writer.can_write_eof():
            return

        self.writer.close()
        await self.writer.wait_closed()

    async def __aenter__(self) -> RPCClient:
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_value: Any, traceback: Any,
    ) -> None:
        await self.close()

    async def handle_requests(self) -> None:
        pass

    async def call(self, func, *args, **kwargs) -> Any:
        pass


if __name__ == "__main__":
    async def run():
        async with RPCServer() as server:
            async with RPCClient("127.0.0.1") as client:
                print(client.writer)

        print("exit")

    asyncio.run(run())
