import asyncio
import time
from functools import reduce
from operator import mul, truediv

import pytest

from patio import Registry, TCPClientBroker, TCPServerBroker
from patio.broker import TimeoutType
from patio.broker.tcp.protocol import Protocol


@pytest.fixture
def registry():
    rpc: Registry = Registry()

    @rpc("mul")
    def multiply(*args: int) -> int:
        return reduce(mul, args)

    @rpc("div")
    def division(*args: int) -> int:
        return reduce(truediv, args)

    @rpc("sleep")
    def sleep(delay: TimeoutType) -> None:
        time.sleep(delay)

    return rpc


async def test_mul(
    server_broker: TCPServerBroker,
    client_broker: TCPClientBroker,
    subtests,
):
    with subtests.test("call from server"):
        assert await server_broker.call("mul", 1, 2, 3) == 6
    with subtests.test("call from client"):
        assert await client_broker.call("mul", 1, 2, 3) == 6

    with subtests.test("max serial reached"):
        client_broker.protocol.serial = Protocol.MAX_SERIAL - 1
        assert await client_broker.call("mul", 2, 2) == 4
        assert client_broker.protocol.serial == 1

    with subtests.test("error"):
        with pytest.raises(ZeroDivisionError):
            assert await server_broker.call("div", 2, 0)


async def test_timeout(
    server_broker: TCPServerBroker,
    client_broker: TCPClientBroker,
    subtests,
):
    with pytest.raises(asyncio.TimeoutError):
        await server_broker.call("sleep", 0.5, timeout=0.1)
    # waiting for response
    await asyncio.sleep(1)
