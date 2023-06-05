from functools import reduce
from operator import mul
from typing import Any, AsyncGenerator

import pytest

from patio import Registry
from patio.broker import MemoryBroker
from patio.executor import AbstractExecutor, ThreadPoolExecutor


@pytest.fixture
def registry():
    rpc: Registry = Registry()

    @rpc("mul")
    def multiply(*args: int) -> int:
        return reduce(mul, args)

    return rpc


@pytest.fixture()
async def executor(
    registry: Registry,
) -> AsyncGenerator[Any, ThreadPoolExecutor]:
    async with ThreadPoolExecutor(registry) as executor:
        yield executor


@pytest.fixture
async def broker(
    registry: Registry, executor: AbstractExecutor,
) -> AsyncGenerator[Any, MemoryBroker]:
    async with MemoryBroker(executor) as broker:
        yield broker


async def test_mul(broker: MemoryBroker):
    assert await broker.call("mul", 1, 2, 3) == 6
