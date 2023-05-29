from functools import reduce
from operator import mul

import pytest

from patio import Registry
from patio.broker import MemoryBroker
from patio.executor import ThreadPoolExecutor


@pytest.fixture
def registry():
    rpc = Registry()

    @rpc("mul")
    def multiply(*args: int) -> int:
        return reduce(mul, args)

    return rpc


@pytest.fixture
async def executor(registry) -> ThreadPoolExecutor:
    async with ThreadPoolExecutor(registry) as executor:
        yield executor


@pytest.fixture
async def broker(registry, executor) -> MemoryBroker:
    async with MemoryBroker(executor) as broker:
        yield broker


async def test_mul(broker: MemoryBroker):
    assert await broker.call("mul", 1, 2, 3) == 6
