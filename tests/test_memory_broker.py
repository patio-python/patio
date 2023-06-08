from functools import reduce
from operator import mul
from typing import Any, AsyncGenerator

import pytest

from patio import Registry
from patio.broker import MemoryBroker
from patio.executor import ThreadPoolExecutor


@pytest.fixture
def registry():
    r: Registry = Registry()

    @r("mul")
    def multiply(*args: int) -> int:
        return reduce(mul, args)

    return r


@pytest.fixture
async def broker(
    thread_executor: ThreadPoolExecutor,
) -> AsyncGenerator[Any, MemoryBroker]:
    async with MemoryBroker(thread_executor) as broker:
        yield broker


async def test_mul(broker: MemoryBroker):
    assert await broker.call("mul", 1, 2, 3) == 6
