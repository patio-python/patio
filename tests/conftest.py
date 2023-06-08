from typing import Any, AsyncGenerator

import pytest

from patio import Registry
from patio.executor import ThreadPoolExecutor


@pytest.fixture
def registry():
    return Registry()


@pytest.fixture()
async def thread_executor(
    registry: Registry,
) -> AsyncGenerator[Any, ThreadPoolExecutor]:
    async with ThreadPoolExecutor(registry) as executor:
        yield executor
