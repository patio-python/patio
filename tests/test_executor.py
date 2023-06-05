import asyncio
from functools import reduce
from operator import mul
from typing import AsyncGenerator, Any

import pytest

from patio import Registry
from patio.executor import (
    AbstractExecutor, AsyncExecutor, NullExecutor, ProcessPoolExecutor,
    ThreadPoolExecutor,
)


class AsyncExecutorBaseCase:
    @staticmethod
    def multiply(*args: int) -> Any:
        raise NotImplementedError

    @pytest.fixture
    def registry(self) -> Registry:
        rpc: Registry = Registry()
        rpc["mul"] = self.multiply
        return rpc

    async def test_multiply(self, executor: AbstractExecutor):
        assert await executor.submit(self.multiply, 1, 2, 3) == 6

    async def test_multiply_gather(self, executor: AbstractExecutor):
        tasks = []

        for _ in range(100):
            tasks.append(executor.submit(self.multiply, 1, 2, 3))

        assert await asyncio.gather(*tasks) == [6] * 100


class TestAsyncExecutor(AsyncExecutorBaseCase):
    @pytest.fixture
    async def executor(
        self, registry: Registry
    ) -> AsyncGenerator[Any, AbstractExecutor]:
        async with AsyncExecutor(registry) as executor:
            yield executor

    @staticmethod
    async def multiply(*args: int) -> int:
        return reduce(mul, args)


class TestThreadPoolExecutor(AsyncExecutorBaseCase):
    @pytest.fixture
    async def executor(
        self, registry: Registry
    ) -> AsyncGenerator[Any, AbstractExecutor]:
        async with ThreadPoolExecutor(registry) as executor:
            yield executor

    @staticmethod
    def multiply(*args: int) -> int:
        return reduce(mul, args)


class TestProcessPoolExecutor(AsyncExecutorBaseCase):
    @pytest.fixture
    async def executor(
        self, registry: Registry
    ) -> AsyncGenerator[Any, AbstractExecutor]:
        async with ProcessPoolExecutor(registry) as executor:
            yield executor

    @staticmethod
    def multiply(*args: int) -> int:
        return reduce(mul, args)


class TestNullExecutor:
    @pytest.fixture
    def registry(self) -> Registry:
        rpc: Registry = Registry()
        return rpc

    @pytest.fixture
    async def executor(
        self, registry: Registry
    ) -> AsyncGenerator[Any, NullExecutor]:
        async with NullExecutor(registry) as executor:
            yield executor

    async def test_simple(self, executor: AbstractExecutor):
        with pytest.raises(RuntimeError):
            await executor.submit(print)
