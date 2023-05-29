import asyncio
from functools import reduce
from operator import mul

import pytest

from patio import Registry
from patio.executor import (
    AbstractExecutor, AsyncAbstractExecutor, NullExecutor, ProcessPoolExecutor,
    ThreadPoolExecutor,
)


class TestAsyncExecutor:
    @staticmethod
    async def multiply(*args: int) -> int:
        return reduce(mul, args)

    @pytest.fixture
    def registry(self) -> Registry:
        rpc = Registry()
        rpc["mul"] = self.multiply
        return rpc

    @pytest.fixture
    async def executor(self, registry) -> AbstractExecutor:
        async with AsyncAbstractExecutor(registry) as executor:
            yield executor

    async def test_multiply(self, executor: AbstractExecutor):
        assert await executor.submit(self.multiply, 1, 2, 3) == 6

    async def test_multiply_gather(self, executor: AbstractExecutor):
        tasks = []

        for _ in range(100):
            tasks.append(executor.submit(self.multiply, 1, 2, 3))

        assert await asyncio.gather(*tasks) == [6] * 100


class TestThreadPoolExecutor(TestAsyncExecutor):
    @pytest.fixture
    async def executor(self, registry) -> AbstractExecutor:
        async with ThreadPoolExecutor(registry) as executor:
            yield executor

    @staticmethod
    def multiply(*args: int) -> int:
        return reduce(mul, args)


class TestProcessPoolExecutor(TestAsyncExecutor):
    @pytest.fixture
    async def executor(self, registry) -> AbstractExecutor:
        async with ProcessPoolExecutor(registry) as executor:
            yield executor

    @staticmethod
    def multiply(*args: int) -> int:
        return reduce(mul, args)


class TestNullExecutor:
    @pytest.fixture
    def registry(self) -> Registry:
        rpc = Registry()
        return rpc

    @pytest.fixture
    async def executor(self, registry) -> NullExecutor:
        async with NullExecutor(registry) as executor:
            yield executor

    async def test_simple(self, executor):
        with pytest.raises(RuntimeError):
            await executor.submit(print)
