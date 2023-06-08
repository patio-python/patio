import asyncio
import concurrent.futures
from functools import cached_property, partial
from typing import Any, Awaitable

from patio.executor.base import AbstractExecutor
from patio.registry import SyncTaskFunctionType, T


class ProcessPoolExecutor(AbstractExecutor[T]):
    """
    Execute jobs in the process pool. Jobs cannot be asynchronous functions.
    This means that the whole registry must not contain functions other than
    specified kind.
    """
    __slots__ = "max_workers", "executor"

    DEFAULT_MAX_WORKERS: int = 4

    executor: concurrent.futures.ProcessPoolExecutor

    @cached_property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    async def setup(self) -> None:
        if hasattr(self, "executor"):
            return

        self.executor = await self.loop.run_in_executor(
            None, concurrent.futures.ProcessPoolExecutor, self.max_workers,
        )

    def submit(
        self, func: SyncTaskFunctionType, *args: Any, **kwargs: Any
    ) -> Awaitable[T]:
        return self.loop.run_in_executor(
            self.executor, partial(func, *args, **kwargs),
        )

    async def shutdown(self) -> None:
        await self.loop.run_in_executor(None, self.executor.shutdown, True)
