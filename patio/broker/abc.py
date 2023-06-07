import asyncio
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any, Awaitable, Coroutine, List, Optional, Set, Union

from patio.compat import Self
from patio.executor import AbstractExecutor
from patio.registry import TaskFunctionType


TimeoutType = Union[int, float]


class AbstractBroker(ABC):
    def __init__(self, executor: AbstractExecutor):
        self.executor = executor
        self.__tasks: Set[asyncio.Task] = set()

    @cached_property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def create_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
        task = self.loop.create_task(coro)
        self.__tasks.add(task)
        task.add_done_callback(self.__tasks.discard)
        return task

    async def setup(self) -> None:
        await self.executor.setup()

    async def close(self) -> None:
        tasks: List[Awaitable[Any]] = [self.executor.shutdown()]
        for task in tuple(self.__tasks):
            if task.done():
                continue
            task.cancel()
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    @abstractmethod
    async def call(
        self,
        func: Union[str, TaskFunctionType],
        *args: Any,
        timeout: Optional[TimeoutType] = None,
        **kwargs: Any,
    ) -> Any:
        ...

    async def __aenter__(self) -> Self:
        await self.setup()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any,
    ) -> None:
        await self.close()

    async def join(self) -> None:
        async def waiter() -> None:
            await self.loop.create_future()
        await self.create_task(waiter())


__all__ = "AbstractBroker", "TimeoutType", "TaskFunctionType"
