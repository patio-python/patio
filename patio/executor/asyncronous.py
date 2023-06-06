import asyncio
from functools import cached_property
from typing import Any, Callable, Set, Tuple

from patio.compat import Queue
from patio.executor.base import AbstractExecutor
from patio.registry import AsyncTaskFunctionType, T


QueueType = Queue[Tuple[Callable[..., T], Any, Any, asyncio.Future]]


class AsyncExecutor(AbstractExecutor[T]):
    """
    Executes the incoming tasks in the pool of the several concurrent tasks.
    Tasks must be asynchronous functions, or functions that return an
    awaitable object.
    This means that the whole registry must not contain functions other than
    specified kind.
    """
    __slots__ = "max_workers", "queue", "tasks"

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.queue: QueueType = Queue(maxsize=self.max_workers)
        self.tasks: Set[asyncio.Task] = set()
        self.started: bool = False

    @cached_property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    async def _executor(self) -> None:
        while True:
            func, args, kwargs, future = await self.queue.get()
            if future.done():
                continue
            try:
                future.set_result(await func(*args, **kwargs))
            except Exception as e:
                if future.done():
                    continue
                future.set_exception(e)

    async def setup(self) -> None:
        if self.started:
            return

        for _ in range(self.max_workers):
            self.tasks.add(asyncio.create_task(self._executor()))
        self.started = True

    async def submit(
        self, func: AsyncTaskFunctionType, *args: Any, **kwargs: Any
    ) -> T:
        future = self.loop.create_future()
        await self.queue.put((func, args, kwargs, future))
        return await future

    async def shutdown(self) -> None:
        cancelled = []
        for task in self.tasks:
            if task.done():
                continue
            task.cancel()
            cancelled.append(task)

        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)
