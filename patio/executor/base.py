from __future__ import annotations

from abc import abstractmethod
from typing import Any, AsyncContextManager, Awaitable, Generic, Union

from patio.registry import Registry, T, TaskFunctionType


class AbstractExecutor(Generic[T], AsyncContextManager):
    """
    An Executor is an entity that executes local functions on the local side.
    The following executors are implemented in the package:

    * :class:`AsyncExecutor`
    * :class:`ThreadPoolExecutor`
    * :class:`ProcessPoolExecutor`
    * :class:`NullExecutor`

    Its role is to reliably execute jobs without taking too much so as not to
    cause a denial of service, or excessive memory consumption.

    The executor instance is passing to the broker, it's usually applies
    it to the whole registry. Therefore, you should understand what functions
    the registry must contain to choose kind of an executor.
    """

    DEFAULT_MAX_WORKERS: int = 4

    def __init__(
        self, registry: Registry, max_workers: int = DEFAULT_MAX_WORKERS,
    ):
        self.registry = registry
        self.max_workers = max_workers

    @abstractmethod
    async def setup(self) -> None:
        """
        Configures the executor, can be called several times, with the
        assumption that it will be configured exactly once.
        """
        raise NotImplementedError(
            f"Not implemented method setup "
            f"in {self.__class__.__name__!r} ",
        )

    @abstractmethod
    def submit(
        self, func: TaskFunctionType, *args: Any, **kwargs: Any
    ) -> Awaitable[T]:
        """
        Passes the function to execute, and waits for the result, returns it.

        :param func: Function to be executed
        :param args: positional arguments of the Function
        :param kwargs: keyword arguments of the function
        :return: the result that the function will return
        """
        raise NotImplementedError(
            f"Not implemented method submit in {self.__class__.__name__!r} "
            f"Call {func!r} with args={args!r}, kwargs={kwargs!r} skipped",
        )

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Performs an executor stop. All related and unperformed tasks
        must be completed.
        """

    async def __aenter__(self) -> AbstractExecutor:
        await self.setup()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()

    async def execute(
        self, func: Union[str, TaskFunctionType], *args: Any, **kwargs: Any
    ) -> T:
        func = self.registry.resolve(func)
        return await self.submit(func, *args, ** kwargs)
