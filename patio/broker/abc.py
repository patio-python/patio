from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from patio.executor import AbstractExecutor
from patio.registry import TaskFunctionType


TimeoutType = Union[int, float]


class AbstractBroker(ABC):
    def __init__(self, executor: AbstractExecutor):
        self.executor = executor

    async def setup(self) -> None:
        await self.executor.setup()

    async def close(self) -> None:
        await self.executor.shutdown()

    @abstractmethod
    async def call(
        self,
        func: Union[str, TaskFunctionType],
        *args: Any,
        timeout: Optional[TimeoutType] = None,
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError(
            f"No way to call {func!r}: args={args!r}, "
            f"kwargs={kwargs!r}, timeout={timeout}s",
        )

    async def __aenter__(self) -> "AbstractBroker":
        await self.setup()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any,
    ) -> None:
        await self.close()


__all__ = "AbstractBroker", "TimeoutType", "TaskFunctionType"
