import asyncio
from typing import Any, Optional, Union

from patio.broker.abc import AbstractBroker, TimeoutType
from patio.registry import TaskFunctionType


class MemoryBroker(AbstractBroker):
    async def call(
        self,
        func: Union[str, TaskFunctionType],
        *args: Any,
        timeout: Optional[TimeoutType] = 86400,
        **kwargs: Any,
    ) -> Any:
        return await asyncio.wait_for(
            self.executor.execute(func, *args, **kwargs),
            timeout=timeout,
        )


__all__ = "MemoryBroker",
