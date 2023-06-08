from typing import Any, Awaitable, Callable

from patio.executor.base import AbstractExecutor
from patio.registry import T


class NullExecutor(AbstractExecutor):
    """
    Doesn't execute anything, serves as a stub to explicitly forbid calls
    to this registry.
    """

    async def setup(self) -> None:
        pass

    def submit(
        self, func: Callable[..., T], *args: Any, **kwargs: Any
    ) -> Awaitable[T]:
        raise RuntimeError("Null executor can't execute anything")

    async def shutdown(self) -> None:
        pass
