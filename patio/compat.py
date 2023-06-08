import asyncio
import sys
from typing import Generic, TypeVar


try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self


T = TypeVar("T")


if sys.version_info >= (3, 9):
    from asyncio import Queue
else:
    class Queue(asyncio.Queue, Generic[T]):
        async def get(self) -> T:
            return await super().get()

        async def put(self, element: T) -> T:
            return await super().put(element)

        def get_nowait(self) -> T:
            return super().get_nowait()

        def put_nowait(self, element: T) -> None:
            return super().put_nowait(element)


__all__ = ("Queue", "Self")
