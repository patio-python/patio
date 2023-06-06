from __future__ import annotations

import builtins
import inspect
import pickle
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any, Iterator


class AbstractSerializer(ABC):
    @abstractmethod
    def pack(self, payload: Any) -> bytes:
        ...

    @abstractmethod
    def unpack(self, payload: bytes) -> Any:
        ...


class PickleSerializer(AbstractSerializer):
    def pack(self, payload: Any) -> bytes:
        return pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)

    def unpack(self, payload: bytes) -> Any:
        return pickle.loads(payload)


def _builtins_exceptions() -> Iterator[str]:
    for item in dir(builtins):
        obj = getattr(builtins, item)
        if not inspect.isclass(obj):
            continue
        if issubclass(obj, BaseException):
            yield f"builtins.{obj.__name__}"


class RestrictedUnpickler(pickle.Unpickler):
    SAFE_EXCLUDES = frozenset({
        "builtins.complex",
        "builtins.set",
        "builtins.frozenset",
        *_builtins_exceptions(),
    })

    UNSAFE_MODULES = frozenset(
        {"builtins", "os", "sys", "posix", "_winapi"},
    )

    def find_class(self, module: str, name: str) -> Any:
        if module not in self.UNSAFE_MODULES:
            return super().find_class(module, name)

        if f"{module}.{name}" in self.SAFE_EXCLUDES:
            return super().find_class(module, name)

        raise pickle.UnpicklingError(f"The '{module}.{name}' is forbidden")


class RestrictedPickleSerializer(PickleSerializer):
    def unpack(self, payload: bytes) -> Any:
        with BytesIO(payload) as fp:
            return RestrictedUnpickler(fp).load()
