from __future__ import annotations

import pickle
from io import BytesIO
from typing import Any


class RestrictedUnpickler(pickle.Unpickler):
    SAFE_CLASSES = frozenset({
        "builtins.complex",
        "builtins.set",
        "builtins.frozenset",
        "builtins.frozenset",
        "patio.broker.tcp.protocol.CallRequest",
    })

    def find_class(self, module: str, name: str) -> Any:
        if f"{module}.{name}" in self.SAFE_CLASSES:
            return super().find_class(module, name)
        raise pickle.UnpicklingError(f"global '{module}.{name}' is forbidden")


def pack(payload: Any) -> bytes:
    return pickle.dumps(payload)


def unpack(data: bytes) -> Any:
    with BytesIO(data) as fp:
        return RestrictedUnpickler(fp).load()
