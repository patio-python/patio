from __future__ import annotations

import hashlib
import pickle
import threading
from dataclasses import dataclass
from enum import IntEnum, unique
from io import BytesIO
from random import getrandbits
from struct import Struct
from typing import Any, Optional, Tuple

from patio import TaskFunctionType


@unique
class PacketTypes(IntEnum):
    AUTH_REQUEST = 0
    AUTH_RESPONSE = 1
    REQUEST = 10
    RESPONSE = 20
    ERROR = 30


@dataclass
class Header:
    STRUCT = Struct("!bII")
    SIZE = STRUCT.size

    type: PacketTypes
    size: int
    serial: int

    def pack(self) -> bytes:
        return self.STRUCT.pack(self.type.value, self.size, self.serial)

    @classmethod
    def unpack(cls, data: bytes) -> Header:
        kind, size, serial = cls.STRUCT.unpack(data)
        return cls(type=PacketTypes(kind), size=size, serial=serial)


class RestrictedUnpickler(pickle.Unpickler):
    SAFE_BUILTINS = frozenset({
        "range", "complex", "set", "frozenset", "slice",
    })

    def find_class(self, module, name):
        if module == "builtins" and name in self.SAFE_BUILTINS:
            return getattr(builtins, name)
        raise pickle.UnpicklingError(f"global '{module}.{name}' is forbidden")


class Protocol:
    HEADER_STRUCT = Struct("!bII")
    MAX_SERIAL = 2 << 31 - 1

    __slots__ = "__key", "serial", "lock"

    def __init__(self, key: bytes = b""):
        self.__key = key
        self.serial = 0
        self.lock = threading.Lock()

    def get_serial(self) -> int:
        with self.lock:
            self.serial += 1

            if self.serial > self.MAX_SERIAL:
                self.serial = 0

            return self.serial

    def digest(
        self, data: bytes, *, salt: Optional[bytes] = None
    ) -> Tuple[bytes, bytes]:
        if salt is None:
            salt = getrandbits(32).to_bytes(4, "big")
        return salt, hashlib.blake2s(data, key=self.__key, salt=salt).digest()

    def pack(
        self, payload: Any, packet_type: PacketTypes,
        *, serial: Optional[int] = None
    ) -> bytes:
        with BytesIO() as fp:
            fp.seek(Header.SIZE)
            pickle.dump(payload, fp)

            header = Header(
                type=packet_type,
                size=fp.tell() - self.HEADER_STRUCT.size,
                serial=serial or self.get_serial(),
            )

            fp.seek(0)
            fp.write(header.pack())

            return fp.getvalue()

    @staticmethod
    def unpack(data: bytes) -> Any:
        with BytesIO(data) as fp:
            return RestrictedUnpickler(fp).load()

    def authorize_request(self, token: bytes) -> bytes:
        return self.pack(self.digest(token), PacketTypes.AUTH_REQUEST)

    def authorize_check(
        self, token: bytes, payload: bytes,
    ) -> bool:
        salt, digest = self.unpack(payload)
        return self.digest(token, salt=salt) == digest

    def pack_request(
        self, func: TaskFunctionType, *args: Any, **kwargs: Any
    ) -> bytes:
        return self.pack((func, args, kwargs), PacketTypes.REQUEST)

    def pack_response(self, result: Any, serial: int) -> bytes:
        return self.pack(result, PacketTypes.REQUEST, serial=serial)

    def pack_error(self, exception: Exception, serial: int) -> bytes:
        return self.pack(exception, PacketTypes.ERROR, serial=serial)
