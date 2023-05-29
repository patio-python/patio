from __future__ import annotations

import hashlib
import pickle
import threading
from dataclasses import dataclass
from enum import IntEnum, unique
from io import BytesIO
from random import getrandbits
from struct import Struct
from typing import Any, Optional, Tuple, Dict

from patio.broker import serializer, TimeoutType


@unique
class PacketTypes(IntEnum):
    AUTH_DIGEST = 0
    AUTH_REQUEST = 1
    AUTH_RESPONSE = 2
    AUTH_OK = 3
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


@dataclass
class CallRequest:
    func: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    timeout: TimeoutType


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

    def authorize_request(self, token: bytes) -> bytes:
        salt, digest = self.digest(token)
        return self.pack((salt, digest, token), PacketTypes.AUTH_REQUEST)

    def authorize_check(self, payload: bytes) -> bool:
        salt, digest, token = serializer.unpack(payload)
        return self.digest(token, salt=salt) == (salt, digest)

    def pack_error(self, exception: Exception, serial: int) -> bytes:
        return self.pack(exception, PacketTypes.ERROR, serial=serial)
