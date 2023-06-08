import os
import pickle

import pytest

from patio import Registry, TCPClientBroker, TCPServerBroker
from patio.broker.serializer import (
    AbstractSerializer, RestrictedPickleSerializer,
)


@pytest.fixture(params=[RestrictedPickleSerializer()], ids=["restricted"])
def serializer(request) -> AbstractSerializer:
    return request.param


@pytest.fixture
def registry():
    rpc: Registry = Registry()

    @rpc("call_any")
    def call_any(func, *args):
        return func(*args)

    return rpc


async def test_restricted(
    server_broker: TCPServerBroker,
    client_broker: TCPClientBroker,
    subtests,
):
    with pytest.raises(pickle.UnpicklingError):
        assert await client_broker.call(
            "call_any", os.execv, "ls", ["ls", "-1"],
        )
