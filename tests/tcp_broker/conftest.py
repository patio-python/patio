import ssl
from pathlib import Path
from typing import Any, AsyncGenerator, Optional, Tuple

import pytest

from patio import TCPClientBroker, TCPServerBroker
from patio.broker.serializer import (
    AbstractSerializer, PickleSerializer, RestrictedPickleSerializer,
)
from patio.executor import ThreadPoolExecutor


SSL_PATH = Path(__file__).parent / "ssl"
SSL_CA_FILE = SSL_PATH / "CA.pem"
SSL_SERVER_CERT_FILE = SSL_PATH / "server.pem"
SSL_CLIENT_CERT_FILE = SSL_PATH / "client.pem"
SSL_SERVER_KEY_FILE = SSL_PATH / "server.key"
SSL_CLIENT_KEY_FILE = SSL_PATH / "client.key"


def ssl_server_context(
    mode=ssl.VerifyMode.CERT_OPTIONAL,
) -> ssl.SSLContext:
    ctx = ssl.SSLContext()
    ctx.verify_mode = mode
    ctx.load_verify_locations(SSL_CA_FILE)
    ctx.load_cert_chain(SSL_SERVER_CERT_FILE, SSL_SERVER_KEY_FILE)
    return ctx


def ssl_client_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext()
    ctx.load_cert_chain(SSL_CLIENT_CERT_FILE, SSL_CLIENT_KEY_FILE)
    return ctx


@pytest.fixture()
def server_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


@pytest.fixture(
    params=[
        [
            ssl_server_context(ssl.VerifyMode.CERT_OPTIONAL),
            ssl.create_default_context(cafile=SSL_CA_FILE),
        ],
        [
            ssl_server_context(ssl.VerifyMode.CERT_REQUIRED),
            ssl_client_context(),
        ],
        [None, None],
    ],
    ids=["ssl", "ssl-client", "no-ssl"],
)
def ssl_context_pair(request) -> Tuple[
    Optional[ssl.SSLContext],
    Optional[ssl.SSLContext],
]:
    return request.param


@pytest.fixture(
    params=[
        PickleSerializer(),
        RestrictedPickleSerializer(),
    ],
    ids=["pickle", "restricted-pickle"],
)
def serializer(request) -> AbstractSerializer:
    return request.param


@pytest.fixture()
async def server_broker(
    server_port: int, thread_executor: ThreadPoolExecutor,
    localhost: str, ssl_context_pair, serializer: AbstractSerializer,
) -> AsyncGenerator[Any, TCPServerBroker]:
    ctx, _ = ssl_context_pair
    async with TCPServerBroker(
        thread_executor, ssl_context=ctx, serializer=serializer,
    ) as broker:
        await broker.listen(localhost, server_port)
        yield broker


@pytest.fixture()
async def client_broker(
    server_port: int, thread_executor: ThreadPoolExecutor,
    server_broker: TCPServerBroker, localhost: str, ssl_context_pair,
    serializer: AbstractSerializer,
) -> AsyncGenerator[Any, TCPClientBroker]:
    _, ctx = ssl_context_pair
    async with TCPClientBroker(
        thread_executor, ssl_context=ctx, serializer=serializer,
    ) as broker:
        await broker.connect(localhost, server_port)
        yield broker
