import asyncio
from functools import reduce

from patio import Registry
from patio.broker.tcp import TCPClientBroker
from patio.executor import ThreadPoolExecutor


rpc = Registry(project="test", strict=True)


def mul(*args):
    return reduce(lambda x, y: x * y, args)


async def main():
    rpc.register(mul, "mul")

    async with ThreadPoolExecutor(rpc) as executor:
        async with TCPClientBroker(executor) as broker:
            await broker.connect(address='127.0.0.1')
            await broker.join()


if __name__ == "__main__":
    asyncio.run(main())
