from functools import reduce

import aiomisc

from patio import Registry
from patio.broker.tcp import Client
from patio.executor import ThreadPoolExecutor


rpc = Registry(project="test", strict=True)


def mul(*args):
    return reduce(lambda x, y: x * y, args)


async def main():
    rpc.register(mul, "mul")

    async with ThreadPoolExecutor(rpc) as executor:
        async with Client(executor) as broker:
            await broker.connect(address='127.0.0.1')
            await broker.join()


if __name__ == "__main__":
    aiomisc.run(main())
