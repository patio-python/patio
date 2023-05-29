import asyncio
from functools import reduce

from patio import MemoryBroker, Registry
from patio.executor import ThreadPoolExecutor


rpc = Registry(project="test", strict=True)


def mul(*args):
    return reduce(lambda x, y: x * y, args)


async def main():
    rpc.register(mul, "mul")

    async with ThreadPoolExecutor(rpc) as executor:
        async with MemoryBroker(executor) as broker:
            print(
                await asyncio.gather(*[
                    broker.call(mul, i, i) for i in range(10)
                ]),
            )


if __name__ == "__main__":
    asyncio.run(main())
