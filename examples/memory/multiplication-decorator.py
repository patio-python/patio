import asyncio
from functools import reduce

from patio import MemoryBroker, Registry, AsyncExecutor


rpc = Registry(project="test", strict=True)


@rpc
async def mul(*args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, reduce, lambda x, y: x * y, args)


async def main():
    async with AsyncExecutor(max_workers=50) as executor:
        async with MemoryBroker(executor) as broker:
            await broker.setup(registry=rpc)

            print(
                await asyncio.gather(*[
                    broker.call(mul, i, i) for i in range(10)
                ]),
            )


if __name__ == "__main__":
    asyncio.run(main())
