import asyncio

from patio import Registry
from patio.broker.tcp import Client
from patio.executor import ThreadPoolExecutor


rpc = Registry(project="test", strict=True)


async def main():
    async with ThreadPoolExecutor(rpc) as executor:
        async with Client(executor, address='127.0.0.1') as broker:
            print(
                await asyncio.gather(*[
                    broker.call('mul', i, i) for i in range(10)
                ]),
            )


if __name__ == "__main__":
    asyncio.run(main())
