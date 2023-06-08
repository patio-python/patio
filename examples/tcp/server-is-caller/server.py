import asyncio

from patio import Registry
from patio.broker.tcp import TCPServerBroker
from patio.executor import ThreadPoolExecutor


rpc = Registry(project="test", strict=True)


async def main():
    async with ThreadPoolExecutor(rpc) as executor:
        async with TCPServerBroker(executor) as broker:
            await broker.listen(address='127.0.0.1')
            while True:
                print(
                    await asyncio.gather(*[
                        broker.call('mul', i, i) for i in range(10)
                    ], return_exceptions=True),
                )
                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
