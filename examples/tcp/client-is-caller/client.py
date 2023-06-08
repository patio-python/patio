import asyncio

from patio import Registry
from patio.broker.tcp import TCPClientBroker
from patio.executor import ThreadPoolExecutor


rpc = Registry(project="test", strict=True)


async def main():
    async with ThreadPoolExecutor(rpc) as executor:
        async with TCPClientBroker(executor) as broker:
            await broker.connect(address="127.0.0.1")
            await broker.connect(address="::1")
            print(
                await asyncio.gather(
                    *[
                        broker.call("mul", i, i) for i in range(10)
                    ]
                ),
            )


if __name__ == "__main__":
    asyncio.run(main())
