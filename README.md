PATIO
=====

PATIO is an acronym for **P**ython **A**synchronous **T**asks for Async**IO**.


Quickstart
----------

```python
import asyncio
from functools import reduce

from patio import Registry
from patio.broker import MemoryBroker
from patio.executor import ThreadPoolExecutor


rpc = Registry()


@rpc("mul")
def multiply(*args: int) -> int:
    return reduce(lambda x, y: x * y, args)


async def main():
    async with ThreadPoolExecutor(rpc) as executor:
        async with MemoryBroker(executor) as broker:
            print(
                await asyncio.gather(
                    *[broker.call("mul", 1, 2, 3) for _ in range(100)]
                )
            )


if __name__ == '__main__':
    asyncio.run(main())
```


The main concepts
-----------------

The main idea in creating this library was to create a maximally
modular and extensible system that can be expanded with third-party
integrations or directly in the user's code.

The basic elements from which everything is built are:

* `Registry` - Key-Value like store for the functions
* `Exectutor` - The thing which execute functions from the registry
* `Broker` - The actor who distribues tasks in your distributed (or local) system.

Registry
--------

This is a container of functions for their subsequent execution.
You can register a function by specific name or without it,
in which case the function is assigned a unique name that depends on
the source code of the function.

This registry does not necessarily have to match on the calling and called
sides, but for functions that you register without a name it is must be,
and then you should not need to pass the function name but the function
itself when you will call it.

An instance of the registry must be transferred to the broker,
the first broker in the process of setting up will block the registry
to write, that is, registering new functions will be impossible.

An optional ``project`` parameter, this is essentially like a namespace
that will help avoid clash functions in different projects with the same
name. It is recommended to specify it and the broker should also use this
parameter, so it should be the same value within the same project.

You can either manually register elements or use a
registry instance as a decorator:

```python
from patio import Registry

rpc = Registry(project="example")

# Will be registered with auto generated name
@rpc
def mul(a, b):
    return a * b

@rpc('div')
def div(a, b):
    return a / b

def pow(a, b):
    return a ** b

def sub(a, b):
    return a - b

# Register with auto generated name
rpc.register(pow)

rpc.register(sub, "sub")
```

Alternatively using ``register`` method:

```python
from patio import Registry

rpc = Registry(project="example")

def pow(a, b):
    return a ** b

def sub(a, b):
    return a - b

# Register with auto generated name
rpc.register(pow)

rpc.register(sub, "sub")
```

Finally, you can register functions explicitly, as if it were
just a dictionary:

```python
from patio import Registry

rpc = Registry(project="example")

def mul(a, b):
    return a * b

rpc['mul'] = mul
```

Executor
--------

An Executor is an entity that executes local functions from registry.
The following executors are implemented in the package:

* `AsyncExecutor` - Implements pool of asyncronous tasks
* `ThreadPoolExecutor` - Implements pool of threads
* `ProcessPoolExecutor` - Implements pool of processes
* `NullExecutor` - Implements nothing and exists just for forbid execute
  anything explicitly.

Its role is to reliably execute jobs without taking too much so as not to
cause a denial of service, or excessive memory consumption.

The executor instance is passing to the broker, it's usually applies
it to the whole registry. Therefore, you should understand what functions
the registry must contain to choose kind of an executor.

Broker
------

The basic approach for distributing tasks is to shift the responsibility
for the distribution to the user implementation. In this way, task
distribution can be implemented through third-party brokers, databases,
or something else.

This package is implemented by the following brokers:

* `MemoryBroker` - To distribute tasks within a single process.
   A very simple implementation, in case you don't know yet how
   your application will develop and just want to leave the decision
   of which broker to use for later, while laying the foundation for
   switching to another broker.
* `TCPBroker` - Simple implementation of the broker just using TCP,
   both Server and Client mode is supported for both the task executor
   and the task provider.
