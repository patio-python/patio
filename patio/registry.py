import hashlib
import inspect
from base64 import b64encode
from collections import defaultdict
from types import MappingProxyType
from typing import (
    Any, Awaitable, Callable, DefaultDict, Dict, Generic, ItemsView, Iterator,
    KeysView, MutableMapping, Optional, Set, Tuple, TypeVar, Union, ValuesView,
    overload,
)


T = TypeVar("T")

AsyncTaskFunctionType = Callable[..., Awaitable[T]]
SyncTaskFunctionType = Callable[..., T]
TaskFunctionType = Union[AsyncTaskFunctionType, SyncTaskFunctionType]


class Registry(MutableMapping, Generic[T]):
    """
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

    .. code-block:: python

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

    Alternatively using ``register`` method:

    .. code-block:: python

        from patio import Registry

        rpc = Registry(project="example")

        def pow(a, b):
            return a ** b

        def sub(a, b):
            return a - b

        # Register with auto generated name
        rpc.register(pow)

        rpc.register(sub, "sub")

    Finally, you can register functions explicitly, as if it were
    just a dictionary:

    .. code-block:: python

        from patio import Registry

        rpc = Registry(project="example")

        def mul(a, b):
            return a * b

        rpc['mul'] = mul

    """

    __slots__ = (
        "__project", "__strict", "__store", "__reverse_store", "__locked",
    )

    def __init__(self, project: Optional[str] = None, strict: bool = False):
        self.__project = project
        self.__strict = strict
        self.__store: Dict[str, TaskFunctionType] = {}
        self.__reverse_store: DefaultDict[TaskFunctionType, Set[str]] = (
            defaultdict(set)
        )
        self.__locked = False

    @property
    def is_locked(self) -> bool:
        return self.__locked

    @property
    def project(self) -> Optional[str]:
        return self.__project

    @property
    def strict(self) -> bool:
        return self.__strict

    def _make_function_name(self, func: TaskFunctionType) -> str:
        parts = []

        if self.project is not None:
            parts.append(self.project)

        if hasattr(func, "__module__"):
            parts.append(func.__module__)

        if hasattr(func, "__name__"):
            parts.append(func.__name__)

        if self.strict:
            sources = inspect.getsource(func)
            parts.append(
                b64encode(
                    hashlib.blake2s(sources.encode()).digest(),
                ).strip(b"=").decode(),
            )

        return ".".join(parts)

    def lock(self) -> None:
        if self.is_locked:
            return
        self.__store = MappingProxyType(self.__store)
        self.__reverse_store = MappingProxyType({
            k: frozenset(v) for k, v in self.__reverse_store.items()
        })
        self.__locked = True

    def register(
        self, func: TaskFunctionType, name: Optional[str] = None,
    ) -> str:
        if name is None:
            name = self._make_function_name(func)
        self[name] = func
        return name

    @overload
    def __call__(self, name: TaskFunctionType) -> TaskFunctionType: ...

    @overload
    def __call__(
        self, name: Optional[str] = None,
    ) -> Callable[..., TaskFunctionType]: ...

    def __call__(
        self, name: Optional[str] = None,
    ) -> Union[Callable[..., TaskFunctionType], TaskFunctionType]:
        if callable(name):
            return self.__call__(None)(name)

        def decorator(func) -> TaskFunctionType:
            self.register(func, name)
            return func
        return decorator

    def __setitem__(self, name: str, func: TaskFunctionType) -> None:
        if name in self.__store:
            raise RuntimeError(
                f"Task with name {name!r} already "
                f"registered for {self.__store[name]!r}",
            )
        self.__store[name] = func
        self.__reverse_store[func] = name

    def __delitem__(self, name: str) -> None:
        func = self.__store.pop(name)
        del self.__reverse_store[func]

    def __getitem__(self, name: str) -> TaskFunctionType:
        return self.__store[name]

    def __len__(self) -> int:
        return len(self.__store)

    def __iter__(self) -> Iterator[str]:
        return iter(self.__store)

    def items(self) -> ItemsView[str, TaskFunctionType]:
        return self.__store.items()

    def keys(self) -> KeysView[str]:
        return self.__store.keys()

    def values(self) -> ValuesView[TaskFunctionType]:
        return self.__store.values()

    def get_names(self, func: TaskFunctionType) -> Tuple[str, ...]:
        return tuple(self.__reverse_store[func])

    def get_name(self, func: TaskFunctionType) -> str:
        candidates = self.get_names(func)
        if not candidates:
            raise KeyError(f"{func!r} has not been registered")
        return candidates[0]

    @overload
    def resolve(self, func: str) -> Callable[..., T]: ...

    @overload
    def resolve(self, func: Callable[..., T]) -> Callable[..., T]: ...

    def resolve(
        self, func: Union[str, Callable[..., T]],
    ) -> Callable[..., Any]:
        if not isinstance(func, str):
            func = self.get_name(func)
        return self[func]


__all__ = (
    "AsyncTaskFunctionType",
    "Registry",
    "SyncTaskFunctionType",
    "T",
    "TaskFunctionType",
)
