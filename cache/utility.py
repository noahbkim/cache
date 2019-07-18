import itertools
from typing import Any, AnyStr, IO


def call(obj: Any, *args, **kwargs) -> Any:
    """Call and return result if possible, otherwise return."""

    if callable(obj):
        return obj(*args, **kwargs)
    return obj


def qualify(func: object) -> str:
    """Qualify a function."""

    return ".".join((func.__module__, func.__qualname__))


def serialize(*args, **kwargs) -> str:
    """Serialize function arguments."""

    return ", ".join(itertools.chain(map(repr, args), (f"{key}={repr(value)}" for key, value in kwargs.items())))


def write(data: AnyStr, file: IO):
    file.write(data)


def read(file: IO) -> AnyStr:
    return file.read()
