import os
import json
import contextlib
import time
import tempfile
import atexit
import logging
from typing import Union, Callable, IO, Dict, Any, AnyStr, Optional
from pathlib import Path
from functools import wraps


logging.basicConfig(
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %h:%M:%S %p",
    format="%(asctime)s %(levelname)s: %(message)s")


class Files:
    """Files access for the cache."""

    _root: Path
    _data: Path
    _manifest: Path

    ROOT = "cache"
    DATA = "data"
    MANIFEST = "manifest.json"

    def __init__(self, root: Path):
        """Initialize a new file manager for a cache."""

        self._root = root.absolute()
        self._data = self._root.joinpath(self.DATA)
        self._manifest = self._root.joinpath(self.MANIFEST)

    @contextlib.contextmanager
    def manifest(self, mode: str="r") -> IO:
        """Return the opened manifest file."""

        try:
            with open(self._manifest, mode) as file:
                yield file
        except FileNotFoundError:
            logging.debug("no manifest file found!")
            self._root.mkdir(parents=True, exist_ok=True)
            with self._manifest.open("w") as file:
                json.dump({}, file)
            logging.debug("manifest created")
            with self._manifest.open(mode) as file:
                yield file

    @contextlib.contextmanager
    def data(self, name: str, mode: str="r") -> IO:
        """Return a file object from the cache."""

        path = self._data.joinpath(name)
        try:
            with open(path, mode) as file:
                yield file
        except FileNotFoundError:
            logging.debug("cache directory missing")
            self._data.mkdir(parents=True, exist_ok=True)
            with open(path, mode) as file:
                yield file

    def random(self) -> str:
        """Get a random unique file name in the cache directory."""

        return os.path.basename(tempfile.mktemp(dir=self._data, prefix=""))


class Entry:
    """Entry in a manifest file."""

    name: str
    data: Any
    created: float
    expiration: float

    def __init__(self, name: str=None, created: float=None, expiration: float=None, **kwargs):
        """Initialize a new entry with formed values.

        We don't assign the data attribute on initialization because
        we want to be able to represent None as an actual value that
        could be cached.
        """

        self.name = name
        self.created = created or time.time()
        self.expiration = expiration or 0

        # Check this way so that None is a valid value
        if "data" in kwargs:
            self.data = kwargs.pop("data")

    def dump(self) -> Dict[str, str]:
        """Dump an entry to JSON."""

        return {"name": self.name, "created": self.created, "expiration": self.expiration}

    @classmethod
    def load(cls, serialized: dict):
        """Load a entry object from a dictionary."""

        try:
            name = serialized["name"]
            created = float(serialized["created"])
            expiration = float(serialized["expiration"])
        except (KeyError, json.JSONDecodeError):
            raise SyntaxError

        return Entry(name, created, expiration)


class Manifest:
    """Manifest file wrapper."""

    _files: Files
    _manifest: Dict[str, Entry]

    def __init__(self, files: Files):
        """Initialize a manifest with the cache file manager."""

        self._files = files
        self._manifest = {}

    def _read(self) -> dict:
        """Read the manifest file."""

        try:
            with self._files.manifest() as file:
                data = json.load(file)
        except json.JSONDecodeError:
            data = self.reset()
        return data

    def _write(self, data: Dict[str, Entry]) -> dict:
        """Write to the manifest file."""

        with self._files.manifest("w") as file:
            json.dump(data, file)
        return data

    def get(self, key: str) -> Optional[Entry]:
        """Get a key from the manifest file."""

        result = self._read().get(key)
        if result is not None:
            result = Entry.load(result)
        return result

    def set(self, key: str, entry: Entry) -> Entry:
        """Set a key in the manifest."""

        data = self._read()
        data[key] = entry.dump()
        self._write(data)
        return entry

    def pop(self, key: str) -> Entry:
        """Remove a key and value from the manifest."""

        data = self._read()
        entry = Entry.load(data.pop(key))  # Maybe too heavy?
        self._write(data)
        return entry

    def reset(self) -> Dict[str, Entry]:
        """Reset the manifest."""

        return self._write({})


def call(obj: Any, *args, **kwargs) -> Any:
    """Call and return result if possible, otherwise return."""

    try:
        return obj(*args, **kwargs)
    except TypeError:
        return obj


def qualify(func) -> str:
    """Qualify a function."""

    return ".".join((func.__module__, func.__qualname__))


def _serialize(func, args: str):
    """Fully serialize a function."""

    return qualify(func) + "(" + args + ")"


def write(data: AnyStr, file: IO):
    file.write(data)


def read(file: IO) -> AnyStr:
    return file.read()


StringOrStringCallable = Union[Callable[..., str], str]


class Cache:
    """A cache object used to speed up access to resources."""

    def __init__(self, inside: Union[str, Path]=None, root: Union[str, Path]=None, sync: bool=True):
        """Initialize a new cache.

        The inside arguments specifies the directory in which the
        cache directory should be accessed or created. The root
        argument specifies the absolute location of cache directory.
        Root takes precedence over inside in the event that a user
        specifies both arguments, although this should not happen.

        The sync argument allows the user to specify whether the
        corresponding manifest file should be modified every time the
        manifest object in memory is. If sync is turned off, the
        manifest file is only modified at exit.
        """

        if type(inside) == str:
            inside = Path(inside)
        if type(root) == str:
            root = Path(root)

        # Check all permutations of inside and root
        if inside is None and root is None:
            inside = Path.cwd()
        if inside and root is None:
            root = inside.joinpath(Files.ROOT)

        self._files = Files(root)
        self._manifest = Manifest(self._files)
        self._cache = {}

    def __call__(self,
                 f: Callable=None,
                 *,
                 serialize: StringOrStringCallable=None,
                 file: StringOrStringCallable=None,
                 extension: StringOrStringCallable=None,
                 store: Callable[[Any, IO], Any]=None,
                 retrieve: Callable[[IO], Any]=None,
                 persist: bool=True,
                 expiration: float=None,
                 binary: bool=False) -> Callable:
        """Decorate a function and cache the return.

        This object primarily acts as a decorator, so to provide that
        functionality the call method is overwritten to modify the
        passed function to utilize the cache.

        The serialize and file commands should be of the form
        function(*args, **kwargs) and will receive the same arguments
        passed to the function.

        The store and retrieve functions should be of the form
        store(path, data) and retrieve(path) respectively, and will
        By default, a standard file write is used.

        The expiration argument should be used to set the number of
        seconds before the function will be re-invoked.

        Binary is the file mode that should be used to open the file
        before it is read from or written to. The w argument as
        opposed to the wb.
        """

        def decorator(func: Callable) -> Callable:
            """Return the configured decorator."""

            @wraps(func)
            def wrapper(*args, reload=False, **kwargs) -> Any:
                """Add options for memory and file system caching.

                First check if the function object is in the memory
                cache lookup. If not, check if persistence is enabled,
                and if so, check the manifest file to see if the
                qualified name is listed in it.
                """

                # Get a unique key from the function and arguments, check if in manifest
                arguments = "" if serialize is None else call(serialize, *args, **kwargs)
                key = _serialize(func, arguments)

                # Get the entry from memory of the manifest
                if not reload:
                    entry = None
                    if key in self._cache:
                        entry = self._cache[key]
                    if persist:
                        entry = self._manifest.get(key)

                    # If it is, get the data
                    if entry is not None:
                        logging.debug("found entry")

                        # Check if it has expired
                        if entry.expiration == 0 or time.time() - entry.created < entry.expiration:

                            # Try to get the data from the entry
                            try:
                                return entry.data
                            except AttributeError:
                                pass

                            # If we're persisting, check the file system
                            if persist:
                                try:
                                    entry.data = self.retrieve(entry.name, method=retrieve, binary=binary)
                                except (FileNotFoundError, Exception) as e:
                                    logging.debug("caught {} while retrieving data".format(e))
                                else:
                                    return entry.data

                        else:
                            logging.debug("data has expired")

                # Set the result and add the entry to the cache
                result = func(*args, **kwargs)
                self._cache[key] = entry = Entry(expiration=expiration, data=result)
                logging.debug("called function")

                if persist:

                    # Set a name for the entry and store it in the manifest
                    name = call(file, *args, **kwargs) if file else self._files.random()
                    entry.name = name + (extension or "")
                    self._manifest.set(key, entry)
                    logging.debug("add to manifest")

                    # Write to the file system
                    self.store(name, result, method=store, binary=binary)
                    logging.debug("stored results")

                return result

            return wrapper

        if f is not None:
            return decorator(f)
        return decorator

    def retrieve(self, name: str, method=None, binary: bool=False) -> object:
        """Read a file from the cache."""

        method = method or read
        with self._files.data(name, "rb" if binary else "r") as file:
            return method(file)

    def store(self, name: str, data, method=None, binary: bool=False):
        """Write data to a file in the cache."""

        method = method or write
        with self._files.data(name, "wb" if binary else "w") as file:
            method(data, file)
