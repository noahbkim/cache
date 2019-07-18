import os
import json
import contextlib
import time
import tempfile
import logging
import atexit
import multiprocessing
from dataclasses import dataclass
from typing import Union, Callable, IO, Dict, Any, Optional
from pathlib import Path
from functools import wraps

from . import utility

__all__ = ("Cache",)

logging.basicConfig(
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %h:%M:%S %p",
    format="%(asctime)s %(levelname)s: %(message)s")

NONE = object()

ClosedOptional = Any  # No good way to annotate, must be Any and not NONE
PathLike = Union[Path, str]
StringOrStringCallable = Union[Callable[..., str], str]
Reader = Callable[[IO], Any]
Writer = Callable[[Any, IO], None]


class Files:
    """Files access for the cache."""

    _root: Path
    _data: Path
    _manifest: Path

    ROOT = "cached"
    DATA = "data"
    MANIFEST = "manifest.json"

    def __init__(self, root: Path):
        """Initialize a new file manager for a cache."""

        self._root = root.absolute()
        self._data = self._root.joinpath(self.DATA)
        self._manifest = self._root.joinpath(self.MANIFEST)

    @contextlib.contextmanager
    def manifest(self, mode: str = "r") -> IO:
        """Return the opened manifest file."""

        try:
            with open(str(self._manifest), mode) as file:
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
    def data(self, name: str, mode: str = "r") -> IO:
        """Return a file object from the cache."""

        path = self._data.joinpath(name)
        try:
            with open(str(path), mode) as file:
                yield file
        except FileNotFoundError:
            logging.debug("cache directory missing")
            self._data.mkdir(parents=True, exist_ok=True)
            with open(str(path), mode) as file:
                yield file

    def random(self, extension: str = "") -> str:
        """Get a random unique file name in the cache directory."""

        return os.path.basename(tempfile.mktemp(dir=self._data, prefix="", suffix=extension))

    def empty(self):
        """Delete all associated files."""

        import shutil
        shutil.rmtree(str(self._root))


@dataclass
class Entry:
    """Entry in a manifest file."""

    name: Optional[str]
    expiration: Optional[float]
    created: float

    data: ClosedOptional

    def __init__(self, name: str = None, expiration: float = None, created: float = None, data: Any = NONE):
        """Initialize a new entry with formed values.

        The private object NONE is used to represent the absence of
        data in the entry, as None should be a distinct option.
        """

        self.name = name
        self.expiration = expiration
        self.created = created or time.time()
        self.data = data

    def dump(self) -> Dict[str, Any]:
        """Dump an entry to JSON."""

        return {"name": self.name, "created": self.created, "expiration": self.expiration}

    @classmethod
    def load(cls, serialized: dict) -> "Entry":
        """Load a entry object from a dictionary."""

        name = serialized["name"]
        created = float(serialized["created"])
        expiration = serialized["expiration"] and float(serialized["expiration"])
        return Entry(name=name, expiration=expiration, created=created)


class Manifest:
    """Manifest file wrapper."""

    _files: Files
    _manifest: Dict[str, Entry]
    _lock: multiprocessing.Lock

    def __init__(self, files: Files):
        """Initialize a manifest with the cache file manager.

        The sync argument allows the user to specify whether the
        corresponding manifest file should be modified every time the
        manifest object in memory is. If sync is turned off, the
        manifest file is only modified at exit.
        """

        self._files = files
        self._manifest = {}
        self._lock = multiprocessing.Lock()
        self.read()

    def read(self):
        """Read the manifest file."""

        self._manifest.clear()
        try:
            with self._files.manifest() as file:
                data = json.load(file)
        except json.JSONDecodeError:
            logging.error("invalidly formatted manifest!")
            return

        for key, value in data.items():
            try:
                self._manifest[key] = Entry.load(value)
            except (KeyError, json.JSONDecodeError):
                logging.error("attempted to deserialize invalid entry!")
                self._manifest.clear()

    def write(self):
        """Write to the manifest file."""

        with self._files.manifest("w") as file:
            json.dump({k: v.dump() for k, v in self._manifest.items()}, file)

    def get(self, key: str) -> Optional[Entry]:
        """Get a key from the manifest file."""

        return self._manifest.get(key)

    def set(self, key: str, entry: Entry) -> Entry:
        """Set a key in the manifest."""

        with self._lock:
            self._manifest[key] = entry
            return entry

    def pop(self, key: str) -> Entry:
        """Remove a key and value from the manifest."""

        with self._lock:
            return self._manifest.pop(key)  # Maybe too heavy?

    def clear(self):
        """Clear the manifest."""

        with self._lock:
            self._manifest.clear()


class Cache:
    """A cache object used to speed up access to resources."""

    _files: Files
    _manifest: Manifest
    _cache: Dict[str, Entry]
    _persist: bool

    def __init__(self, inside: PathLike = None, root: PathLike = None):
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
        self._persist = False

        atexit.register(self.persist)

    def __call__(
            self,
            f: Callable = None,
            *,
            serialize: StringOrStringCallable = utility.serialize,
            file: StringOrStringCallable = None,
            extension: StringOrStringCallable = "",
            store: Callable[[Any, IO], Any] = utility.write,
            retrieve: Callable[[IO], Any] = utility.read,
            persist: bool = True,
            expiration: float = None,
            binary: bool = False) -> Callable:
        """Decorate a function and cache the return.

        This object primarily acts as a decorator, so to provide that
        functionality the call method is overwritten to modify the
        passed function to utilize the cache.

        The serialize and file functions should be a string or of the
        form function(*args, **kwargs) and will receive the same
        arguments passed to the function. Note that the default
        serialization will not work for objects that don't implement
        a stable __repr__.

        The store and retrieve functions should be of the form
        store(path, data) and retrieve(path) respectively, and will
        By default, a standard file write is used.

        The expiration argument should be used to set the number of
        seconds before the function will be re-invoked.

        Binary is the file mode that should be used to open the file
        before it is read from or written to. The w argument as
        opposed to the wb.

        :parameter f: decorating function.
        :parameter serialize: serializer for function arguments.
        :parameter file: cache file name.
        :parameter extension: cache file extension.
        :parameter store: method for writing an object to a file.
        :parameter retrieve: method for reading an object from a file.
        :parameter persist: whether to store in the file system.
        :parameter expiration: seconds to expiration.
        :parameter binary: whether to open the file in binary mode.
        :returns: a decorated function that caches the result.
        """

        # Update if we're persisting
        self._persist = self._persist or persist

        def decorator(func: Callable) -> Callable:
            """Return the configured decorator."""

            @wraps(func)
            def wrapper(*args, reload: bool = False, **kwargs) -> Any:
                """Add options for memory and file system caching.

                First check if the function object is in the memory
                cache lookup. If not, check if persistence is enabled,
                and if so, check the manifest file to see if the
                qualified name is listed in it.
                """

                # Get a unique key from the function and arguments, check if in manifest
                arguments = utility.call(serialize, *args, **kwargs)
                key = f"{utility.qualify(func)}({arguments})"

                # Get the entry from memory of the manifest
                if not reload:
                    entry = None
                    if key in self._cache:
                        entry = self._cache[key]
                    elif persist:
                        entry = self._manifest.get(key)

                    # If it is, get the data
                    if entry is not None:
                        logging.debug("found entry")

                        # Check if it has expired
                        if entry.expiration is None or time.time() - entry.created < entry.expiration:

                            # Try to get the data from the entry
                            if entry.data is not NONE:
                                return entry.data

                            logging.debug(entry)
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
                    if file is None:
                        name = self._files.random(extension=extension)
                    else:
                        name = utility.call(file, *args, **kwargs) + extension

                    entry.name = name
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

    def retrieve(self, name: str, method: Reader = utility.read, binary: bool = False) -> Any:
        """Read a file from the cache."""

        with self._files.data(name, "rb" if binary else "r") as file:
            return method(file)

    def store(self, name: str, data: Any, method: Writer = utility.write, binary: bool = False):
        """Write data to a file in the cache."""

        with self._files.data(name, "wb" if binary else "w") as file:
            method(data, file)

    def persist(self):
        """Write the manifest to memory at exit."""

        if self._persist:
            self._manifest.write()

    def clear(self):
        """Clear everything from memory."""

        self._cache.clear()
        self._manifest.clear()

    def empty(self):
        """Empty all files in the cache."""

        self.clear()
        self._files.empty()
