import os
import json
import contextlib
import collections
import tempfile
import logging
from functools import wraps

from . import meta


logging.basicConfig(level=logging.DEBUG)


class Files(metaclass=meta.Singleton):
    """Files access for the cache."""

    _cwd = os.getcwd()
    _directory = os.path.join(_cwd, "cache", "data")
    _manifest = os.path.join(_cwd, "cache", "cache.json")

    @contextlib.contextmanager
    def manifest(self, mode: str="r"):
        """Return the opened manifest file."""

        try:
            with open(self._manifest, mode) as file:
                yield file
        except FileNotFoundError:
            logging.debug("no manifest file found!")
            os.makedirs(os.path.dirname(self._manifest))
            if not os.path.exists(self._manifest):
                with open(self._manifest, "w") as file:
                    json.dump({}, file)
            logging.debug("manifest created")
            with open(self._manifest, mode) as file:
                yield file

    @contextlib.contextmanager
    def cache(self, name: str, mode: str="r"):
        """Return a file object from the cache."""

        path = os.path.join(self._directory, name)
        try:
            with open(path, mode) as file:
                yield file
        except FileNotFoundError:
            logging.debug("cache directory missing")
            os.makedirs(self._directory, exist_ok=True)
            with open(path, mode) as file:
                yield file

    def random(self):
        """Get a random unique file name in the cache directory."""

        return os.path.basename(tempfile.mktemp(dir=self._directory, prefix=""))


files = Files()


class Manifest(metaclass=meta.Singleton):
    """Manifest file wrapper."""

    def get(self, key: str, default=None):
        """Get a key from the manifest file."""

        try:
            with files.manifest() as file:
                data = json.load(file)
                return data.get(key, default)
        except json.JSONDecodeError:
            data = self.reset()
            return data.get(key, default)  # Just in case something is added to base

    def set(self, key: str, value: str):
        """Set a key in the manifest."""

        with files.manifest() as file:
            data = json.load(file)
        data[key] = value
        with files.manifest("w") as file:
            json.dump(data, file, indent=2)

    def pop(self, key: str):
        """Remove a key and value from the manifet."""

        with files.manifest() as file:
            data = json.load(file)
        data.pop(key)
        with files.manifest("w") as file:
            json.dump(data, file, indent=2)

    def reset(self):
        """Reset the manifest."""

        base = {}
        with files.manifest("w") as file:
            json.dump(base, file, indent=2)
        return base


manifest = Manifest()


def call(obj, *args, **kwargs):
    """Call and return result if possible, otherwise return."""

    try:
        return obj(*args, **kwargs)
    except TypeError:
        return obj


def qualify(func: collections.Callable):
    """Qualify a function."""

    return ".".join((func.__module__, func.__qualname__))


def _serialize(func, args):
    """Fully serialize a function."""

    return qualify(func) + "(" + args + ")"


def write(data, file):
    file.write(data)


def read(file):
    return file.read()


class Cache(metaclass=meta.Singleton):
    """A cache object used to speed up access to resources."""

    _cache = {}

    def __call__(self,
                 f=None,
                 serialize=None,
                 file=None,
                 extension=None,
                 store=None,
                 retrieve=None,
                 persist=True,
                 binary=False):
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

        Binary is the file mode that should be used to open the file
        before it is read from or written to. The w argument as
        opposed to the wb.
        """

        def decorator(func):
            """Return the configured decorator."""

            @wraps(func)
            def wrapper(*args, reload=False, **kwargs):
                """Add options for memory and file system caching.

                First check if the function object is in the memory
                cache lookup. If not, check if persistence is enabled,
                and if so, check the manifest file to see if the
                qualified name is listed in it.
                """

                if func in self._cache and not reload:
                    return self._cache[func]

                path = ""  # Path will be defined, this just prevents code check errors
                if persist:

                    # Get a unique key from the function and arguments, check if in manifest
                    arguments = "" if serialize is None else call(serialize, *args, **kwargs)
                    key = _serialize(func, arguments)
                    path = manifest.get(key)

                    # If it is, get the data
                    if path is not None:
                        logging.debug("found path in manifest")
                        data = self.retrieve(path, method=retrieve, binary=binary)
                        if data is not None:
                            logging.debug("retrieved data correctly")
                            self._cache[func] = data
                            return data
                        else:
                            logging.debug("couldn't access cache data")

                    # Otherwise, generate a new path with the provided file function
                    if file is not None:
                        path = call(file, *args, **kwargs) + (extension or "")
                        manifest.set(key, path)

                    # Otherwise, generate a random file name
                    else:
                        path = files.random() + (extension or "")
                        manifest.set(key, path)

                self._cache[func] = result = func(*args, **kwargs)

                if persist:
                    self.store(path, result, method=store, binary=binary)
                    logging.debug("stored results")

                return result

            return wrapper

        if f is not None:
            return decorator(f)
        return decorator

    def retrieve(self, name: str, method=None, binary: bool=False) -> object:
        """Read a file from the cache."""

        method = method or read
        try:
            with files.cache(name, "rb" if binary else "r") as file:
                return method(file)
        except FileNotFoundError:
            return None
        except Exception as exception:
            logging.error(f"caught {exception} while retrieving")
            return None

    def store(self, name: str, data, method=None, binary: bool=False):
        """Write data to a file in the cache."""

        method = method or write
        with files.cache(name, "wb" if binary else "w") as file:
            method(data, file)


cache = Cache()
