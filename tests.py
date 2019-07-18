import unittest
import json
import time
import os
import sys

sys.path.append(os.path.dirname(__file__))

from cache import Cache
from cache import logging


counter = 0


def data(argument=None):
    """Get some arbitrary data."""

    if argument is not None:
        return argument

    global counter
    counter += 1
    return str(counter)


cache = Cache()


class CacheTest(unittest.TestCase):
    """Test all cache cases."""

    @classmethod
    def tearDown(cls):
        """Clear the memory cache each time."""

        logging.info("clearing memory cache")
        cache._cache.clear()

    @classmethod
    def test_memory_cache(cls):
        """Check storing a function call in memory."""

        logging.info("starting memory cache test")
        func = cache(persist=False)(data)  # Essentially calls the decorator
        result = func()
        assert func() == result  # The counter should change but the cached result should not

    @classmethod
    def test_persistent_cache(cls):
        """Check storing a function call in a file."""

        logging.info("starting persistent cache test")
        func = cache(persist=True)(data)
        result = func()
        cache._cache.clear()
        assert func() == result

    @classmethod
    def test_memory_serialize_arguments(cls):
        """Check custom functions for argument serialization."""

        logging.info("starting argument serialization test")
        func = cache(persist=False, serialize=lambda argument: str(hash(argument)))(data)
        result = func(argument="Hello, world!")
        key = "tests.data({})".format(hash("Hello, world!"))
        assert cache._cache[key].data == result

    @classmethod
    def test_memory_expiration(cls):
        """Test whether expiration works."""

        logging.info("starting expiration cache test")
        func = cache(persist=False, expiration=2)(data)
        result = func()
        time.sleep(1)
        assert func() == result
        time.sleep(1.5)
        assert func() != result

    @classmethod
    def test_persistent_file(cls):
        """Check if file names are stored correctly."""

        logging.info("starting file name cache test")
        func = cache(persist=True, file=lambda argument: str(argument), extension=".txt")(data)
        func("hello")
        assert cache._files._data.joinpath("hello.txt").exists()

    @classmethod
    def test_store_retrieve(cls):
        """Check the store and retrieve overrides."""

        logging.info("starting store and retrieve test")
        func = cache(persist=True, store=json.dump, retrieve=json.load, extension=".json")(data)
        func({"number": 1})
        cache._cache.clear()
        cache._manifest.write()
        cache._manifest._manifest.clear()
        cache._manifest.read()
        assert type(func({"number": 1})) == dict and func({"number": 1})["number"] == 1

    @classmethod
    def tearDownClass(cls):
        """Clean up after ourselves."""

        cache.empty()
