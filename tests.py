import unittest
import json
import shutil
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

    def tearDown(self):
        """Clear the memory cache each time."""

        logging.info("clearing memory cache")
        cache._cache.clear()
        cache._manifest.clear()

    def test_memory_cache(self):
        """Check storing a function call in memory."""

        logging.info("starting memory cache test")
        func = cache(persist=False)(data)  # Essentially calls the decorator
        result = func()
        assert func() == result  # The counter should change but the cached result should not

    def test_persistent_cache(self):
        """Check storing a function call in a file."""

        logging.info("starting persistent cache test")
        func = cache(persist=True)(data)
        result = func()
        cache._cache.clear()
        assert func() == result

    def test_memory_serialize_arguments(self):
        """Check custom functions for argument serialization."""

        logging.info("starting argument serialization test")
        func = cache(persist=False, serialize=lambda argument: str(hash(argument)))(data)
        result = func(argument="Hello, world!")
        key = "tests.data({})".format(hash("Hello, world!"))
        assert cache._cache[key].data == result

    def test_memory_expiration(self):
        """Test whether expiration works."""

        logging.info("starting expiration cache test")
        func = cache(persist=False, expiration=2)(data)
        result = func()
        time.sleep(1)
        assert func() == result
        time.sleep(1.5)
        assert func() != result

    def test_persistent_file(self):
        """Check if file names are stored correctly."""

        logging.info("starting file name cache test")
        func = cache(persist=True, file=lambda argument: str(argument), extension=".txt")(data)
        func("hello")
        assert os.path.exists(cache._files._data.joinpath("hello.txt"))

    def test_store_retrieve(self):
        """Check the store and retrieve overrides."""

        logging.info("starting store and retrieve test")
        func = cache(persist=True, store=json.dump, retrieve=json.load)(data)
        func({"number": 1})
        cache._cache.clear()
        assert type(func()) == dict and func()["number"] == 1

    @classmethod
    def tearDownClass(cls):
        """Clean up after ourselves."""

        shutil.rmtree(cache._files._root)
