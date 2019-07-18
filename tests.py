import unittest
import json
import pickle
import time

from cache import Cache
from cache import logging


counter = 0


def data(arg=None):
    """Get some arbitrary data."""

    if arg is not None:
        return arg

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

    def test_memory_cache(self):
        """Check storing a function call in memory."""

        logging.info("starting memory cache test")
        func = cache(persist=False)(data)  # Essentially calls the decorator
        result = func()
        self.assertEqual(func(), result)  # The counter should change but the cached result should not

    def test_persistent_cache(self):
        """Check storing a function call in a file."""

        logging.info("starting persistent cache test")
        func = cache(persist=True, store=pickle.dump, retrieve=pickle.load, binary=True)(data)
        result = func([1, 2, "a", "b"])
        cache._manifest.write()
        cache.clear()
        cache._manifest.read()
        self.assertListEqual(result, func([1, 2, "a", "b"]))

    def test_memory_serialize_arguments(self):
        """Check custom functions for argument serialization."""

        logging.info("starting argument serialization test")
        func = cache(persist=False, serialize=lambda argument: str(hash(argument)))(data)
        result = func("Hello, world!")
        key = "tests.data({})".format(hash("Hello, world!"))
        self.assertEqual(cache._cache[key].data, result)

    def test_memory_expiration(self):
        """Test whether expiration works."""

        logging.info("starting expiration cache test")
        func = cache(persist=False, expiration=2)(data)
        result = func()
        time.sleep(1)
        self.assertEqual(func(), result)
        time.sleep(1.5)
        self.assertNotEqual(func(), result)

    def test_persistent_file(self):
        """Check if file names are stored correctly."""

        logging.info("starting file name cache test")
        func = cache(persist=True, file=lambda argument: str(argument), extension=".txt")(data)
        func("hello")
        self.assertTrue(cache._files._data.joinpath("hello.txt").exists())

    def test_store_retrieve(self):
        """Check the store and retrieve overrides."""

        logging.info("starting store and retrieve test")
        func = cache(persist=True, store=json.dump, retrieve=json.load, extension=".json")(data)
        func({"number": 1})
        cache._manifest.write()
        cache.clear()
        cache._manifest.read()
        self.assertTrue(type(func({"number": 1})) == dict and func({"number": 1})["number"] == 1)

    @classmethod
    def tearDownClass(cls):
        """Clean up after ourselves."""

        cache.empty()
        cache._persist = False
