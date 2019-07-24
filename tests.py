import unittest
import json
import pickle
import time

from cache import Cache


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

        cache._cache.clear()

    def test_memory_cache(self):
        """Check storing a function call in memory."""

        func = cache(persist=False)(data)  # Essentially calls the decorator
        result = func()
        self.assertEqual(func(), result)  # The counter should change but the cached result should not

    def test_persistent_cache(self):
        """Check storing a function call in a file."""

        func = cache(persist=True, store=pickle.dump, retrieve=pickle.load, binary=True)(data)
        result = func([1, 2, "a", "b"])
        cache._manifest.write()
        cache.clear()
        cache._manifest.read()
        self.assertListEqual(result, func([1, 2, "a", "b"]))

    def test_memory_serialize_arguments(self):
        """Check custom functions for argument serialization."""

        func = cache(persist=False, serialize=lambda argument: str(hash(argument)))(data)
        result = func("Hello, world!")
        key = "tests.data({})".format(hash("Hello, world!"))
        self.assertEqual(cache._cache[key].data, result)

    def test_memory_expiration(self):
        """Test whether expiration works."""

        func = cache(persist=False, expiration=2)(data)
        result = func()
        time.sleep(1)
        self.assertEqual(func(), result)
        time.sleep(1.5)
        self.assertNotEqual(func(), result)

    def test_persistent_file(self):
        """Check if file names are stored correctly."""

        func = cache(persist=True, file=lambda argument: str(argument), extension=".txt")(data)
        func("hello")
        self.assertTrue(cache._files._data.joinpath("hello.txt").exists())

    def test_store_retrieve(self):
        """Check the store and retrieve overrides."""

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
