# Cache

The cache module provides a convenient interface for dynamic memory and file-system caching.
It has no dependencies and not installation process.
To use it, simply instantiate a cache object and use it to decorate the functions whose results you wish to cache.
Take a look at the following example:

```python
from cache import Cache

cache = Cache()

@cache
def download() -> str:
    return "Hello, world!"
```

The first time `download()` is called, it will run the function code.
However, every times after it will look into the cache instead of re-invoking the function unless the `reload=True` argument is passed.

The cache module includes other useful functionality:

```python
@cache(serialize=lambda a, b: f"{a}, {b}")
def add(a: int, b: int) -> int:
    """Cache unique values for different arguments."""

    return a + b
```

My approach to caching different results based on the arguments passed to the function is basically to push the work of serializing them onto the user.
What's happening here is that internally, the key used to remember the function call is combined with the serialized arguments.
In this case, `add(1, 2)` would be serialized as `"module.add(1, 2)"`.

Caching in the file system is also available, and is enabled by default.
This creates a cache directory and manifest file in the working directory, but the location of the file system can be configured in the `Cache` initialization.
The file name can be specifically set, either as a static value or with a function that is passed the arguments of the function call.
If it is not set, a random file name is generated.
A file extension can also be added in both cases so that the files may be viewed externally.
To store and retrieve with a custom format like JSON, for example, pass a `load` and `dump` function to the cache decorator.
And finally, to set expiration on a function, use the corresponding argument in the decorator with the number of seconds after which the result should be invalidated.

For more examples, take a look at the tests until I add full examples.
