
Logging and Exceptions
======================


Motivation
----------

Exception handling and logging are undeniably linked.  There are many instances
where exceptions are raised and must be logged, except when they are
appropriately handled.  The greatness of exception handling semantics comes from
decoupling the cause from the solution, but this is at odds with clean logging -
which couples raising and handling to make appropriate decisions about what to
ultimately emit to the log.  For this reason, the logging module is responsible
for collecting the trace and context, raising the exception, and then deducing
if there is something that will handle it (so it can be ignored), or if it
really must be logged.

***Instead of `raise` use Log.error()

```python
    Log.error("This will throw an error")
```

***Always chain your exceptions

```python
    try:
        # Do something that might raise exception
    except Exception, e:
        Log.error("Describe what you were trying to do", e)
```

***Name your logging parameters for later machine manipulation:

```python
    def worker(value):
        try:
            Log.note("Start working with {{value}}", {"value": value})
            # Do something that might raise exception
        except Exception, e:
            Log.error("Failure to work with {{value}}", {"value":value}, e)
```

***If you can deal with an exception, then it will never be logged

```python
    def worker(value):
        try:
            Log.error("Failure to work with {{value}}", {"value": value})
        except Exception, e:
            # Try something else
```


***But, even if you can deal with an exception, sometimes you still want to log the error because it was not ideal

```python
    def worker(value):
        try:
            Log.note("Start working with {{value}}", {"value": value})
            # Do something that might raise exception
        except Exception, e:
            Log.warning("Failure to work with {{value}}", {"value":value}, e)
```


This library also expects all log messages and exception messages to have named
parameters so they can be stored in easy-to-digest JSON, which can be processed
by downstream tools.

```python
    Log.error("This will throw an {{name}} error", {"name":"special")
```









pyLibrary.debugs.logs
---------------------

The logs module will log to the console by default.  ```Log.start(settings)```
will redirect the logging to other streams, as defined by the settings:

 *  log - List of all log-streams and their parameters
 *  trace - Show more details in everry log line (default False)
 *  cprofile - Used to enable the builtin python c-profiler (default False)
 *  profile - Used to enable pyLibrary's simple profiling (default False)
    (eg with Profiler("some description"):)
 *  constants - Map absolute path of module constants to the values that will
    be assigned.  Used mostly to set debugging constants in modules.

Of course logging should be the first thing to be setup (aside from digesting
settings of course).  For this reason, applications should have the following
structure:

    def main():
        try:
            settings = startup.read_settings()
            Log.start(settings.debug)

            # DO WORK HERE

        except Exception, e:
            Log.error("Complain, or not", e)
        finally:
            Log.stop()

