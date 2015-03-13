
Logging and Exceptions
======================


Motivation
----------

Exception handling and logging are undeniably linked.  There are many instances
where exceptions are raised and must be logged, except when a subsuming system can handle compensate.  Exception handling semantics are great because they decoupling the cause from the solution, but this is at odds with clean logging -
which couples raising and catching to make appropriate decisions about what to
ultimately emit to the log.  

This logging module is additionally responsible for raising exceptions, collecting the trace and context, and then deducing
if it must be logged, or it can be ignored because something can handle it.

Getting Started
---------------

###Instead of `raise` use `Log.error()`

```python
    Log.error("This will throw an error")
```

###Always chain your exceptions

```python
    try:
        # Do something that might raise exception
    except Exception, e:
        Log.error("Describe what you were trying to do", e)
```

###Name your logging parameters for later machine manipulation:

```python
    def worker(value):
        try:
            Log.note("Start working with {{key1}}", {"key1": value1})
            # Do something that might raise exception
        except Exception, e:
            Log.error("Failure to work with {{key2}}", {"key2":value2}, e)
```

###If you can deal with an exception, then it will never be logged

```python
    def worker(value):
        try:
            Log.error("Failure to work with {{key3}}", {"key3": value3})
        except Exception, e:
            # Try something else
```

###Use `Log.warning()` if your code can deal with an exception, but you still want to log it as an issue

```python
    def worker(value):
        try:
            Log.note("Start working with {{key4}}", {"key4": value4})
            # Do something that might raise exception
        except Exception, e:
            Log.warning("Failure to work with {{key4}}", {"key4":value4}, e)
```


This library also expects all log messages and exception messages to have named
parameters so they can be stored in easy-to-digest JSON, which can be processed
by downstream tools.

```python
    Log.error("This will throw an {{name}} error", {"name":"special")
```


Configuration
-------------

The `logs` module will log to the console by default.  ```Log.start(settings)```
will redirect the logging to other streams, as defined by the settings:

 *  **log** - List of all log-streams and their parameters
 *  **trace** - Show more details in everry log line (default False)
 *  **cprofile** - Used to enable the builtin python c-profiler (default False)
 *  **profile** - Used to enable pyLibrary's simple profiling (default False)
    (eg with Profiler("some description"):)
 *  **constants** - Map absolute path of module constants to the values that will
    be assigned.  Used mostly to set debugging constants in modules.

Of course, logging should be the first thing to be setup (aside from digesting
settings of course).  For this reason, applications should have the following
structure:

```python
    def main():
        try:
            settings = startup.read_settings()
            Log.start(settings.debug)

            # DO WORK HERE

        except Exception, e:
            Log.error("Complain, or not", e)
        finally:
            Log.stop()
```


Log 'Levels'
------------

The `logs` module has no concept of logging levels it is expected that debug variables (variables prefixed with `DEBUG_` are used to control the logging output.
    

```python
	# simple.py
	DEBUG_SHOW_DETAIL = True

    def worker():
		if DEBUG_SHOW_DETAIL:
			Log.note("Starting")

        # DO WORK HERE

		if DEBUG_SHOW_DETAIL:
			Log.note("Done")

    def main():
        try:
            settings = startup.read_settings()
            Log.start(settings.debug)

            # DO WORK HERE

        except Exception, e:
            Log.error("Complain, or not", e)
        finally:
            Log.stop()

```

These debug variables can be set by configuration file:

```json
	// settings.json
	{
		"debug":{
			"constants":{"simple.DEBUG_SHOW_DETAILS":false}
		}
	}
```








