
More Logs! - Structured Logging and Exception Handling
=============++++=====================================

This library provides two main features

* **Structured logging** - output is all JSON (with options to serialize to text)
* **Exception handling weaved in** - Good logs must represent what happened,
and that can not be done if the logging library is not intimately familiar with
the (exceptional) code paths taken.

Motivation
----------

Exception handling and logging are undeniably linked. There are many instances
where exceptions are raised and must be logged, and others where the subsuming 
system can fully handle the exception, and no log should be emitted. Exception 
handling semantics are great because they decouple the cause from the solution, 
but this can be at odds with clean logging - which couples raising and catching 
to make appropriate decisions about what to emit to the log.  

This logging module is additionally responsible for raising exceptions,
collecting the trace and context, and then deducing if it must be logged, or
if it can be ignored because something can handle it.

**More Reading**

* **Structured Logging is Good** - https://sites.google.com/site/steveyegge2/the-emacs-problem


Basic Usage
-----------

**Use `Log.note()` for all logging**

```python
    Log.note("Hello, World!")
```

There is no need to create logger objects. The `Log` module will keep track of
what, where and who of every call.


**Use named parameters**

Do not use Python's formatting operator "`%`" nor it's `format()` function.
Using them will create a string at call time, which is a parsing nightmare
for log analysis tools.

```python
    Log.note("Hello, {{name}}!", name="World!")
```

All logs are structured logs; the parameters will be included, unchanged, in
the log structure. This library also expects all parameter values to be JSON-
serializable so they can be stored/processed by downstream JSON tools.

```javascript
	{//EXAMPLE STRUCTURED LOG
		"template": "Hello, {{name}}!",
		"param": {"name": "World!"},
		"timestamp": 1429721745,
		"thread": {
			"name": "Main thread"
		},
		"location": {
			"line": 3,
			"file": "hello.py",
			"method": "hello"
		},
		"machine": {
            "python": "CPython",
            "os": "Windows10",
            "name": "ekyle-win"
		}
	}
```

**Instead of `raise` use `Log.error()`**

```python
    Log.error("This will throw an error")
```

The actual call will always raise an exception, and it manipulates the stack
trace to ensure the caller is appropriately blamed. Feel free to use the
`raise` keyword (as in `raise Log.error("")`), if that looks nicer to you. 

**Always chain your exceptions**

The `cause` parameter accepts an `Exception`, or a list of exceptions.
Chaining is generally good practice that helps you find the root cause of
a failure. 

```python
    try:
        # Do something that might raise exception
    except Exception as e:
        Log.error("Describe what you were trying to do", cause=e)
```

**Always catch all `Exceptions`**

Catching all exceptions is preferred over the *only-catch-what-you-can-handle*
strategy. First, exceptions are not lost because we are chaining. Second,
we catch unexpected `Exceptions` early and we annotate them with a
description of what the local code was intending to do. This annotation
effectively groups the possible errors (known, or not) into a class, which
can be used by callers to decide on appropriate mitigation.  

To repeat: When using dependency injection, callers can not reasonably be
expected to know about the types of failures that can happen deep down the
call chain. This makes it vitally important that methods summarize all
exceptions, both known and unknown, so their callers have the information to
make better decisions on appropriate action.  

For example: An abstract document container, implemented on top of a SQL 
database, should not emit SQLExceptions of any kind: A caller that uses a 
document container should not need to know how to handle SQLExceptions (or any 
other implementation-specific exceptions).  Rather, in this example, the 
caller should be told it "can not add a document", or "can not remove a 
document". This allows the caller to make reasonable desisions when they do 
occur.  The original cause (the SQLException) is in the causal chain. 

**Use named parameters in your error descriptions too**

Error logging accepts keyword parameters just like `Log.note()` does

```python
    def worker(value):
        try:
            Log.note("Start working with {{key1}}", key1=value1)
            # Do something that might raise exception
        except Exception as e:
            Log.error("Failure to work with {{key2}}", key2=value2, cause=e)
```

**No need to formally type your exceptions**

An exception can be uniquely identified by the first-parameter string template
it is given; exceptions raised with the same template are the same type. You
should have no need to create new exception sub-types.

**Testing for exception "types"**

This library advocates chaining exceptions early and often, and this hides
important exception types in a long causal chain.   More Logs! allows you to easily
test if a type (or string, or template) can be found in the causal chain by using
the `in` keyword:   

```python
    def worker(value):
        try:
            # Do something that might raise exception
        except Exception as e:
            if "Failure to work with {{key2}}" in e:
				# Deal with exception thrown in above code, no matter
				# how many other exception handlers where in the chain
```

**If you can deal with an exception, then it will never be logged**

When a caller catches an exception from a callee, it is the caller's
responsibility to handle that exception, or re-raise it. There are many
situations a caller can be expected to handle exceptions; and in those cases
logging an error would be deceptive. 

```python
    def worker(value):
        try:
            Log.error("Failure to work with {{key3}}", key3=value3)
        except Exception as e:
            # Try something else
```

**Use `Log.warning()` if your code can deal with an exception, but you still 
want to log it as an issue**

```python
    def worker(value):
        try:
            Log.note("Start working with {{key4}}", key4=value4)
            # Do something that might raise exception
        except Exception as e:
            Log.warning("Failure to work with {{key4}}", key4=value4, cause=e)
```
**Don't loose your stack trace!**

Be aware your `except` clause can also throw exceptions: In the event you
catch a vanilla Python Exception, you run the risk of loosing its stack trace.
To prevent this, wrap your exception in an `Except` object, which will capture
your trace for later use. Exceptions thrown from this `Log` library need not
be wrapped because they already captured their trace. If you wrap an `Except`
object, you simply get back the object you passed.

```python
	try:
		# DO SOME WORK		
    except Exception as e:
        e = Except.wrap(e)
        # DO SOME FANCY ERROR RECOVERY
 ```


Other forms
-----------

All the `Log` functions accept a `default_params` as a second parameter, like so:

```python
    Log.note("Hello, {{name}}!", {"name": "World!"})
```

this is meant for the situation your code already has a bundled structure you
wish to use as a source of parameters. If keyword parameters are used, they
will override the default values. Be careful when sending whole data
structures, they will be logged!

**Please, never use locals()**

```python
    def worker(value):
		name = "tout le monde!"
		password = "123"
        Log.note("Hello, {{name}}", locals())  	# DO NOT DO THIS!
```

Despite the fact using `locals()` is a wonderful shortcut for logging it is
dangerous because it also picks up sensitive local variables. Even if
`{{name}}` is the only value in the template, the whole `locals()` dict will
be sent to the structured loggers for recording. 


Log 'Levels'
------------

The `logs` module has no concept of logging levels it is expected that debug
variables (variables prefixed with `DEBUG_` are used to control the logging
output.


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

        except Exception as e:
            Log.error("Complain, or not", e)
        finally:
            Log.stop()
```

These debug variables can be set by configuration file:

```javascript
	// settings.json
	{
		"debug":{
			"constants":{"simple.DEBUG_SHOW_DETAILS":false}
		}
	}
```

Configuration
-------------

The `logs` module will log to the console by default. ```Log.start(settings)```
will redirect the logging to other streams, as defined by the settings:

 *  **log** - List of all log-streams and their parameters
 *  **trace** - Show more details in every log line (default False)
 *  **cprofile** - Used to enable the builtin python c-profiler (default False)
 *  **profile** - Used to enable pyLibrary's simple profiling (default False)
    (eg with Profiler("some description"):)
 *  **constants** - Map absolute path of module constants to the values that will
    be assigned. Used mostly to set debugging constants in modules.

Of course, logging should be the first thing to be setup (aside from digesting
settings of course). For this reason, applications should have the following
structure:

```python
    def main():
        try:
            settings = startup.read_settings()
            Log.start(settings.debug)

            # DO WORK HERE

        except Exception as e:
            Log.error("Complain, or not", e)
        finally:
            Log.stop()
```



		"log": [
			{
				"class": "logging.handlers.RotatingFileHandler",
				"filename": "examples/logs/examples_etl.log",
				"maxBytes": 10000000,
				"backupCount": 100,
				"encoding": "utf8"
			},
			{
				"log_type": "email",
				"from_address": "klahnakoski@mozilla.com",
				"to_address": "klahnakoski@mozilla.com",
				"subject": "[ALERT][DEV] Problem in ETL Spot",
				"$ref": "file://~/private.json#email"
			},
			{
				"log_type": "console"
			}
		]



Problems with Python Logging
----------------------------

[Python's default `logging` module](https://docs.python.org/2/library/logging.html#logging.debug)
comes close to doing the right thing, but fails:  
  * It has keyword parameters, but they are expanded at call time so the values are lost in a string.  
  * It has `extra` parameters, but they are lost if not used by the matching `Formatter`.  
  * It even has stack trace with `exc_info` parameter, but only if an exception is being handled.

Python 2.x has no builtin exception chaining, like [Python 3 does](https://www.python.org/dev/peps/pep-3134/)


