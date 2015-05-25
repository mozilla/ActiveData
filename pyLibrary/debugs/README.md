
Logging and Exceptions
======================


Motivation
----------

Exception handling and logging are undeniably linked.  There are many instances
where exceptions are raised and must be logged, except when a subsuming system 
can compensate.  Exception handling semantics are great because they 
decoupling the cause from the solution, but this is at odds with clean logging -
which couples raising and catching to make appropriate decisions about what to
emit to the log.  

This logging module is additionally responsible for raising exceptions, 
collecting the trace and context, and then deducing if it must be logged, or 
if it can be ignored because something can handle it.

Basic Forms
-----------

**Use `Log.note()` for all logging**

```python
    Log.note("Hello, World!")
```

There is no need to create logger objects.  The `Log` module will keep track of what, where and who of every call.


**Use named parameters**

Do not use Python's formatting operator "`%`" nor it's `format()` function. Using them will create a string at call time, which is a parsing nightmare for log analysis tools.

```python
    Log.note("Hello, {{name}}!", name="World!")
```

All logs are structured logs; the parameters will be included, unchanged, in the log structure.  This library also expects all parameter values to be JSON-serializable so they can be stored/processed by downstream JSON tools.
  
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
		}
	}
```

**Instead of `raise` use `Log.error()`**

```python
    Log.error("This will throw an error")
```

The actual call will always raise an exception, and it manipulates the stack trace to ensure the caller is approriatly blamed.  Feel free to use the `raise` keyword (as in `raise Log.error("")`), if that looks nicer to you. 

**Always chain your exceptions**

The `cause` parameter accepts an `Exception`, or a list of exceptions.  Chaining is generally good practice that helps you find the root cause of a failure. 

```python
    try:
        # Do something that might raise exception
    except Exception, e:
        Log.error("Describe what you were trying to do", cause=e)
```

**Use named parameters in your error descriptions too**

Error logging accepts keyword parameters just like `Log.note()` does

```python
    def worker(value):
        try:
            Log.note("Start working with {{key1}}", key1=value1)
            # Do something that might raise exception
        except Exception, e:
            Log.error("Failure to work with {{key2}}", key2=value2, cause=e)
```

**If you can deal with an exception, then it will never be logged**

```python
    def worker(value):
        try:
            Log.error("Failure to work with {{key3}}", key3=value3)
        except Exception, e:
            # Try something else
```

**Use `Log.warning()` if your code can deal with an exception, but you still want to log it as an issue**

```python
    def worker(value):
        try:
            Log.note("Start working with {{key4}}", key4=value4)
            # Do something that might raise exception
        except Exception, e:
            Log.warning("Failure to work with {{key4}}", key4=value4, cause=e)
```

Other forms
-----------

All the `Log` functions all accept a `default_params` as a second parameter, like so: 

```python
    Log.note("Hello, {{name}}!", {"name": "World!"})
```

this is meant for the situation your code already has a bundled structure you wish to use as a source of parameters.  If keyword parameters are used, they will override the default values.  Be careful when sending whole data structures, they will be logged!

**Please, never use locals()**

```python
    def worker(value):
		name = "tout le monde!"
		password = "123"
        Log.note("Hello, {{name}}", locals())  	# DO NOT DO THIS!
```

Despite the fact using `locals()` is a wonderful shortcut for logging it is dangerous because it also picks up sensitive local variables.  Even if `{{name}}` is the only value in the template, the whole `locals()` dict will be sent to the structured loggers for recording. 


Configuration
-------------

The `logs` module will log to the console by default.  ```Log.start(settings)```
will redirect the logging to other streams, as defined by the settings:

 *  **log** - List of all log-streams and their parameters
 *  **trace** - Show more details in every log line (default False)
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

        except Exception, e:
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
