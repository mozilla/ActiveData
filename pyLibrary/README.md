
Module `convert`
---------------

General conversion library with functions in the form `<from_type> "2" <to_type>`.
Most of these functions are simple wrappers around common Python functions;
often dealing with `None` values gracefully. Many functions are generally
useful. A few are esoteric.


Module `meta`
-------------

###Decorator `use_settings`###

**Description**

`@use_settings` will decorate a function to accept a `settings` parameter which is just like `**kwargs`, but the named parameters can override the properties in `settings`, rather than raise duplicate keyname exceptions.


**Example**

We decorate the `login()` function with `@use_settings`. In this case, `username` is a required parameter, and `password` will default to `None`. The settings parameter should always default to `None` so that it's not required.

```python
		@use_settings
		def login(username, password=None, settings=None):
			pass
```

Define some `dicts` for use with our `settings` parameter:

		creds = {"userame": "ekyle", "password": "password123"}
		alt_creds = {"username": "klahnakoski"}


The simplest case is when we use settings with no overrides

		login(settings=creds)
		# SAME AS
		login(**creds)
		# SAME AS
		login(username="ekyle", password="password123")

You may override any property in settings, in this case it is `password`

		login(password="123", settings=creds)
		# SAME AS
		login(username="ekyle", password="123")

There is no problem with overriding everything in `settings`:

		login(username="klahnakoski", password="asd213", settings=creds)
		# SAME AS
		login(username="klahnakoski", password="asd213")

You may continue to use `**kwargs`; which provides a way to overlay one parameter template (`creds`) with another (`alt_creds`)

		login(settings=creds, **alt_creds)
		# SAME AS
		login(username="klahnakoski", password="password123")


**Motivation** - Extensive use of dependency injection, plus managing the configuration
for each of the components being injected, can result in some spectacularly
complex system configuration. One way to reduce the complexity is to use
configuration templates that contain useful defaults, and simply overwrite
the properties that need to be changed for the new configuration.
`@use_settings` has been created to provide this templating system for Python
function calls (primarily class constructors).


Module `strings`
----------------

Contains several more string functions. None of them are intended for direct
use in a Python program, rather for use in the `expand_template` function.


###Function `expand_template()`###

The creation of this function was motivated by the desire to extend Python's
`format()` function with more features and replace the [formatting mini language](https://docs.python.org/2/library/string.html#formatspec),
because it appears to be Perl-inspired line noise.

    	pyLibrary.strings.expand_template(template, value)

A simple function that replaces variables in `template` with the properties
found in `value`. Variables are indicated by the double moustaches;
`{{example}}` is a variable.

Properties are converted to `unicode()` before replacing variables. In the case
of complex properties; converted to JSON. Further string formatting can be
performed by feeding properties to functions using the pipe (`|`) symbol:

```python
    >>> from pyLibrary.strings import expand_template
    >>> total = 123.45
    >>> some_list = [10, 11, 14, 80]

    >>> print expand_template("it is currently {{now|datetime}}", {"now": 1420119241000})
    it is currently 2015-01-01 13:34:01

    >>> print expand_template("Total: {{total|right_align(20)}}", {"total": total})
    Total:               123.45

    >>> print expand_template("Summary:\n{{list|json|indent}}", {"list": some_list})
    Summary:
            [10, 11, 14, 80]
```

Look into the [`pyLibrary.strings.py`](https://github.com/klahnakoski/pyLibrary/blob/dev/pyLibrary/strings.py) to see a full list of transformation
functions.

Variables are not limited to simple names: You may use dot (`.`) to specify
paths into the properties

```python
    >>> details = {"person":{"name":"Kyle Lahnakoski", "age":40}}
    >>> print expand_template("{{person.name}} is {{person.age}} years old", details)
    Kyle Lahnakoski is 40 years old
```

**Nested Objects and Template Expansion**

Templates are not limited to strings, but can also be queries to expand lists
found in property paths:

<incomplete, add more docs here>
