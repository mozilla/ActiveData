pyLibrary.convert
-----------------

General conversion library with functions in the form `<from_type> "2" <to_type>`.
Most of these functions are simple wrappers around common Python functions,
some are more esoteric.  A few are special implementations.


pyLibrary.strings.expand_template()
-----------------------------------

    expand_template(template, value)

A simple function that replaces variables in `template` with the properties
found in `value`. Variables are indicated by the double mustaches;
`{{example}}` is a variable.

Properties are converted to `unicode()` before replacing variables.  In the case
of complex properties; converted to JSON.  Further string manipulation can be
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

Look into the `pyLibrary.strings.py` to see a full list of transformation
functions.

Variables are not limited to simple names: You may use dot (`.`) to specify
paths into the properties
```python
    >>> details = {"person":{"name":"Kyle Lahnakoski", "age":40}}
    >>> print expand_template("{{person.name}} is {{person.age}} years old", details)
    Kyle Lahnakoski is 40 years old
```
Templates are not limited to strings, but can also be queries to expand lists
found in property paths:

<incomplete>
