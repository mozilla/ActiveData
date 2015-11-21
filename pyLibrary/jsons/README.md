

Module `jsons.encode`
=====================

Function: `jsons.encode.json_encoder()`
-------------------------------------

Fast JSON encoder used in `convert.value2json()` when running in Pypy.  Run the
[speedtest](https://github.com/klahnakoski/pyLibrary/blob/master/tests/speedtest_json.py)
to compare with default implementation and ujson


Module `jsons.stream`
=====================

A module supporting the implementation of queries over very large JSON 
strings.  The overall objective is to make a large JSON document appear like 
a hierarchical database, where arrays of any depth, can be queried like 
tables. 

### Limitations

This is not a generic streaming JSON parser.  This module has two main 
restrictions:

1. **Objects are not streamed** - All objects will reside in memory.  Large 
   objects, with a multitude of properties, may cause problems.  Property 
   names should be known at query time.  If you must serialize large objects; 
   instead of `{<name>: <value>}` format, try a list of name/value pairs 
   instead: `[{"name": <name>, "value": <value>}]`  This format is easier to 
   query, and gentler on the various document stores that you may put this 
   data into. 
2. **Array values must be the last object property** - If you query into a 
   nested array, all sibling properties found after that array must be ignored 
   (must not be in the `expected_vars`).  If not, then those arrays will not 
   benefit from streaming, and will reside in memory.   


Function `jsons.stream.parse()`
-------------------------------

Will return an iterator over all objects found in the JSON stream.

**Parameters:**

* **json** - a parameter-less function, when called returns some number of 
  bytes from the JSON stream.  It can also be a string.
* **path** - a list of strings specifying the nested JSON paths.  Use 
  `"."` if your JSON starts with `[`, and is a list. 
* **expected_vars** - a list of strings specifying the full property names 
  required (all other properties are ignored)


###Examples

**Simple Iteration**

	json = {"b": "done", "a": [1, 2, 3]}
	parse(json, path="a", required_vars=["a", "b"]}

We will iterate through the array found on property `a`, and return both `a` and `b` variables.  It will return the following values:

	{"b": "done", "a": 1}
	{"b": "done", "a": 2}
	{"b": "done", "a": 3}


**Bad - Property follows array**

The same query, but different JSON with `b` following `a`:

	json = {"a": [1, 2, 3], "b": "done"}
	parse(json, path="a", required_vars=["a", "b"]}

Since property `b` follows the array we're iterating over, this will raise an error. 

**Good - No need for following properties**

The same JSON, but different query, which does not require `b`:

	json = {"a": [1, 2, 3], "b": "done"}
	parse(json, path="a", required_vars=["a"]}

If we do not require `b`, then streaming will proceed just fine:

	{"a": 1}
	{"a": 2}
	{"a": 3}

**Complex Objects**

This streamer was meant for very long lists of complex objects.  Use dot-delimited naming to refer to full name of the property

	json = [{"a": {"b": 1, "c": 2}}, {"a": {"b": 3, "c": 4}}, ...
	parse(json, path=".", required_vars=["a.c"])

The dot (`.`) can be used to refer to the top-most array.  Notice the structure is maintained, but only includes the required variables.

	{"a": {"c": 2}}
	{"a": {"c": 4}}
	...

**Nested Arrays**

Nested array iteration is meant to mimic a left-join from parent to child table; 
as such, it includes every record in the parent. 

	json = [
		{"o": 1: "a": [{"b": 1}: {"b": 2}: {"b": 3}: {"b": 4}]},
		{"o": 2: "a": {"b": 5}},
		{"o": 3}
	]
	parse(json, path=[".", "a"], required_vars=["o", "a.b"])

The `path` parameter can be a list, which is used to indicate which properties 
are expected to have an array, and to iterate over them.  Please notice if no 
array is found, it is treated like a singleton array, and missing arrays still 
produce a result.

	{"o": 1, "a": {"b": 1}}
	{"o": 1, "a": {"b": 2}}
	{"o": 1, "a": {"b": 3}}
	{"o": 1, "a": {"b": 4}}
	{"o": 2, "a": {"b": 5}}
	{"o": 3}




Module `jsons.ref`
==================

A JSON-like storage format intended for configuration files

Load your settings easily:

    settings = jsons.ref.get(url):

The file format is JSON, with three important features.

1. Comments
2. References (using `$ref`)
3. Parameterization


Comments
--------

End-of-line Comments are allowed, using either `#` or `//` prefix:

```javascript
    {
        "key1": "value1",  //Comment 1
    }
```

```python
        "key1": "value1",  #Comment 1
```

Multiline comments are also allowed, using either Python's triple-quotes
(`""" ... """`) or Javascript's block quotes `/*...*/`

```javascript
    {
        "key1": /* Comment 1 */ "value1",
    }
```

```python
        "key1": """Comment 1""" "value1",
```


Reference Other JSON
--------------------

The `$ref` key is special.  Its value is interpreted as a URL pointing to more JSON

**Absolute Internal Reference**

The simplest form of URL is an absolute reference to a node in the same
document:


```python
    {
        "message": "Hello world",
        "repeat": {"$ref": "#message"}
    }
```

The reference must start with `#`, and the object with the `$ref` is replaced
with the value it points to:

```python
    {
        "message": "Hello world",
        "repeat": "Hello world"
    }
```

**Relative Internal References**

References that start with dot (`.`) are relative, with each additional dot
referring to successive parents.   In this case the `..` refers to the
ref-object's parent, and expands just like the previous example:

```python
    {
        "message": "Hello world",
        "repeat": {"$ref": "#..message"}
    }
```

**File References**

Configuration is often stored on the local file system.  You can in-line the
JSON found in a file by using the `file://` scheme:

It is good practice to store sensitive data in a secure place...

```python
    {# LOCATED IN C:\users\kyle\password.json
        "host": "database.example.com",
        "username": "kyle",
        "password": "pass123"
    }
```
...and then refer to it in your configuration file:

```python
    {
        "host": "example.com",
        "port": "8080",
        "$ref": "file:///C:/users/kyle/password.json"
    }
```

which will be expanded at run-time to:

```python
    {
        "host": "example.com",
        "port": "8080",
        "username": "kyle",
        "password": "pass123"
    }
```

Please notice the triple slash (`///`) is referring to an absolute file
reference.

**Object References**

Ref-objects that point to other objects (dicts) are not replaced completely,
but rather are merged with the target; with the ref-object
properties taking precedence.   This is seen in the example above: The "host"
property is not overwritten by the target's.

**Relative File Reference**

Here is the same, using a relative file reference; which is relative to the
file that contains this JSON

```python
    {#LOCATED IN C:\users\kyle\dev-debug.json
        "host": "example.com",
        "port": "8080",
        "$ref": "file://password.json"
    }
```

**Home Directory Reference**

You may also use the tilde (`~`) to refer to the current user's home directory.
Here is the same again, but this example can be anywhere in the file system.

```python
    {
        "host": "example.com",
        "port": "8080",
        "$ref": "file://~/password.json"
    }
```

**HTTP Reference**

Configuration can be stored remotely, especially in the case of larger
configurations which are too unwieldy to inline:

```python
    {
        "schema":{"$ref": "http://example.com/sources/my_db.json"}
    }
```

**Scheme-Relative Reference**

You are also able to leave the scheme off, so that whole constellations of
configuration files can refer to each other no matter if they are on the local
file system, or remote:

```python
    {# LOCATED AT SOMEWHERE AT http://example.com
        "schema":{"$ref": "///sources/my_db.json"}
    }
```

And, of course, relative references are also allowed:

```python
    {# LOCATED AT http://example.com/sources/dev-debug.json
        "schema":{"$ref": "//sources/my_db.json"}
    }
```

**Fragment Reference**

Some remote configuration files are quite large...

```python
    {# LOCATED IN C:\users\kyle\password.json
        "database":{
            "username": "kyle",
            "password": "pass123"
        },
        "email":{
            "username": "ekyle",
            "password": "pass123"
        }
    }
```

... and you only need one fragment.  For this use the hash (`#`) followed by
the dot-delimited path into the document:

```python
    {
        "host": "mail.example.com",
        "username": "ekyle"
        "password": {"$ref": "//~/password.json#email.password"}
    }
```

**Environment Variables Reference**

json.ref uses the unconventional `env` scheme for accessing environment variables:

```python
    {
        "host": "mail.example.com",
        "username": "ekyle"
        "password": {"$ref": "env://MAIL_PASSWORD"}
    }
```

Parametrized JSON
-----------------

JSON documents are allowed named parameters, which are surrounded by moustaches `{{.}}`.

```javascript
	{//above_example.json
	 	{{var_name}}: "value"
	}
```

Parameter replacement is performed on the unicode text before being interpreted by the JSON parser.  It is your responsibility to ensure the parameter replacement will result in valid JSON.

You pass the parameters by including them as URL parameters:

	{"$ref": "//~/above_example.json?var_name=%22hello%22"}

Which will expand to

```javascript
	{
	 	"hello": "value"
	}
```

The pipe (`|`) symbol can be used to perform some common conversions


```javascript
	{
	 	{{var_name|quote}}: "value"
	}
```

The `quote` transformation will deal with quoting, so ...

	{"$ref": "//~/above_example.json?var_name=hello"}

... expands to the same:

```javascript
	{
	 	"hello": "value"
	}
```

Please see [`expand_template`](../README.md) for more on the parameter replacement, and transformations available

---

also see [http://tools.ietf.org/id/draft-pbryan-zyp-json-ref-03.html](http://tools.ietf.org/id/draft-pbryan-zyp-json-ref-03.html)
