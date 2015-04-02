

Function: `jsons.encode.json_encoder()`
-------------------------------------

Fast JSON encoder used in `convert.value2json()` when running in Pypy.  Run the
[speedtest](https://github.com/klahnakoski/pyLibrary/blob/master/tests/speedtest_json.py)
to compare with default implementation and ujson


Module `jsons.ref`
==================

A JSON-like storage format intended for configuration files

Load your settings easily:

    settings = jsons.ref.get(url):

The file format is JSON, with three important features.

* Comments
* References (using `$ref`)
* Parameterization


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
file that contains

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
	{
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

Please see [`exapand_template`](../README.md) for more on the parameter replacement, and transformations available





also see [http://tools.ietf.org/id/draft-pbryan-zyp-json-ref-03.html](http://tools.ietf.org/id/draft-pbryan-zyp-json-ref-03.html)
