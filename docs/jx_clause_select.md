
`select` Clause
===============


Introduction
------------

Getting a feel for the JSON Expressions `select` clause is easiest done by comparing to SQL. Just like SQL, the `select` clause is used for renaming:

<table><tr><td>
<b>JSON Query Expression</b><br>
<pre>
{
    "select": [
        {"name":"name", "value":"person"},
        {"name":"age", "value":"years"}
    ],
    "from": test
}
</pre>
</td><td>
<b>SQL</b><br>
<pre>

SELECT
	person AS name,
	years AS age
FROM
	test 

</pre>
</td></tr></table>

JSON expressions are clearly more verbose than SQL, but there are many shortcuts to mitigate this, plus the `select` clause can do more than SQL.


The Standard `select` Clause
----------------------------

The select clause is used for both selecting particular values, and for creating new structures from those values. A select clause is a list that maps the leaves of one data structure to the leaves of another.

	"select": [
		{"value": source_path_1, "name": destination_path_1}
		{"value": source_path_2, "name": destination_path_2}
		...
	]

The path separator is the dot (`.`), with literal dots escaped using backslash. Paths need not point to primitive values, and can refer to objects.


### Example Transformation

**Source Data**

	test = [{
	    "build": {
	        "branch": {
	            "name": "mozilla-inbound",
	            "locale": "en-US"
	        }
	        "date": "2015-09-18"
	    },
	    "run": {
	        "suite": "mochitest"
	        "chunk": 2
	    }
	}]

**Query**

	{
	    "from": test,
	    "select": [
	        {"name":"branch.name", "value":"build.branch.name"},
	        {"name":"suite.name", "value":"run.suite"},
	        {"name":"suite.chunk", "value":"run.chunk"}
	    ],
	    "format": "list
	}

**Result**

	test = [{
	    "branch": {
	        "name": "mozilla-inbound"
	    },
	    "suite": {
	        "name": "mochitest"
	        "chunk": 2
	    }
	}]


`select` Simplifications
------------------------

All other select clause variations are shortcuts for the standard clause.

### Simple String

If the name, or path, of the value is not changed, a string will do just fine:

<table><tr><td>
<b>Use String Names</b><br>
<pre>
{
    "from": people,
    "select": [
        "person", 
        "years"
    ],
    "format": "list
}
</pre>

</td><td>
<b>Equivalent Standard Form</b><br>
<pre>
{
    "from": people,
    "select": [
        {"name":"person", "value":"person"},
        {"name":"years", "value":"years"}
    ],
    "format": "list"
}
</pre>
</td></tr></table>

### Selecting A Single Value

The standard `select` clause is used for returning objects and structures. You can return a single (unnamed) value by eliminating the array (`[]`):


<table><tr><td>
<b>Single Value Query</b><br>
<pre>
{
    "from": people,
    "select": "person",
    "format": "list
}
</pre>

<b>Example Result<b>
<pre>
[
    "Kyle",
    "Michael"
]
</pre>
</td><td>
<b>Object Query</b><br>
<pre>
{
    "from": people,
    "select": ["person"],
    "format": "list
}
</pre>

<b>Example Result<b>
<pre>
[
    {"person": "Kyle"},
    {"person": "Michael"}
]
</pre>
</td></tr></table>


### Selecting Self (`.`)

Sometimes the whole object is desired; in this case, use a single dot (`"."`). Since `"."` is the default select clause, you do not even need to specify it.

<table><tr><td>
<b>Select Self</b><br>
<pre>
{
    "from": people,
    "select": [{
        "name":"data", 
        "value": "."
    }],
    "format": "list
}
</pre>
<b>Result<b>
<pre>
[
    {"data": {
        "person": "Kyle", 
        "years": 42
    }},
    {"data": {
        "person": "Michael", 
        "years": 28
    }}
]
</pre>

</td><td>
<b>Select Self as Value</b><br>
<pre>
{
    "from": people,
    "select": ".",
    "format": "list
}



</pre>
<b>Result (original data)<b>
<pre>
[
    {
        "person": "Kyle", 
        "years": 42
    },
    {
        "person": "Michael", 
        "years": 28
    }
]
</pre>
</td><td>
<b>Implied Dot Query</b><br>
<pre>
{
    "from": people,
    "format": "list
}




</pre>
<b>Result (same original data)<b>
<pre>
[
    {
        "person": "Kyle", 
        "years": 42
    },
    {
        "person": "Michael", 
        "years": 28
    }
]
</pre>
</td></tr></table>

### Selecting Leaves (`*`)

The star (`*`) can be used to refer to all leaves of a path. This behaviour is different from dot (`.`): Dot does not change structure, the star will flatten it. Star is useful when converting data to table form.

<table><tr><td>
<b>Select All Leaves as List</b><br>
<pre>
{
    "from": test,
    "select": "*",
    "format": "list"
}
</pre>

<b>Result (list of shallow objects)</b><br><br>
<pre>
{
    "data": [{
        "build.branch.name": "mozilla-inbound",
        "build.branch.locale": "en-US",
        "build.date": "2015-09-18",
        "run.suite": "mochitest",
        "run.chunk": 2
    }]
}







</pre>

</td><td>


<b>Select all Leaves as Table</b><br>
<pre>
{
    "from": test,
    "select": "*",
    "format": "table"
}
</pre>
<b>Result (many primitive-value columns)</b><br>
<pre>
{
    "header": [
        "build.branch.name",
        "build.branch.locale",
        "build.date",
        "run.suite",
        "run.chunk"
    ],
    "data": [[
        "mozilla-inbound",
        "en-US",
        "2015-09-18",
        "mochitest",
        2
    ]]
}]
</pre>

</td><td>
<b>Select Self as Table</b><br>
<pre>
{
    "from": test,
    "select": ".",
    "format": "table"
}
</pre>
<b>Result (one complex-value column)</b><br><br>
<pre>
{
    "header": ["."],
    "data": [[
        "build": {
            "branch": {
                "name": "mozilla-inbound",
                "locale": "en-US"
            }
            "date": "2015-09-18"
        },
        "run": {
            "suite": "mochitest"
            "chunk": 2
        }
    ]]
}
</pre>



</td></tr></table>

The star can be used on the end of a path. The path is not flattened, but the structure it points to is.

<table><tr><td>
<b>Query as List</b><br>
<pre>
{
    "from": test,
    "select": "build.*",
    "format": "list"
}
</pre>
<b>Result</b><br>
<pre>
{
    "data": [
        {
            "branch.name": "mozilla-inbound",
            "branch.locale": "en-US",
            "date": "2015-09-18"
        }
    ]
}



</pre>
</td><td>
<b>Query as Table</b><br>
<pre>
{
    "from": test,
    "select": "build.*",
    "format": "table"
}
</pre>
<b>Result</b><br>
<pre>
{
    "header":[
        "branch.name",
        "branch.locale",
        "date"
    ],
    "data":[
        "mozilla-inbound",
        "en-US",
        "2015-09-18"
    ]
}
</pre>


</td></tr></table>


Aggregation
-----------

When a JSON query expressions includes a `groupby` or `edges` clause, the members of the `select` clause must include the `aggregate` property; which indicates what to do with the many possible values being assigned to the `name`.







  


