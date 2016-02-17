JSON Query Expression Documentation
======================

JSON query expressions are JSON structures that mimic SQL query semantics; each property corresponds to a SQL clause.  There are some differences from SQL, especially when it comes to using default clauses, but I hope your knowledge of SQL can jump-start your use of JSON Expressions.

Motivation
----------

JSON Expressions provides ...

* Simplified query expressions over unclean data.
* Ability to translate shape of the data.
* Extract data as pivot tables and data frames
* Some language independence


Non Goals and Limitations
-------------------------

Are JSON Expressions for you?

Non-Goals
---------

* **Use something better than JSON** - JSON expressions are deliberately a JSON specification; and avoids the complexities of defining a DSL syntax.  Using another language is not an option, because each language makes a design choice that conflicts with JSON Expressions somewhere.  SQL has a lot of overlap:  Mapping a subset of SQL to a subset of JSON Expressions may be useful.      
* **Extend to a procedural language** - JSON expressions are meant to be purely functional, adding procedural features is much more work, and outside the objective of providing concise data transformation expressions.
* **Joins** - There is currently no attempt to provide clauses for joins.  Although, there are some JSON Expressions expression forms that can be abused to perform joins.
* **Graph Operations** - Graph traversal, aggregation, or SQL's `CONNECT BY` are not implemented.
* **Under development** - The JSON Expressions specification is not fully implemented, and the specification itself is incomplete.  What does exist has tests to maintain stability.

### Expression Simplification

JSON Expressions provides data transformation and expressions over multi-dimensional and unclean data.  It simplifies expressions by defining [`null` as out-of-context](https://github.com/klahnakoski/pyLibrary/tree/dev/pyLibrary/dot#null-is-the-new-none).  The *out-of-context* definition is different than the definition used by many other languages; which means every operator and expression must be translated from JSON Expressions to the destination language.  This translation is not complicated, just annoying.  Here is a definition of the `add` function:

#### Summation with `nulls`

The summation aggregate is probably the simplest example of how the definition of `null` impacts the definition of all other functions.  Databases use this definition.

JSON Expression

	{"add": ["a", "b"]}

Javascript equivalent

	function add(a, b){
		if (a==null) return b;
		if (b==null) return a;
		return a + b;
 	}


#### Dereferencing with `nulls`

The dot operator is a function. It too is impacted by the definition of `null`.  Databases have only primitive concepts of objects and dereferencing.

JSON Expression

	"a.b"

Javascript equivalent

	function get(a, x){  //x=="b"
		if (a==null) return null;
		if (x==null) return null;
		if (a[x]===undefined) return null;
		return a[x];
 	}


Using the *out-of-context* definition,  expressions, list comprehensions, and query expressions are all simplified:  

1. **`null` checks are avoided** - all `null` checks are built into every function 
2. **boundary checks are avoided** - window functions are not required to verify boundaries because all points outside a domain map to `null`.  

### Translating Data Shape

JSON Expressions operates on JSON, with a focus on translating arrays of JSON, which is still just JSON.  JSON expressions are not limited to arrays, and works on other (un)ordered sets that come out of databases and document stores.  The [`select` clause](jx_Clause_Select.md) is responsible for record-wise translation.


### Pivot Tables and Data Frames

A specific type of data transformation involves converting general sets into data frames, which pivot tables are specific instances.  The full domain of each dimension is representing in a data frame, an that domain is not affected by the filter; and can result in a sparse matrix.  The columns of a SQL `group by` clause have their domain affected by the resultant rows, a denser data set, but missing domain values. 

Here is an example that shows the problem.  

<table><tr><td>
<b>SQL</b><br>
<pre>
SELECT
	state,
	count(id) as `count`
FROM
	employees
GROUP BY
	state
ORDER BY
	state
</pre>
</td><td>
<b>Result</b><br>
<pre>
| state | count |
| ----- | ----- |
|  AL   |    1  |
|  AZ   |   10  |
|  AR   |    4  |
|  CA   |  467  |
|  CO   |    6  |
|  CT   |    7  |
...
</pre>
</td></tr></table>

An inspection of this table may have you conclude AL (Alaska) does not exist.  A pivot table does not have this problem:

<table><tr><td>
<b>JSON Query Expression</b><br>
<pre>
{
	"select": {
		"name":"count", 
		"value":"id", 
		"aggregate":"count
	},
	"from": "employees",
	"edges":["state"]
	"sort": "state"
}
</pre>
</td><td>
<b>Result (as a table)</b><br>
<pre>
| state | count |
| ----- | ----- |
|  AL   |    1  |
|  AK   |    0  |
|  AZ   |   10  |
|  AR   |    4  |
|  CA   |  467  |
|  CO   |    6  |
|  CT   |    7  |

</pre>
</td></tr></table>

Alaska will show, despite it having no employees.  Furthermore, filtering employees by some criterion will continue to return the same number of rows, only with the `count` changed.  


History
-------

JSON Expressions was originally designed to send complex aggregation queries to Elasticsearch version 0.90.x.  In that version, ES only had "facets"; which limited grouping data on a single property.  In order to group by multiple columns you had to provide a server side script to concatenate multiple columns, and the complementary script on the client to break them apart.  JSON Expressions was a Javascript library that did the script generation and provided a simpler interface.  It is still in use now by [MoDevMetrics](https://github.com/klahnakoski/MoDevMetrics) and [charts](https://github.com/mozilla/charts) which read off an old, but perfectly functional, ES cluster.

ElasticSearch now has aggregations, and the JSON Expressions translation layer is simplified, but the pivot table extraction, and expression simplification is still required.

##More Reading

* [Tutorial](jx_Tutorial.md) - For some examples
* [Select Clause](jx_Clause_Select.md) - Data transformation using the `select` clause
* [Window Clause](jx_Clause_Window.md) - Using window functions
* [Expressions](jx_Expressions.md) - Covers all the other expressions
* [Time Math](jx_Time_Math.md) - Writing expressions in the time domain
* [Commands](jx_Update.md) - Update data with JSON Expressions
* [Reference](Reference.md) - A bare list of allowed clauses and options for JSON query expressions
