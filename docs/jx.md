JSON Query Expression Documentation
===================================

JSON query expressions are structures that mimic SQL query semantics; each
property corresponds to a SQL clause. There are some differences from SQL,
especially when it comes to using default clauses, but I hope your knowledge
of SQL can jump-start your use of JSON Expressions.

Specific Reading
----------------

* [Tutorial](jx_tutorial.md) - For some examples
* [Select Clause](jx_clause_select.md) - Data transformation using the `select` clause
* [Edges Clause](jx_clause_edges.md) - Grouping and Aggregation using the `edges` clause
* [Window Clause](jx_clause_window.md) - Using window functions
* [Expressions](jx_expressions.md) - Covers all the other expressions
* [Time Math](jx_time.md) - Writing expressions in the time domain
* [Commands](jx_update.md) - Update data with JSON Expressions
* [Reference](jx_reference.md) - A bare list of allowed clauses and options for JSON query expressions


Motivation
----------

JSON Expressions have the following benefits

* Simplified expressions over unclean data.
* Ability to translate shape of the data.
* Extract data as pivot tables and data frames
* Easy Meta-Programming
* Language independence


Non-Goals
---------

* **Use something better than JSON** - JSON expressions are deliberately a
JSON specification; and avoids the complexities of defining a DSL syntax.
Using an existing language is not an option, because each language makes a
design choice that conflicts with JSON Expressions somewhere. That said, SQL
has a lot of overlap: Mapping a subset of SQL to a subset of JSON
Expressions may be useful.
* **Extend to a procedural language** - JSON expressions are meant to be
purely functional, adding procedural features is much more work, and outside
the objective of providing concise data transformation.
* **Joins** - There is currently no attempt to provide clauses for joins.
Although, there are some JSON Expressions forms that can be abused to perform
joins.
* **Graph Operations** - Graph traversal, aggregation, or SQL's `CONNECT BY`
are not implemented.
* **Under development** - The JSON Expressions specification is not fully
implemented, and the specification itself is incomplete. What does exist has
tests to maintain stability.

More about the Benefits
-----------------------

### Expression Simplification

JSON Expressions provide data transformation and expressions over multi-
dimensional and unclean data. It simplifies expressions by defining
[`null` as out-of-context](https://github.com/klahnakoski/pyLibrary/tree/dev/pyLibrary/dot#null-is-the-new-none).
The *out-of-context* definition is different than the definition used by
many other languages; which means every operator and expression must be
translated from JSON Expressions to the destination language. This translation
is not complicated, just annoying. Here is a definition of the `add`
function:

#### Summation with `nulls`

The summation aggregate is probably the simplest example of how the definition
of `null` impacts the definition of all other functions. Databases use this
definition.

JSON Expression

	{"add": ["a", "b"]}

Javascript equivalent

	function add(a, b){
		if (a==null) return b;
		if (b==null) return a;
		return a + b;
 	}


#### Dereferencing with `nulls`

The dot operator is a function. It too is impacted by the definition of `null`.
Databases have only primitive concepts of objects and dereferencing.

JSON Expression

	"a.b"

Javascript equivalent

	function get(a, x){ //x=="b"
		if (a==null) return null;
		if (x==null) return null;
		if (a[x]===undefined) return null;
		return a[x];
 	}


Using the *out-of-context* definition, expressions, list comprehensions, and
query expressions are all simplified:  

1. **`null` checks are avoided** - all `null` checks are built into every function
2. **boundary checks are avoided** - window functions are not required to
verify boundaries because all points outside a domain map to `null`.  

### Translating Data Shape

JSON Expressions operate on JSON, with a focus on translating arrays of JSON.
JSON expressions are not limited to arrays, and work on other (un)ordered sets
that come out of databases and document stores. The [`select` clause](jx_clause_select.md)
is responsible for record-wise translation.


### Pivot Tables and Data Frames

A specific type of data transformation involves converting general sets into
data frames, which pivot tables are specific instances. The full domain of
each dimension is representing in a data frame, an that domain is not affected
by the filter; and can result in a sparse matrix. The columns of a SQL
`group by` clause have their domain affected by the resultant rows, a denser
data set, but missing domain values. 

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

An inspection of this table may have you conclude AL (Alaska) does not exist. A pivot table does not have this problem:

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

Alaska will show, despite it having no employees. Furthermore, filtering
employees by some criterion will continue to return the same number of
rows, only with the `count` changed.  

Meta-Programming
----------------

The `from` expression is the most complex; covering set operations, list
comprehensions, and relational operators; each shaped by the variety of
clauses the `from` expression accepts. These clauses can be programatically
composed because JSON is just data. In practice this happens most when
specifying query domains.


History
=======

Original Implementation
-----------------------

JSON Expressions were originally designed to send complex aggregation queries
to Elasticsearch version 0.90.x. In that version, ES only had "facets"; which
limited grouping data on a single property. In order to group by multiple
columns you had to provide a server side script to concatenate columns,
and the complementary script on the client to break them apart. JSON
Expressions was a Javascript library that did the script generation and
provided a simpler interface. It is still in use now by
[MoDevMetrics](https://github.com/klahnakoski/MoDevMetrics) and
[charts](https://github.com/mozilla/charts) which read off an old, but
perfectly functional, ES cluster.

ElasticSearch now has aggregations, and the JSON expression translation layer
is simplified, but the pivot table extraction, and expression simplification
is still required.

Exploring Possible Expression Formats
-------------------------------------

When serializing data structures, specifically data structures involving
function operations, there are three common operator positions:

* Prefix - ```+ a b```
* Infix  - ```a + b```
* Suffix - ```a b +```

Encoding these as JSON objects gives us:

* Prefix - ```{"add": {"a": "b"}}```
* Infix  - ```{"a": {"add": "b"}}```
* Suffix - ```{"a": {"b": "add"}}```

Personally, I find infix ordering aesthetically pleasing in the limited case
of binary commutative operators. Unfortunately, many operators have
a variable number of operands, which makes infix clumsy.

Previous Work
-------------

Even if I believe infix should not be used, there is still benefit
to reusing existing JSON-encoded operations found in other applications
But, it seems no planning was put into the existing serializations:

* MongoDB uses a combination of [infix notation](http://docs.mongodb.org/manual/reference/operator/query/gt/#op._S_gt),
[prefix notation](http://docs.mongodb.org/manual/reference/operator/query/and/#op._S_and),
and [nofix notation](http://caffinc.com/blog/2014/02/mongodb-eq-operator-for-find/),
which is clearly a mess.
* ElasticSearch has standardized on a [prefix notation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html),
and has some oddities like the [range filter](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-filter.html)
which is a combination of prefix and suffix, and probably a side effect of some
leaky abstraction.


| Operation                     | JSON Expression                | MongoDB                           | ElasticSearch                       |
|:------------------------------|:-------------------------------|:----------------------------------|:------------------------------------|
|Equality                       |`{"eq": {field: value}}`        |`{field: value}`                   |`{"term": {field: value}}`           |
|Inequality `gt, gte, ne, lte, lt`|`{"gt": {field: value}}`      |`{field: {"$gt": value} }`         |`{"range": {field: {"gt": value}}}`  |
|Logical Operators `and, or`    |`{"and": [a, b, c, ...]}`       |`{"$and": [a, b, c, ...]}`         |`{"and": [a, b, c, ...]}`            |
|Match All                      |`true`                          |`{}`                               |`{"match_all": {}}`                  |
|Exists                         |`{"exists": field}`             |`{field: {"$exists": true}}`       |`{"exists": {"field": field}}`       |
|Missing                        |`{"missing": field}`            |`{field: {"$exists": false}}`      |`{"missing": {"field": field}}`      |
|Match one of many              |`{"in": {field:[a, b, c, ...]}` |`{field {"$in":[a, b, c, ...]}`    |`{"terms": {field: [a, b, c, ...]}`  |
|Prefix                         |`{"prefix": {field: prefix}}`   |`{field: {"$regex": /^prefix\.*/}}`|`{"prefix": {field: prefix}}`        |
|Regular Expression             |`{"regex": {"field":regex}`     |`{field: {"$regex": regex}}`       |`{"regexp":{field: regex}}`          |
|Script                         |`{"script": javascript}`        |`{"$where": javascript}`           |`{"script": {"script": mvel_script}}`|

**Special note on nulls**
  * JSON Expressions - null values do not `exists` and are considered `missing`
  * MongoDB and ES - null values `exist` and are not `missing`

Prefix Operator Benefits
------------------------

Consistent use of the prefix operator gives us additional benefit:

* **Operator namespace** - If we can assume the JSON property names are
operators, in their own namespace exclusive of variable names, we do not need
an operator prefix, like MongoDb's dollar sign (`$`).
* **Familiar** - Prefix operators also read like functional notation, which
gives it familiarity.
* **Clauses** - If property names are operators, the additional properties on
the same object can act as operator modifiers, or "clauses". Clauses allow us
to define trinary operators, and beyond, naturally. They allow us to override
default behaviour of common operators in the face of missing values. And,
allow us to mimic multi-clause languages, like SQL.


