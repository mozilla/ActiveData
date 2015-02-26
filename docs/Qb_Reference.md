Qb (pronounced kyo͞ob) Queries
==============================

Motivation
----------

[Data cubes](http://en.wikipedia.org/wiki/OLAP_cube) facilitate strong typing of data volumes.  Thier cartesian nature
makes counting and aggregation trivial, provably correct, operations. [MultiDimensional query eXpressions (MDX)](http://en.wikipedia.org/wiki/MultiDimensional_eXpressions)
takes advantage of the data cube uniformity to provide a simple query language to filter and group by.  Unfortunately,
MDX is too simple for general use, and requires copious up-front work to get the data in the cubic form required.

My experience with ETL has shown existing languages to be lacking:  Javascript, and procedural languages in general,
are not suited for general transformations because the logic is hidden in loops and handling edge case of those loops.
SQL has been my preferred ETL language because it can state many common data transformations simply, but [SQL has many
of it's own shortcomings](SQL Shortcomings.md)

I want to extend SQL with the good parts of MDX to provide a ETL data transformation language which will avoid common
ETL bugs.


Design
------

Generally, Qb queries are JSON structures meant to mimic the Abstract Syntax
Tree (AST) of SQL for the same.  It is hoped the similarity with SQL will
make it accessible to a wider audience.   The JSON nature is simply to avoid
making another language parser.

There are differences when it comes to ```group by``` and joins, but that
is the influence of MDX.

Intended Audience
-----------------

This document is only a reference document.  It is expected the reader already
knows how to write Qb queries.  For a tutorial, start [here](BZ_Tutorial.md)


Nomenclature
------------

The nomenclature closely follows that used in business intellegnce.

  - **cube** – a set of values in an n-space.  A good example for n=2 is a spreadsheet.
  - **edge** – each edge defines a dimension of the cube and the topology of that dimension.  Our spreadsheet example has two dimensions: Rows and Columns.
  - **domain** – every edge has a domain which defines it’s valid values.  The spreadsheet's rows have natual numbers as thier domain (1, 2, 3, ...) and the columns are in the alphabet domain (A, B, C, ....)
  - **partition** – every domain can be partitioned in multiple ways.  Each partition is an ordered array of mutually exclusive parts that cover the domain.  In the case of the spreadsheet, you may want to group many rows, or many columns together and treat them all the same.  Maybe columns are retail outlets, grouped by region, and rows are customers, group by demographic
  - **part** – one part of a partition.  Eg "north-east region", or "under 20"
  - **part objects** - Partitions are often an array of objects (with a name, value, and other attributes).  These objects usually represent the values along the axis of a chart.  Eg {"name": "NorthEast", "director":"Ann"} {"name":"Under 20", "display color":"blue"}
  - **coordinates** - a unique tuple representing one part from each edge: Simply an array of part objects.
  - **cell** – the conceptual space located at a set of coordinate
  - **fact** - the value/number/object in the cell at given coorinates
  - **attribute** - any one coordinate, which is a *part*
  - **record/row** – anaglous to a database row.  In the case of a cube, there is one record for every cell: which is an object with having attributes
  - **column** – anagolous to a database column, a dimension or an edges

ORDER OF OPERATIONS
-------------------
Each of the clauses are executed in a particular order, irrespective of their
order in the JSON structure.   This is most limiting in the case of the
where clause.  Use sub queries to get around this limitation for now.

  - **from** – the array, or list, to operate on.  Can also be the results of a query, or an in-lined subquery.
  - **edges** – definition of the edge names and their domains
  - **where** – early in the processing to limit rows and aggregation: has access to domain names
  - **select** – additional aggregate columns added
  - **window** – window columns added
  - **having** - advanced filtering
  - **sort** – run at end, but only if output to a list.
  - **isLean** - used by ElasticSearch to use _source on all fields

QUERY STRUCTURE
---------------

Queries are in a JSON structure which can be interpreted by ESQuery.js (for
ES requests, limited by ES’s functionality) and by Qb.js (for local
processing with Javascript).

from
----
The from clause states the table, index, or relation that is being processed
by the query.  In Javascript this can be an array of objects, a cube, or
another Qb query.  In the case of ES, this is the name of the index being
scanned. Nested ES documents can be pulled by using a dots (.) as a path
separator to nested property.

Example: Patches are pulled from the BZ

    {
    "from":"bugs.attachments",
    "select":"_source",
    "where": {"term":{"bugs.attachments[\"attachments.ispatch\"]":"1"}}
    }

Example: Pull review requests from BZ:

    {
    "from":"bugs.attachments.flags",
    "select":"_source",
    "where": {"term":{"bugs.attachments.flags.request_status" : "?"}}
    }

ESQuery.js can pull individual nested documents from ES.  ES on it’s own can only return a document once.  Aggregation
over nested documents is not supported.

select
------

The select clause can be a single attribute, or an array of attributes.  The former will result
in nameless value inside each cell of the resulting cube.  The latter will result in an object, with given
attributes, in each cell.

Here is an example counting the current number of bugs (open and closed) in the KOI project:

    {
    "from":"bugs",
    "select":{"name":"num bugs", "value":"bug_id", "aggregate":"count"},
    "where": {"and":[
        {"range":{"expires_on":{"gte":NOW}}},
        {"term":{"cf_blocking_b2g":"koi+"}}
    ]}
    }

We can pull some details on those bugs

    {
    "from":"bugs",
    "select":[
        {"name":"bug number", "value":"bug_id"},
        {"name":"owner", "value":"assigned_to"}
    ],
    "where": {"and":[
        {"range":{"expires_on":{"gte":NOW}}},
        {"term":{"cf_blocking_b2g":"koi+"}}
    ]}
    }

if you find the ```select``` objects are a little verbose, and you have no need to rename the attribute, they can be
replaced with simply the value:

    {
    "from":"bugs",
    "select":["bug_id", "assigned_to", "modified_ts"],
    "where": {"and":[
        {"range":{"expires_on":{"gte":NOW}}},
        {"term":{"cf_blocking_b2g":"koi+"}}
    ]}
    }



  - **name** – The name given to the resulting attribute.   Optional if ```value``` is a simple variable name.
  - **value** – Name of the attribute, or list of attributes, or lambda, or source code to generate the attribute value (MVEL for ES)
  - **aggregate** – one of many aggregate operations
  - **default** to replace null in the event there is no data
  - **sort** – one of ```increasing```, ```decreasing``` or ```none``` (default).  Only meaningful when the output of the query is a list, not a cube.

select.aggregate
----------------

The ```aggregate``` sub-clause has many options.  Unfortunately not all of them are available to queries destined for
ES.  ES only supports (count, sum, mean, variance).

  - **none** – when expecting only one value
  - **one** – when expecting all values to be identical
  - **binary** – returns 1 if value found, 0 for no value
  - **exists** – same as binary but returns boolean
  - **count** – count number of values
  - **sum** – mathematical summation of values
  - **average** – mathematical average of values
  - **geomean** - geometric mean of values
  - **minimum** – return minimum value observed
  - **maximum** – return maximum value observed
  - **first** - return first value observed (assuming ordered ```from``` clause)
  - **any** - return any value observed (that is not null, of course)
  - **percentile** – return given percentile
    - **select.percentile** defined from 0.0 to 1.0 (required)
    - **select.default** to replace null in the event there is no data
  - **median** – return median (percentile = 50%)
  - **middle** - return middle percentile, a range min, max that ignores total and bottom (1-middle)/2 parts
    - **select.percentile** defined from 0.0 to 1.0 (required)
    - **select.default** to replace null in the event there is no data
  - **join** – concatenate all values to a single string
    - **select.separator** to put between each of the joined values
  - **array** - return an array of values (which can have duplicates)
    - **select.sort** - optional, to return the array sorted
  - **list** - return an list of values (alternate name for array aggregate)
    - **select.sort** - optional, to return the array sorted
  - **union** - return an array of unique values.  In the case of javascript, uniquness is defined as the string the object can be coorced to (```""+a == ""+b```).

All aggregates ignore the null values; If all values are null, it is the same as having no data.


where
-----

Where clause is code to return true/false or whether the data will be included in the aggregate.  This does not impact
the edges; every edge is restricted to it’s own domain.

The elasticsearch.org's [documentation on filters](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-filters.html)
covers the types of filters and the format expected.

<table>
<tr>
<td>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-and-filter.html">{"and": list}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-exists-filter.html">{"exists": {"field": fieldName}}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-match-all-filter.html">{"match_all": {}}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-not-filter.html">{"not": filter}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-or-filter.html">{"or": list}</a><br>
</td>
<td>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-prefix-filter.html">{"prefix":{fieldName: prefix}}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-filter.html">{"range":{fieldName: limits}}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-script-filter.html">{"script":{"script": mvelCode}}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html">{"term": {fieldName: value}}</a><br>
<a href="http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-filter.html">{"terms": {fieldName: values}}</a><br>
</td>
</tr>
</table>


esfilter
--------

Similar to the where clause, but used by ES to filter the top-level documents only. The where clause can filter out
nested documents, esfilter can not.  esfilter is very fast and should be used whenever possible to restrict the data
being processed by scripts and facets.

edges
-----

The edges clause is an array of edge definitions.  Each edge is a column which SQL group-by will be applied; with the
additional stipulation that all parts of all domains will have values, even if null (count==0).

  - **name** – The name given to the resulting edge (optional, if the value is a simple attribute name)
  - **value** – The code to generate the edge value before grouping
  - **range** – Can be used instead of value,  but only for algebraic fields: In which case, if the minimum of a domain part is in the range, it will be used in the aggregate.
      - **min** – The code that defined the minimum value
      - **max** – The code defining the supremum (of all values greater than the range, pick the smallest)
  - **mode** – ```inclusive``` will ensure any domain part that intersects with the range will be used in the aggregate.  ```snapshot`` (default) will only count ranges that contain the domain part key value.
  - **test** – Can be used instead of value: Code that is responsible for returning true/false on whether the data will match the domain parts.  Use this to simulate a SQL join.
  - **domain** – The range of values to be part of the aggregation
  - **allowNulls** – Set to true if you want to aggregate all values outside the domain

edges.domain
--------------

The domain is defined as an attribute of every edge.  Each domain defines a covering partition.

  - **name** – Name given to this domain definition, for use in other code in the query (default to ```type``` value).
  - **type** – One of a few predefined types  (Default ```{"type":"default"}```)
  - **value** – Domain partitions are technically JSON objects with descriptive attributes (name, value, max, min, etc).  The value attribute is code that will extract the value of the domain after aggregation is complete.
  - **key** – Code to extract the unique key value from any part object in a partition.  This is important so a 1-1 relationship can be established – mapping fast string hashes to slow object comparisons.
  - **isFacet** – for ES queries:  Will force each part of the domain to have it’s own facet.  Each part of the domain must be explicit, and define ```edges[].domain.partition.esfilter``` as the facet filter.  Avoid using ```{"script"...}``` filters in facets because they are WAY slow.

edges.domain.type
-------------------

Every edge must be limited to one of a few basic domain types.  Which further defines the other domain attributes which can be assigned.

  - **default**- For when the type parameter is missing: Defines parts of domain as an unlimited set of unique values.  Useful for numbers and strings, but can be used on objects in general.
  - **time** – Defines parts of a time domain.
      - **edge.domain.min** – Minimum value of domain (optional)
      - **edge.domain.max** – Supremum of domain (optional)
      - **edge.domain.interval** – The size of each time part. (max-min)/interval must be an integer
  - **duration** – Defines an time interval
      - **edge.domain.min** – Minimum value of domain (optional)
      - **edge.domain.max** – Supremum of domain (optional)
      - **edge.domain.interval** – The size of each time part. (max-min)/interval must be an integer
  - **numeric** – Defines a unit-less range of values
      - **edge.domain.min** – Minimum value of domain (optional)
      - **edge.domain.max** – Supremum of domain (optional)
      - **edge.domain.interval** – The size of each time part. (max-min)/interval must be an integer
  - **count** – just like numeric, but limited to integers >= 0
  - **set** – An explicit set of unique values
      - **edge.domain.partitions** – the set of values allowed.  These can be compound objects, but ```edge.test``` and ```edge.domain.value``` need to be defined.

window
------

The `window` clause defines a sequence of window functions to be applied to the result set.  Each window function defines an additional attribute, and does not affect the  number of rows returned.  For each window, the data is grouped, sorted and assigned a ```rownum``` attribute that can be used to calculate the attribute value.

  - **name** – name given to resulting attribute
  - **value** – can be a function (or a string containing javascript code) to determine the attribute value.  The functions is passed three special variables:
      - ```row``` – the row being processed
      - ```rownum``` – which is integer, starting at zero for the first row
      - ```rows``` – an array of all data in the group.
  - **edges** – an array of column names used to determine the groups
  - **where** – code that returns true/false to indicate if a record is a member of any group.  This will not affect the number of rows returned, only how the window is calculated.  If where returns false then rownum and rows will both be null:  Be sure to properly handle those values in your code.
  - **sort** – a single attribute name, or array of attribute names, used to sort the members of each group
  - **range** - the interval which the window function will apply, outside the range the ```row``` is null.  Only makes sense when **sort** is defined
      - **min** - offset from ```rownum``` where window starts
      - **max** - offset from ```rownum``` where window ends (```rows[rownum + max] == null```)
  - **aggregate** - an aggregate function to apply on **value** over the **range**, (or whole group if range is not defined)

**Please note: The javascript Qb library uses "analytic" instead of "window".**

having
------

The `having` clause is a filter that uses aggregates and partitions to determine inclusion in the resultcube.

  - **edges** – an array of column names used to determine how the rows are partitioned
  - **sort** – a single attribute name, or array of attribute names, used to declare the rank of every row in the group
  - **aggregate** - an aggregate function used to determine which row is selected

Pre-Defined Dimensions
----------------------

Pre-defined dimensions simplify queries, and double as type information for the dataset.  In this project [```Mozilla.*```
have been pre-defined](https://github.com/klahnakoski/Qb/blob/master/html/es/js/Dimension-Bugzilla.js).
[More documentation on dimension definitions here](Dimension Definitions.md).

  - **select** - Any pre-defined dimension with a partition defined can be used in a select clause. Each record will be
  assigned it's part.

    <pre>var details=yield(ESQuery.run({
        "from":"bugs",
        "select":[
        	"bug_id",
    		<b>Mozilla.BugStatus.getSelect()</b>,
    		"assigned_to",
    		{"name":"dependson", "value":"get(_source, \"dependson\")"},
    		"status_whiteboard",
    		"component"
    	],
    	"esfilter":{"and":[
    		Mozilla.CurrentRecords.esfilter,
    		{"terms":{"bug_id":Object.keys(allBugs)}}
    	]}
    }));</pre>

  - **edge.domain** - Pre-defined dimensions can be used as domain values

    <pre>var chart=yield (ESQuery.run({
    	"from":"bugs",
    	"select": {"name":"num_bug", "value":"bug_id", "aggregate":"count"},
    	"edges":[
    		{"name":"type", allowNulls:true, "domain":<b>Mozilla.Projects["B2G 1.0.1 (TEF)"].getDomain()</b>},
    		{"name":"date",
    			"range":{"min":"modified_ts", "max":"expires_on"},
    			"allowNulls":false,
    			"domain":{"type":"time", "min":sampleMin, "max":sampleMax, "interval":sampleInterval}
    		}
    	]
    }));</pre>


  - **esfilter** - most commonly used in esfilters so that simple names replace complex filtering logic

    <pre>var q = yield(ESQuery.run({
    	"name":"Product Breakdown",
		"from":"bugs",
		"select":{"name":"count", "value":"bug_id", "aggregate":"count"},
		"edges":[
			{"name":"product", "value":"product"}
		],
		"esfilter":<b>Mozilla.BugStatus.Open.esfilter</b>
	});</pre>


