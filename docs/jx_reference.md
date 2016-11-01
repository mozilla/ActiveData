JSON Query Expressions Reference
==============================

Intended Audience
-----------------

This document is only a reference document. It is expected the reader already
knows how to write JSON query expressions. For a tutorial, start [here](jx_tutorial.md)

Nomenclature
------------

The nomenclature closely follows that used in business intelligence.

  - **cube** – a set of values in an n-space. A good example for n=2 is a 
  spreadsheet.
  - **edge** – each edge defines a dimension of the cube and the topology of 
  that dimension. Our spreadsheet example has two dimensions: Rows and Columns.
  - **domain** – every edge has a domain which defines it’s valid values. The 
  spreadsheet's rows have natural numbers as their domain (1, 2, 3, ...) and 
  the columns are in the alphabet domain (A, B, C, ....)
  - **partition** – every domain can be partitioned in multiple ways. Each 
  partition is an ordered array of mutually exclusive parts that cover the 
  domain. In the case of the spreadsheet, you may want to group many rows, or 
  many columns together and treat them all the same. Maybe columns are retail 
  outlets, grouped by region, and rows are customers, group by demographic
  - **part** – one part of a partition. Eg "north-east region", or "under 20"
  - **part objects** - Partitions are often an array of objects, rather than 
  an array of values. These objects describe the part better than a value can.  
  For example: `{"name": "NorthEast", "director":"Ann"} {"name":"Under 20", 
  "display color":"blue"}`
  - **coordinates** - a unique tuple representing one part from each edge: 
  Simply an array of part objects.
  - **cell** – the conceptual space located at a set of coordinate
  - **fact** - the value/number/object in the cell at given coordinates
  - **attribute** - any one coordinate, or fact
  - **record/row** – analogous to a database row. In the case of a cube, there 
  is one record for every cell: which is an object with many attributes
  - **column** – analogous to a database column, a dimension or atribute

Order of Operations
-------------------

Each of the clauses are executed in a particular order, irrespective of their
order in the JSON structure.   This is most limiting in the case of the
where clause. Use sub queries to get around this limitation for now.

  - **from** – the array, or list, to operate on. Can also be the results of 
  a query, or an in-lined sub-query.
  - **edges** – definition of the edge names and their domains
  - **groupby** - names of the attributes to group by
  - **where** – early in the processing to limit rows and aggregation: has 
  access to domain names
  - **select** – additional aggregate columns added
  - **window** – window columns added
  - **having** - advanced filtering
  - **sort** – run at end, but only if output to a list.
  - **isLean** - used by ElasticSearch to use `_source` on all fields

Query Clauses
=============

Queries are complex operators over sets, tables, and lists. Technically, 
queries are `from` operators with a variety of optional clauses that 
direct data transformation.

`from` Operator
---------------

The `from` operator accepts one parameter: the table, an index, or relation that 
is being processed by the query. In Javascript this can be an array of 
objects, a cube, or another JSON query expression. In the case of Elasticsearch, this 
is the name of the index being scanned. Nested Elasticsearch documents can be pulled by 
using a dots (.) as a path separator to nested property.

Example: Patches are pulled from the BugzillES cluster:

    {
    "from":"bugs.attachments",
    "select":"_source",
    "where":{"eq":{"ispatch": "1"}}
    }

Example: Pull review requests from BZ:

    {
    "from":"bugs.attachments.flags",
    "select":"_source",
    "where":{"eq":{"request_status":"?"}}
    }

`select` Clause
---------------

The select clause can be a single object, or an array of objects. The former 
will result in nameless value inside each cell of the resulting cube. The 
latter will result in an object, with given attributes, in each cell.

Here is an example counting the current number of bugs (open and closed) in 
the KOI project:

    {
    "from":"bugs",
    "select":{"name":"num bugs", "value":"bug_id", "aggregate":"count"},
    "where": {"and":[
        {"gte":{"expires_on":"{{now}}"},
        {"eq":{"cf_blocking_b2g":"koi+"}}
    ]}
    }

We can pull some details on those bugs

    {
    "from":"bugs",
    "select":[
        {"name":"bug number", "value":"bug_id"},
        {"name":"owner", "value":"assigned_to"}
    ],
    "where":{"and":[
        {"gte":{"expires_on":"{{now}}"}},
        {"eq":{"cf_blocking_b2g":"koi+"}}
    ]}
    }

if you find the `select` objects are a little verbose, and you have no need 
to rename the attribute, they can be replaced with simply the value:

    {
    "from":"bugs",
    "select":["bug_id", "assigned_to", "modified_ts"],
    "where": {"and":[
        {"gte":{"expires_on":"{{now}}"}},
        {"eq":{"cf_blocking_b2g":"koi+"}}
    ]}
    }



  - **name** – The name given to the resulting attribute. Optional if `value` 
  is a simple variable name.
  - **value** – Expression to calculate the result value
  - **aggregate** – one of many aggregate operations
  - **default** to replace null in the event there is no data
  - **sort** – one of `increasing`, `decreasing` or `none` (default). Only 
  meaningful when the output of the query is a list, not a cube.

`select.aggregate` Subclause
----------------------------

The `aggregate` sub-clause directs the particular aggregation 

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
  - **first** - return first value observed (assuming ordered `from` clause)
  - **any** - return any value observed (that is not null, of course)
  - **percentile** – return given percentile
    - **select.percentile** defined from 0.0 to 1.0 (required)
    - **select.default** to replace null in the event there is no data
  - **median** – return median (percentile = 50%)
  - **middle** - return middle percentile, a range min, max that ignores total 
  and bottom (1-middle)/2 parts
    - **select.percentile** defined from 0.0 to 1.0 (required)
    - **select.default** to replace null in the event there is no data
  - **join** – concatenate all values to a single string
    - **select.separator** to put between each of the joined values
  - **array** - return an array of values (which can have duplicates)
    - **select.sort** - optional, to return the array sorted
  - **list** - return an list of values (alternate name for array aggregate)
    - **select.sort** - optional, to return the array sorted
  - **union** - return an array of unique values. In the case of javascript, 
  uniqueness is defined as the string the object can be coerced to 
  (`""+a == ""+b`).
    - **select.limit** - limit on the size of the set

All aggregates ignore the null values; If all values are null, it is the same 
as having no data.


`where` Clause
------------

The `where` clause is [an expression](jx_Expresions.md) that returns a Boolean 
indicating whether the document will be included in the aggregate. If the 
query is returning a pivot-table, or data cube, the where clause does not 
affect the dimensions' domains.

`edges` Clause
--------------

The edges clause is used to produce pivot tables and data cubes; each edge 
defines a side. Each edge is a column which SQL group-by will be applied; with 
the additional stipulation that all parts of all domains are represented, even 
if null (count==0).

  - **name** – The name given to the resulting edge (optional, if the value is 
  a simple attribute name)
  - **value** – The expression to generate the edge value before grouping
  - **range** – Can be used instead of value, but only for algebraic fields: 
  In which case, if the minimum of a domain part is in the range, it will be 
  used in the aggregate.
      - **min** – The expression that defines the minimum value
      - **max** – The expression defining the supremum (of all values greater 
      than the range, pick the smallest)
      - **mode** – `inclusive` will ensure any domain part that intersects 
      with the range will be used in the aggregate. `snapshot` (default) will 
      only count ranges that contain the domain part key value.
  - **test** – Expression to be used instead of value: It must return a 
  Boolean indicating if the data will match the domain parts. Use this to 
  simulate a SQL join.
  - **domain** – The range of values to be part of the aggregation
  - **allowNulls** – Set to `true` (default) if you want to aggregate all 
  values outside the domain. 

`edges.domain` Subclause
------------------------

The domain is defined as an attribute of every edge. Each domain defines a covering partition.

  - **name** – Name given to this domain definition, for use in other code in 
  the query (default to `type` value).
  - **type** – One of a few predefined types (Default `{"type":"default"}`)
  - **limit** - for `"type": "default"` domains; limit the number of parts that will be returned 
  - **value** – Domain partitions are technically JSON objects with 
  descriptive attributes (name, value, max, min, etc). The value attribute is 
  code that will extract the value of the domain after aggregation is complete.
  - **key** – Code to extract the unique key value from any part object in a 
  partition. This is important so a 1-1 relationship can be established – 
  mapping fast string hashes to slow object comparisons.


`edges.domain.type` Subclause
-----------------------------

Every edge must be limited to one of a few basic domain types. Which further 
defines the other domain attributes which can be assigned.

  - **default**- For when the type parameter is missing: Defines parts of 
  domain as an unlimited set of unique values. Useful for numbers and strings, 
  but can be used on objects in general.
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
      - **edge.domain.partitions** – the set of values allowed. These can be 
      compound objects, but `edge.test` and `edge.domain.value` need to be defined.
  - **range** - A list of ranges, probably not of the same interval, over some 
  algebraic field. The ranges can have holes, but can not overlap.
      - **edge.domain.partitions.N.min** - minimum value for this partition
      - **edge.domain.partitions.N.max** - supremum value for this partition

`window` Clause
---------------

The `window` clause defines a sequence of window functions to be applied to 
the result set. Each window function defines an additional attribute, and 
does not affect the number of rows returned. For each window, the data is 
grouped, sorted and assigned a `rownum` attribute that can be used to 
calculate the attribute value.

  - **name** – name given to resulting attribute
  - **value** – a JSON expression used to determine the attribute value. The 
  functions is passed three special variables:
      - `row` – the row being processed
      - `rownum` – an integer, starting at zero for the first row
      - `rows` – an array of all data in the group.
  - **edges** – an array of column names used to determine the groups
  - **where** – code that returns true/false to indicate if a record is a 
  member of any group. This will not affect the number of rows returned, 
  only how the window is calculated. If where returns false then rownum and 
  rows will both be null: Be sure to properly handle those values in your code.
  - **sort** – a single attribute name, or array of attribute names, used to 
  sort the members of each group
  - **range** - the interval which the window function will apply, outside the 
  range the `row` is null. Only makes sense when **sort** is defined
      - **min** - offset from `rownum` where window starts
      - **max** - offset from `rownum` where window ends (`rows[rownum + max] == null`)
  - **aggregate** - an aggregate function to apply on **value** over the 
  **range**, (or whole group if range is not defined)

**Please note: The javascript JSON Expressions library uses "analytic" instead of "window".**

having
------

The `having` clause is a filter that uses aggregates and partitions to 
determine inclusion in the resultant cube.

  - **edges** – an array of column names used to determine how the rows are 
  partitioned
  - **sort** – a single attribute name, or array of attribute names, used to 
  declare the rank of every row in the group
  - **aggregate** - an aggregate function used to determine which row is selected

Pre-Defined Dimensions
----------------------

Pre-defined dimensions simplify queries, and double as type information for 
the dataset. In this project [`Mozilla.*` have been pre-defined](https://github.com/klahnakoski/Qb/blob/master/html/es/js/Dimension-Bugzilla.js).
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


