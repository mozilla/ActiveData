
Advanced Query Operations
=========================

Select Clause Variations
------------------------

The select clause is used for both selecting particular values, and for declaring the structure those values are contained in.  In every case, the result of a query will return a set of rows, and the select clause can be used to give those rows structure.

Selecting a single value, without a list, will ensure the row is identical to the value,
"select": "*"
"select": "."
"select": ".*"
"select": "a"
"select": "a.b"
"select": "a.*"
"select": [ 



Window Functions
----------------


`window` functions are exclusively used for marking up the result-set, they do not change the number of rows returned, and each adds just one extra column to the result-set.

		"window": [{
		    "select": "rows.last().result.ok",
		    "edges": [
		       "build.platform",
		        "build.type",
		        "run.suite",
		        "result.test"
		     ],
		    "sort":"build.date"
		}]

In this case, all rows in the result-set are grouped (aka edged) into buckets and sorted.  This gives you access to three extra properties:

* **row** - the current row in the sorted list
* **rownum** - the index of the row in the sorted list
* **rows** - the sorted list itself

Which can be used in an expression to calculate a variety of values, including the `result` of the latest build: `rows.last().result.ok`

Window functions can be found in Postgres' PSQL and Oracle's PLSQL, but Qb has changed the names of the clauses to match a SQL query better.  For me, when I saw Oracle add window functions to SQL back in the 90s, it was a big step toward removing any need for the looping constructs we commonly find in procedural code.


Having Clause
-------------




