Window Functions
----------------


`window` functions are exclusively used for marking up the result-set, they do not change the number of rows returned, and each window adds just one extra column to the result-set.

		"window": [{
		    "select": "rows.last.result.ok",
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

Which can be used in an expression to calculate a variety of values, including the `result` of the latest build: `rows.last.result.ok`


Having Clause
-------------




