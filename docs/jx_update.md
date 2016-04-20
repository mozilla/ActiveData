JSON Expressions for Changing Documents
=======================================

JSON Expressions have limited update features, assuming they are even enabled for your chosen document container.

`insert` Command
----------------

Add documents to the container

	{"insert": [doc1, doc2, ...]}


`update` Command
----------------

The `update` command accepts up to three parameters: `set`, `clear` and `where`

	{"update": {
		"where": filter,
		"clear": [path1, path2, ...],
		"set": {
			leaf: expression
			...
		}
	}}

* The `where` clause is an expression that must return `true` for the documents to be updated. If this clause is missing, all records are updated.
* The `clear` clause is used to remove values from the document. The path(s) can refer to specific primitive values or whole objects. Clearing is done **before** the `set` clause.
* The `set` clause is a dictionary that assigns leaves to the given value. A `leaf` is a dot-separated path into the document. The `expression` can be an [JSON Expression](jx_expressions.md). A literal value can be wrapped in `{"literal": value}`.


Removing Documents
------------------

Removing documents is done by clearing them

	{"update": {
		"where": filter,
		"clear": ".",
	}}

The dot (`.`) refers to the whole document; by clearing it you are effectively remove it.

