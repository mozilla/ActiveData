JSON Expressions
================

Summary
-------

JSON Expressions are JSON objects inspired by [Polish prefix notation](http://en.wikipedia.org/wiki/Polish_notation): All expression objects are `{name: value}` pairs, where the operator is the name, and value holds the parameters.

	{operator_name: parameters}

Expressions are composed of 

* primitive values: `true`, `false`, `null`, numbers and strings
* strings representing dot-separated property names (with format of `\w+(?:\.\w+)*`)
* objects representing compound expressions


Example: `eq` Operator
----------------------

### `eq` Operator ###

The equality operator is most used, and has a range of parameters. We will use the `eq` operator to demonstrate the format for the rest of this document.  

`eq` returns a Boolean, and has two major forms:

		{"eq": {variable: constant}}
		{"eq": [expr1, expr2]}

***Be careful of the distinction between a parameter as an object `{}` or an array `[]`***

Since "eq" is commutative, the array form is not limited to just two expressions 

		{"eq": [expr1, expr2, ... exprN]}

`eq` also has a compound form that allows you to compare multiple `{variable: constant}` pairs succinctly:

		{"eq": {
			variable_1: constant_1,
			variable_2: constant_2,
			...
			variable_N: constant_N
		}}

which is logically equivalent to:

		{"and":[
			{"eq": {variable_1: constant_1}},
			{"eq": {variable_1: constant_1}},
			...
			{"eq": {variable_N: constant_N}}	
		]}

`null` constants are ignored by the `eq` operator. If you want to compare a `variable` to `null`, you must use the `missing` operator.


--------------------------------------------------------------------------------

Reference
=========

### `coalesce` Operator ###

Return the first not `null` value in the list of evaluated expressions. 

		{"coalesce": {variable, constant}}
		{"coalesce": [expr1, expr2, ... exprN]}

For the *simple* form; `null` is a legitimate `constant`. Generally, if all expressions evaluate to `null`, or the expression list is empty, then the result is `null` 



Boolean Operators
-----------------

### `and` Operator ##

		{"and": [expr_1, expr_2, expr_2, ... , expr_N]}

expressions that evaluate to `null` are ignored

		{"and": [null, expr, ...]} ⇒ {"and": [expr, ...]}

an empty list evaluates to true  

		{"and": []} ⇒ true

### `or` Operator ###

		{"or": [expr_1, expr_2, expr_2, ... , expr_N]}

expressions that evaluate to `null` are ignored

		{"or": [null, expr, ...]} ⇒ {"or": [expr, ...]}

an empty list evaluates to `false`  

		{"or": []} ⇒ false


### `not` Operator ###

Negation of another expression. 

		{"not": expr}

Note: negation of `null` is `null`
		
		{"not": null} ⇒ null


Comparison Operators
--------------------

### `eq` Operator ###

As detailed above, `eq` has two main forms. The *simple* form:  

		{"eq": {
			variable_1: constant_1,
			variable_2: constant_2,
			...
			variable_N: constant_N
		}}

and the `formal` form

		{"eq": [expr1, expr2, ... expr3]}


### `ne` Operator ###

Returns `true` if two expressions are not null *and* not equal.

		{"ne": {variable: value}}
		{"ne": [expr1, expr2]}

`eq` and `ne` are not complements: For example, they both return `false` when only one expression is `null`:

		{"ne": [null, 1]} ⇒ false
		{"eq": [null, 1]} ⇒ false


### `gt`, `gte`, `lte`, `lt` Operators ###

Compare two expressions, and return a Boolean

		{"gt": {variable: value}}   ⇒  variable > value
		{"lte": [expr1, expr2]}     ⇒  expr1 ≤ expr2

If either operand is `null` then the result is `null`; which is effectively `false` in a comparison.


Math Operators
--------------

All the math operators, except `count`, return `null` if *any* the operands are `null`. You can change the return value by including a `default`.   This is different from the aggregates that go by the same name; which simply ignore the `null` values.

		# if **any** expressions evaluate to `null` then return zero
		{"sum": [expr1, expr2, ... exprN], "default": 0}


### `count` Operator (commutative)###

For counting the number of not-null values.

		{"count": [expr1, expr2, ... exprN]}
	
`nulls` are not counted, and the empty list returns zero:

		{"count": []} ⇒ 0


### `sum` Operator (commutative)###

For adding the result of many expressions. Also known as `add`.

		{"sum": [expr1, expr2, ... exprN]}
		
By default, if **any** expressions evaluate to `null`, then `null` is returned. You can change this with `"nulls":true`; so `nulls` are ignored during summation, returning `null` only if **all** expressions evaluate to `null`:  

		# `null` expressions are ignored
		{"sum": [expr1, expr2, ... exprN], "nulls": true}

The empty list always evaluates to the default value, or `null`.

		{"sum": []} ⇒ null


### `sub` Operator ###

Subtract two expressions. Also known as `subtract` and `minus`

		{"sub": {variable: value}}
		{"sub": [expr_a, expr_b]}


### `mult` Operator (commutative)###

Multiply multiple values. Also known as `multiply` and `mul`

		{"mult": [expr1, expr2, ... exprN]}

By default, if **any** expressions evaluate to `null`, then `null` is returned. You can change this with `"nulls":true`; so `nulls` are ignored during summation, returning `null` only if **all** expressions evaluate to `null`:  

		# `null` expressions are ignored
		{"mult": [expr1, expr2, ... exprN], "nulls": true}

The empty list always evaluates to the default value, or `null`.

		{"mult": []} ⇒ null

### `div` Operator ###

For division.

		{"div": {variable: denominator}} 
		{"div": [numerator, denominator]} 

If either operand is `null`, or if the denominator is zero, the operator will return `null`. The `default` clause can be used to replace that.

		{"div": [numerator, denominator], "default": 0}  // {"div": ["x", 0]} == 0 for all x


### `exp` Operator ###

Raise the base to given exponent. Also known as `pow` and `power`

		{"exp": [base, exponent]} ⇒ base ** exponent

to resolve ambiguity, we define 0<sup>0</sup> as `null`:

		{"exp": [0, 0]} ⇒ null

of course, the `default` clause allows you to provide the definition that suites you best:

		{"exp": [base, exponent], "default": 0}     // {"exp": [0, x]} == 0 for all x
		{"exp": [base, exponent], "default": base} // {"exp": [x, 1]} == x for all x


### `mod` Operator ###

Calculate the modulo, always results in a non-negative number
	
		{"mod": [dividend, divisor]}  ⇒ dividend % divisor

### `floor` Operator ###

Highest integer less than, or equal to, `dividend`
	
		{"floor": [dividend, divisor]} ⇒ dividend - (dividend % divisor)


Search Operators
----------------

### `term` Operator ###

Identical to the `eq` operator, but exists for Elasticsearch compatibility.

		{"term": {variable, value}} 


### `in` Operator ###

The `in` operator tests if property can be found in a list of constants.  

		{"in": {"a": [1, 2, 3, 4]}}

and is logically the same as 

		{"or": [
			{"eq": {"a": 1}}
			{"eq": {"a": 2}}
			{"eq": {"a": 3}}
			{"eq": {"a": 4}}
		]}


### `missing` Operator ###

Test if a property is `null`, or missing from the record

		{"missing": variable}


### `exists` Operator ###

Test is a property is not `null`

		{"exists": variable}

### `prefix` Operator ###

Test if a property has the given prefix. Only the *simple* form exists.

		{"prefix": {variable: prefix}}

### `regexp` Operator ###

Return `true` if a property matches a given regular expression. The whole term must match the expression; use `.*` for both a prefix and suffix to ensure you match the rest of the term. Also be sure you escape special characters: This is a JSON string of a regular expression, not a regular expression itself. Only the *simple* form exists.

		{"regexp": {variable: regular_expression}}

### `match_all` Operator ###

Dummy operator that always returns `true`. It is an artifact of ElasticSearch filter expressions. Use `true` instead.

		{"match_all": {}}


String Operators
----------------

### `length` Operator ###

Return the length of a string.

		{"length": expression}

### `left` Operator ###

Return the left-part of given string. `null` parameters result in `null`. `left` accepts a numeric `length`: negative length results in the empty string.  `left` also accepts a string `sentinel`: If found, will return everything to the left of the `sentinel`, otherwise will return the whole string.

		{"left": {variable: length}}
		{"left": {variable: sentinel}}
		{"left": [expression, length]}

### `right` Operator ###

Return the right-part of given string. `null` parameters result in `null`; negative length results in the empty string.  

		{"right": {variable: length}}
		{"right": [expression, length]}

### `between` Operator ###

Return the leftmost substring between a given `prefix` and `suffix`

		{"between": {variable: [literal_prefix, literal_suffix]}}

The prefix and suffix are interpreted as literals when using the object form. The array form will accept JSON expressions:

		{"between": [value, prefix, suffix]}

For example, the following two expressions are identical:

		{"between": {"a": ["http://", "/"]}}
		{"between": ["a", {"literal": "http://"}, {"literal": "/"}]}

If the `prefix` is `null`, everything from the beginning of the string to the suffix will be returned. If `suffix` is `null`, everything from the `prefix` to the end of the string will be returned.

`between` has two optional parameters: 

* `default` can be set in the event there is no match.
* `start` is an index into the string where the search begins

		{"between": {"a": ["http://", "/"]}, "start":200, "default":"<none>"}



### `find` Operator ###

Test if property contains given substring, return the index of the first character if found.

		{"find": {variable: substring}}

JSON Expressions treat zero (`0`) as a truthy value; this implies `find` can be used in conditional expressions, even if the `substring` happens to be the prefix.

		{
			"when": {"find": [
				{"literal": "hello world"}, 
				{"literal": "hello"}
			]}
			"then": {"literal": "always reached"}
			"else": {"literal": "not reached"}
		}


`find` will return a default value if the substring is not found, that default value is usually `null`, but can be set with the `default` clause.   

		{"find": {variable, substring}, "default": -1}


### `concat` Operator ###

Concatenate an array of strings. with the given `separator`

	{"concat": [string1, string2, ... stringN], "separator":separator}

The `separator` defaults to the empty string (`""`).

	{"concat": [
		{"literal":"hello"}, 
		{"literal":"world"}
	]}
	⇒ "helloworld"

You must specify your separator, if you want it 

	{
		"concat": [
			{"literal":"hello"}, 
			{"literal":"world"}
		], 
		"separator":" "
	} 
	⇒ "hello world"

If any of the strings empty, or are `null`, they are ignored. 

	{
		"concat": [
			None, 
			{"literal":"hello"}, 
			{"literal":""}, 
			{"literal":"world"}
		], 
		"separator":"-"
	} 
	⇒ "hello-world"




### `not_left` Operator ###

Removes the `length` left-most characters from the given string, returning the rest. `null` parameters result in `null`; negative `length` returns the whole string. Notice that concatenating `left` and `not_left` will return the original expression for all integer `length`.    

		{"not_left": {variable: length}}
		{"not_left": [expression, length]}

### `not_right` Operator ###

Removes the `length` right-most characters from the given string, returning the rest. `null` parameters result in `null`; negative `length` returns the whole string. Notice that concatenating `right` and `not_right` will return the original expression for all integer `length`.    

		{"not_right": {variable: length}}
		{"not_right": [expression, length]}

Conditional Operators
---------------------

Conditional operators expect a Boolean value to decide on. If the value provided is not Boolean, it is considered `true`; if the value is missing or `null`, it is considered `false`. This is different than many other languages: ***Numeric zero (`0`) is truthy***  

### `when` Operator ###

If the `when` clause evaluates to `true` then return the value found in the `then` clause, otherwise return the value of the `else` clause. 
	
		{
			"when": test_expression,
			"then": pass_expression,
			"else": fail_expression
		}

Both the `then` and `else` clause are optional, and default to returning `null`.

### `case` Operator ###

Evaluates a list of `when` sub-clauses in order, if one evaluates to `true` the `then` clause is evaluated for a return value, and the remaining sub-clauses are ignored.

		{"case": [
			{"when":condition1, "then":expression1},
			{"when":condition2, "then":expression2},
			...
			{"when":conditionN, "then":expressionN},
			default_expression
		]}

The last item in the list can be a plain expression, called the `default_expression`. It is evaluated-and-returned only if all previous conditions evaluate to `false`. If the `default_expression` is missing, and all conditions evaluate to `false`, `null` is returned.
***If any `when` sub-clauses contain an `else` clause, the `else` clause is ignored.***


Type Conversion
---------------

### `number` Operator ###

Convert a string to a numeric value.

		{"number": expression}

### `string` Operator ###

Convert a number to a string value

		{"string": expression}

### `date` Operator ###

Convert a literal value to an absolute, or relative, unix datestamp. Only literal values, and not JSON Expressions, are acceptable operands. 

		{"date": literal}

The literal is parsed according to a [date and time mini language](jx_time.md).


### `literal` Operator ###

Except for the right-hand-side of simple form operations, JSON expressions will interpret JSON as an expression. Sometimes you just want the literal JSON value. `literal` simply returns the property value, unchanged.

		{"eq": {"name": "kyle"}}

Can be stated using the formal (array) form: 

		{"eq": ["test", {"literal": "kyle"}]}

The literal can be primitive, or whole objects

		{"literal": "kyle"}
		{"literal": {"name": "Kyle Lahnakoski", "age": 41}}

### `tuple` Operator ###

You can build a tuple from a JSON array of expressions. This is useful when building compound terms, or building lists with dynamic content. 

		{"tuple": [{"date":"now"}, {"date":"today"}]}

gives

		[1459619303.633, 1459555200.0]


### `leaves` Operator ###

Flattens a (deep) JSON structure to leaf form - where each property name is a dot-delimited path to the values found. Shallow JSON objects are not changed. The `leaves` operator can be used in queries to act like the star (`*`) special form. 

		{
			"from": "unittest",
			"select": {"leaves":"."}
		}


The `leaves` operator action on a literal

		{"leaves": {"literal": {"a": {"index.html": "Hello"}, "b": "World"}}}

results in  

		{
			"a.index\\.html": "Hello", 
			"b": "World"
		}

Please notice the dots (`.`) from the original properties are escaped with backslash.

### `items` Operator

Return an array of `{"name":name, "value":value}` pairs for a given object. 

		{"items": {"literal": {"name": "Kyle Lahnakoski", "age": 41}}}

returns

		[
			{"name": "name", "value": "Kyle"},
			{"name": "age", "value": 41}
		]

`items` on an empty object, or `null` will return `null` (effectively an empty list).  `items` on a primitive `value` will return a list with one name/value pair, like so:

		{"items": value}  ⇒  [{"name":".", "value":value}]

The `items` operator, and its complement, the `object` operator, can be used to manipulate property names via JSON Query expressions. This example accepts a table of `people`, and returns the same, but with all property names in uppercase.

		{
			"from": "people",
			"select":[
				{"name":".", "value":{"object":{
					"from":{"items":"."},
					"select":[
						{"name":"name", "value":{"upper":"name"}},
						"value"
					]
				}}}
			]
		}

The most important column in these situations is the `name` column; it must be provided, in order for the `object` operator to work, and to perform any transform needed on that name.  At least one `value` column should be provided, plucking multiple inner values out of the `value` can be done with many select columns, just be sure all have a name starting with `"value."`.


### `object` Operator

Accept an array of `{"name":name, "value":value}` pairs, and return an object with property names assigned to the values.

		{"object": {"literal": [
			{"name": "name", "value": "Kyle"},
			{"name": "age", "value": 41}
		]}}

returns

		{"name": "Kyle Lahnakoski", "age": 41}



Set Operators (and Variables)
-----------------------------

### `last` Operator ###

Return the last element of a list, or tuple.

		{"last": expression}

If the `expression` value is not a list, then the expression value is returned. If list is empty, or if the expression is `null`, then `null` is returned.


### `rows` Operator ###

Window functions are given additional context variables to facilitate calculation over a set of (sorted) values covered by the window. These are 

* `row` - for the current row; which is implied, but can sometimes be useful
* `rownum` - the index into the window
* `rows` - an array representing the window

JSON Expressions reference inner property names using dot-separated paths. In the case of referencing specific rows in a window function, this is possible, but verbose:

		{"get": ["rows", {"add":{"rownum": offset}}, variable]}

Here we are accessing the `rows` array at `rownum + offset`, where `offset` can be positive or negative. The `rows` operator exists to simplify this access pattern:

		{"rows": {variable: offset}}

Please notice `offset` is relative to `rownum`, not an absolute index into the `rows` array.  JSON expressions allow array indexing outside the limits an array; returning `null` when doing so.


Reflective Operators
--------------------

### `with` Operator ###

Effectively assigns expressions to variables so they can be used in subsequent expressions

		{"with": [
			{name1: expression1},
			{name2: expression2},
			...
			{nameN: expressionN},
			expression
		]} 

Each expression may use any of the names defined before it.

### `get` Operator ###

JSON Expressions reference inner property names using dot-separated paths. Sometimes you will want to compose that path from dynamic values:

		{"get": [accessor1, accessor2, ... accessorN]}

The benefits are easier to see in an example where `t == "test"`, `d == "duration"`. The following are equivalent:

		"run.test.duration"
		{"get": {"run.test", "d"}} 
		{"get": ["run", "t", "d"]}


This can also be used to refer to numeric offsets found in tuples, or lists.  Let `temp == {"tuple": [3, 5]}`:

		{"get": {"temp": 0}} ⇒ 3


### `script` Operator ###

**For testing only** - Used to deliver a string, representing code, to the backend container. ActiveData uses ElasticSearch, where scripting is a security vulnerability, so it is turned off. 

		{"script": "doc['build.date'].value"}

This is useful during development and debugging of ActiveData, and exploring ElasticSearch's responses in more detail.


### `ref` Operator ###

**Not implemented. Security risk if not done properly (eg file://)**

Uses the URL provided to retrieve a JSON value.  

		{"ref": "http://example.com/my_data.json"}

### `eval` Operator ###

**Not implemented. Requires a security review.**

Evaluate the given JSON as an expression:

		{"eval": {"gt": {"build.date": "{{today}}"}}}

`eval` can be useful within the context of `ref`.
