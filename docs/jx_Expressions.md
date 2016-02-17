JSON Expressions
==============

Summary
-------

JSON Expressions have a limited expression language to increase the number of useful queries that can be written.  Expressions are JSON objects inspired by [Polish prefix notation](http://en.wikipedia.org/wiki/Polish_notation):  All expression objects are `{name: value}` pairs, where the operator is the name, and value holds the parameters.

	{operator_name: parameters}

As a side note, JSON query expressions are also expressions: `from` is the operator, and other properties act as modifiers. 


Expressions are composed of 

* primitive values: `true`, `false`, `null`, numbers and strings
* strings representing dot-separated property names (with format of `\w+(?:\.\w+)*`)
* objects representing compound expressions


Example: `eq` Operator
----------------------

###`eq` Operator###

The equality operator is most used, and has the most complex range of parameters.  We will use the `eq` operator to demonstrate the format for the rest of this document.  

`eq` returns a Boolean, and has two major forms:

		{"eq": {variable: constant}}
		{"eq": [expr1, expr2]}

***Be careful of the distinction between a parameter as an object `{}` or an array `[]`***

Since "eq" is commutative, the *formal* form is not limited to just two expressions 

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

`null` constants are ignored by the `eq` operator.  If you want to compare a `variable` to `null`, you must use the `missing` operator.


--------------------------------------------------------------------------------

Reference
=========


Boolean Operators
-----------------

###`and` Operator##

		{"and": [expr_1, expr_2, expr_2, ... , expr_N]}

expressions that evaluate to `null` are ignored

		{"and": [null, expr, ...]} ⇒ {"and": [expr, ...]}

an empty list evaluates to true  

		{"and": []} ⇒ true

###`or` Operator###

		{"or": [expr_1, expr_2, expr_2, ... , expr_N]}

expressions that evaluate to `null` are ignored

		{"or": [null, expr, ...]} ⇒ {"or": [expr, ...]}

an empty list evaluates to `false`  

		{"or": []} ⇒ false


###`not` Operator###

Negation of another expression. 

		{"not": expr}

Note: negation of `null` is `null`
		
		{"not": null} ⇒ null


Comparison Operators 
--------------------

###`eq` Operator###

As detailed above, `eq` has two main forms.  The *simple* form:  

		{"eq": {
			variable_1: constant_1,
			variable_2: constant_2,
			...
			variable_N: constant_N
		}}

and the `formal` form

		{"eq": [expr1, expr2, ... expr3]}


###`ne` Operator###

Returns `true` if two expressions are not null *and* not equal.

		{"ne": {variable: value}}
		{"ne": [expr1, expr2]}

`eq` and `ne` are not complements: For example, they both return `false` when only one expression is `null`:

		{"ne": [null, 1]} ⇒ false
		{"eq": [null, 1]} ⇒ false


###`gt`, `gte`, `lte`, `lt` Operators###

Compare two expressions, and return a Boolean

		{"gt": {variable: value}}   ⇒  variable > value
		{"lte": [expr1, expr2]}     ⇒  expr1 ≤ expr2

If either operand is `null` then the result is `null`; which is effectively `false` in a comparison.


Math Operators
--------------

All the math operators, except `count`, return `null` if *any* the operands are `null`. You can change the return value by including a `default`. 

		# if **any** expressions evaluate to `null` then return zero
		{"sum": [expr1, expr2, ... exprN], "default": 0}


###`count` Operator (commutative)###

For counting the number of not-null values.

		{"count": [expr1, expr2, ... exprN]}
	
`nulls` are not counted, and the empty list returns zero:

		{"count": []} ⇒ 0


###`sum` Operator (commutative)###

For adding the result of many expressions.  Also known as `add`.

		{"sum": [expr1, expr2, ... exprN]}
		
By default, if **any** expressions evaluate to `null`, then `null` is returned.  You can change this with `"nulls":true`; so `nulls` are ignored during summation, returning `null` only if **all** expressions evaluate to `null`:  

		# `null` expressions are ignored
		{"sum": [expr1, expr2, ... exprN], "nulls": true}

The empty list always evaluates to the default value, or `null`.

		{"sum": []} ⇒ null


###`sub` Operator###

Subtract two expressions.  Also known as `subtract` and `minus`

		{"sub": {variable: value}}
		{"sub": [expr_a, expr_b]}


###`mult` Operator (commutative)###

Multiply multiple values.  Also known as `multiply` and `mul`

		{"mult": [expr1, expr2, ... exprN]}

By default, if **any** expressions evaluate to `null`, then `null` is returned.  You can change this with `"nulls":true`; so `nulls` are ignored during summation, returning `null` only if **all** expressions evaluate to `null`:  

		# `null` expressions are ignored
		{"mult": [expr1, expr2, ... exprN], "nulls": true}

The empty list always evaluates to the default value, or `null`.

		{"mult": []} ⇒ null

###`div` Operator###

For division.

		{"div": {variable: denominator}} 
		{"div": [numerator, denominator]} 

If either operand is `null`, or if the denominator is zero, the operator will return `null`.  The `default` clause can be used to replace that.

		{"div": [numerator, denominator], "default": 0}  // {"div": ["x", 0]} == 0 for all x


###`exp` Operator###

Raise the base to given exponent.  Also known as `pow` and `power`

		{"exp": [base, exponent]} ⇒ base ** exponent

to resolve ambiguity, we define 0<sup>0</sup> as `null`:

		{"exp": [0, 0]} ⇒ null

of course, the `default` clause allows you to provide the definition that suites you best:

		{"exp": [base, exponent], "default": 0}     // {"exp": [0, x]} == 0 for all x
		{"exp": [base, exponent], "default": base}  // {"exp": [x, 1]} == x for all x


###`mod` Operator###

Calculate the modulo, always results in a non-negative integer
	
		{"mod": [dividend, divisor]}  ⇒ dividend % divisor

###`floor` Operator###

Highest integer less than, or equal to, `dividend`
	
		{"floor": [dividend, divisor]} ⇒ dividend - (dividend % divisor)


Search Operators
----------------

###`term` Operator###

Identical to the `eq` operator, but exists for Elasticsearch compatibility.

		{"term": {variable, value}} 


###`in` Operator###

The `in` operator tests if property can be found in a list of constants.  

		{"in": {"a": [1, 2, 3, 4]}}

and is logically the same as 

		{"or": [
			{"eq": {"a": 1}}
			{"eq": {"a": 2}}
			{"eq": {"a": 3}}
			{"eq": {"a": 4}}
		]}


###`missing` Operator###

Test if a property is `null`, or missing from the record

		{"missing": variable}


###`exists` Operator###

Test is a property is not `null`

		{"exists": variable}

###`match_all` Operator###

Dummy operator that always returns `true`.  It is an artifact of ElasticSearch filter expressions.  Use `true` instead.

		{"match_all": {}}


String Operators
----------------

###`length` Operator###

Return the length of a string.

		{"length": expression}

###`left` Operator###

Return the left-part of given string.  `null` parameters result in `null`; negative length results in the empty string.  

		{"left": {variable: length}}
		{"left": [expression, length]}

###`not_left` Operator###

Removes the `length` left-most characters from the given string, returning the rest.  `null` parameters result in `null`; negative `length` returns the whole string.  Notice that concatenating `left` and `not_left` will return the original expression for all integer `length`.    

		{"not_left": {variable: length}}
		{"not_left": [expression, length]}

###`right` Operator###

Return the right-part of given string.  `null` parameters result in `null`; negative length results in the empty string.  

		{"right": {variable: length}}
		{"right": [expression, length]}

###`not_right` Operator###

Removes the `length` right-most characters from the given string, returning the rest.  `null` parameters result in `null`; negative `length` returns the whole string.  Notice that concatenating `right` and `not_right` will return the original expression for all integer `length`.    

		{"not_right": {variable: length}}
		{"not_right": [expression, length]}

###`contains` Operator###

Test if property contains given substring.  

		{"contains": {variable, substring}}


###`prefix` Operator###

Test if a property has the given prefix.  Only the *simple* form exists.

		{"prefix": {variable: prefix}}

###`regexp` Operator###

Return `true` if a property matches a given regular expression.  The whole term must match the expression; use `.*` for both a prefix and suffix to ensure you match the rest of the term.  Also be sure you escape special characters:  This is a JSON string of a regular expression, not a regular expression itself.  Only the *simple* form exists.

		{"regexp": {variable: regular_expression}}



Conditional Operators
---------------------

Conditional operators expect a Boolean value to decide on.  If the value provided is not Boolean, it is considered `true`; if the value is missing or `null`, it is considered `false`.  This is different than many other languages: ***Numeric zero (0) is truthy***  

###`coalesce` Operator###

Return the first not `null` value in the list of evaluated expressions. 

		{"coalesce": {variable, constant}}
		{"coalesce": [expr1, expr2, ... exprN]}

For the *simple* form; `null` is a legitimate `constant`.  Generally, if all expressions evaluate to `null`, or the expression list is empty, then the result is `null` 

###`when` Operator###

If the `when` clause evaluates to `true` then return the value found in the `then` clause, otherwise return the value of the `else` clause. 
	
		{
			"when": test_expression,
			"then": pass_expression,
			"else": fail_expression
		}

Both the `then` and `else` clause are optional

###`case` Operator###

Evaluates a list of `when` sub-clauses in order, if one evaluates to `true` the `then` clause is evaluated for a return value, and the remaining sub-clauses are ignored.

		{"case": [
			{"when":condition1, "then":expression1},
			{"when":condition2, "then":expression2},
			...
			{"when":conditionN, "then":expressionN},
			default_expression
		]}

The last item in the list can be a plain expression, called the `default_expression`.  It is  evaluated-and-returned only if all previous conditions evaluate to `false`.  If the `default_expression` is missing, and all conditions evaluate to `false`, `null` is returned.  
***If any `when` sub-clauses contain an `else` clause, the `else` clause is ignored.***


Type Conversion
---------------

###`number` Operator###

Convert a string to a numeric value.

		{"number": expression}

###`string` Operator###

Convert a number to a string value

		{"string": expression}

###`date` Operator###

Convert a literal value to an absolute, or relative, unix datestamp.   Only literal values, and not JSON Expressions, are acceptable operands. 

		{"date": literal}

The literal is parsed according to a [date and time mini language](jx_Time_Math.md).


###`literal` Operator###

Except for the right-hand-side of simple form operations, JSON Expressions will interpret JSON as an expression.  Sometimes you just want the literal JSON value.  `literal` simply returns the property value, unchanged.

		{"eq": {"test", 42}}

Can be stated in a more complicated form 

		{"eq": ["test", {"literal": 42}]}

The literal can be primitive, or whole objects

		{"literal": 42}
		{"literal": {"name": "Kyle Lahnakoski", "age": 41}}


### `script` Operator ###

**For testing only** - Used to deliver a string, representing code, to the backend container.  ActiveData uses ElasticSearch, where scripting is a security vulnerability, so it is turned off. 

		{"script": "doc['build.date'].value"}

This is useful during development and debugging of ActiveData, and exploring ElasticSearch's responses in more detail.

 
### `ref` Operator ###

**Not implemented.  Security risk if not done properly (eg file://)**

Uses the URL provided to retrieve a JSON value.  

		{"ref": "http://example.com/my_data.json"}

### `eval` Operator ###

**Not implemented.  Requires a security review.**

Evaluate the given JSON as an expression:

		{"eval": {"gt": {"build.date": "{{today}}"}}}

`eval` can be useful within the context of `ref`.