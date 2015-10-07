Qb Expressions (INCOMPLETE)
==============

Summary
-------

Qb has a limited expression language to increase the number of useful queries that can be written.  Expressions are JSON objects inspired by [Polish prefix notation](http://en.wikipedia.org/wiki/Polish_notation):  All expression objects are `{name: value}` pairs, where the operator is the name, and value holds the parameters.

	{operator_name: parameters}

As a side note, Qb queries are also expressions: `from` is the operator, and other name/value pairs act as operation modifiers. 


Expressions are composed of 

* primitive values `true`, `false`, `null`
* strings representing dot-separated property names (with format of `\w+(?:\.\w+)*`)
* objects representing operators on compound expressions



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

Returns `true` if two expressions are not equal

		{"ne": {variable: value}}
		{"ne": [expr1, expr2]}



###`gt`, `gte`, `lte`, `lt` Operators###

Compare two expressions, and return a Boolean

		{"gt": {variable: value}}   ⇒  variable > value
		{"lte": [expr1, expr2]}     ⇒  expr1 ≤ expr2



Math Operators
--------------

###`count` Operator###

For counting the number of not-null values.

		{"count": [expr1, expr2, ... exprN]}
	
`nulls` are not counted

		{"count": []} ⇒ 0


###`sum` Operator###

For adding the result of many expressions.  Also known as `add`.

		{"sum": [expr1, expr2, ... exprN]}
	
expressions evaluating to `null` are ignored.  The empty list evaluates to `null`.

		{"sum": []} ⇒ null

 
###`sub` Operator###

Subtract two expressions.  Also known as `subtract` and `minus`

		{"sub": {variable: value}}
		{"sub": [expr_a, expr_b]}


###`mult` Operator###

Multiply multiple values.  Also known as `multiply` and `mul`

		{"mult": [expr1, expr2, ... exprN]}

expressions evaluating to `null` are ignored.  The empty list evaluates to `null`.

		{"mult": []} ⇒ null


###`div` Operator###

For division.  There is no *simple* form.

		{"div": [numerator, denominator]} 


###`exp` Operator###

Raise the base to given exponent.  Also known as `pow` and `power`

		{"exp": [base, exponent]} ⇒ base ** exponent

to resolve ambiguity, we define 0<sup>0</sup> as `null`

		{"exp": [0, 0]} ⇒ null

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


###`terms` Operator###

The `terms` operator (*note it's plural*) has been borrowed from Elasticsearch; it compares a property to any number of constants:

		{"terms": {"a": [1, 2, 3, 4]}}

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

Dummy operator that always returns `true`.  It is an artifact of ElasticSearch filter expressions.

		{"match_all": {}}



###`prefix` Operator###

Test if a property has the given prefix.  Only the *simple* form exists.

		{"prefix": {variable: prefix}}

###`regexp` Operator###

Return `true` if a property matches a given regular expression.  The whole term must match the expression; use `.*` for both a prefix and suffix to ensure you match the rest of the term.  Also be sure you escape special characters:  This is a JSON string of a regular expression, not a regular expression itself.  Only the *simple* form exists.

		{"regexp": {variable: regular_expression}}


String Operators
----------------

###`length` Operator###

Return the length of a string.

		{"length": variable}

###`number` Operator###

Convert a string to a numeric value.

		{"number": variable}



Conditional Operators (Not Yet Implemented)
-------------------------------------------

###`coalesce` Operator###

Return the first not `null` value in the list of evaluated expressions 

		{"coalesce": [expr1, expr2, ... exprN]}

If all expressions evaluate to `null`, or the list is empty, then the result is `null` 

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

Value Operators
---------------

Value operators are meant to convert the JSON argument into some useful abstract value. 

###`literal` Operator###

Except for the right-hand-side of simple form operations, Qb will interpret JSON as an expression.  Sometimes you just want the literal JSON value.  `literal` simply returns the property value, unchanged.

		{"eq": {"test", 42}}

Can be stated in a more complicated form 

		{"eq": ["test", {"literal":42}]}

The literal ca be primitive, or whole objects

		{"literal": 42}
		{"literal": {"name":"Kyle Lahnakoski", "age": 41}}


### `ref` Operator ###

**Not implemented.  Security risk if not done properly (eg file://)**

Uses the URL provided to retrieve a JSON value.  

		{"ref": "http://example.com/my_data.json"}

### `eval` Operator ###

**Not implemented.  Requires a security review.**

Evaluate the given JSON as an expression:

		{"eval": {"gt": {"build.date": "{{today}}"}}}

`eval` can be useful within the context of `ref`.   
 


Operator Philosophy
-------------------

Put at bottom of document because it only explains general design choices

###Operator forms###

Many operators have a *simple* form and a *formal* form which use parameter objects or parameter lists respectively.    

**Simple:**

Simple form uses a simple parameter object, and is for comparing a property (a variable) to a constant value.
	
		{"op": {variable: value}}

**Formal:**

Formal form requires a parameter list with two items.  It is useful for building compound expressions.

		{"op": [expr1, expr2]}

**Constant**

The JSON values `true`, `false`, and `null` are also legitimate expressions. 


###Commutative Operators###

Commutative operators can compound many expressions, and therefore only have a *formal* version:

		{"op": [term1, term2, ... termN]}

###Expressions involving `null`###

As a general rule, the commutative operators will ignore expressions that evaluate to `null`, and the binary operators usually return `null` if any parameter is `null`.  Specific behaviour of each operator on `null` is included below.


