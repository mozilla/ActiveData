Qb Expressions
==============

Summary
-------

Qb has a limited expression language to increase the number of useful queries that can be written.  Expressions are JSON objects inspired by [Polish prefix notation](http://en.wikipedia.org/wiki/Polish_notation):  All expression objects use the operator as the name, and operands (terms) as the value.

As a side note, Qb queries are also expressions: `from` is the operator, and other name/value pairs are modifiers. 


`eq` Operator
-------------

The most used, and most complex operator.  It has two major forms:

###Compare to constant###

	{"eq": {variable: value}}


###Compare variables###

	{"eq": [variable_A, variable_b]}

