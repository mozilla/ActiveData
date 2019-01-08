# Null-aware operators in JSON Expressions

JSON expressions are null aware; which is important for dealing with JSON from an unclean data lake.



## Definitions

We will be deliberately vague when defining `null`. Instead, we will categorize operator behaviour on `null` realizing that the behaviour of an operator depends on the definition of `null`.

The different variations can depend on the the definition of `null`.  Javascript has both `null`, and `undefined`; the behaviour of a single operator can behave differently given these different 

and there can be overlap, or inconsistencies, 



Let `●` represent an operator. For a given operator, we can classify its behaviour in one of three variations:

### Strict

operators that raise an exception, or compile-time type check, if either `a` or `b` are `null`.  For example,

    sum(42, null) => ¡ABORT!

### Conservative

operators that define `a ● b == null` if either `a` or `b` are `null`.  For example, 

    sum(42, null) => null
 
### Decisive

operators that `a ● b != null` if either `a` or `b` are not `null`.  For example,

    sum(42, null) => 42


for example, the SQL `OR` operator is considered conservative, even though there are some decisive combinations:

    True OR NULL => True


## Choosing a variation for JSON Expressions

Strict operators are common in most procedural languages. Conservative operators are used in SQL expressions. In both these cases we can get good practice with the coding style required to operate on unclean JSON.

The decisive operators are rare: Most applications have clear internal data structures; and a decisive operator will only hide coding mistakes. The value of decisive operators is only apparent in domains that have `nulls`, and this does not happen in most applications because the use of `nulls` can always be mitigated with a "better" set of type definitions.

Json Expressions 
*If* you do not dictate the type system, or the types are changing often, or you deal with types from many different systems; in other words, you are processing "unclean data"; the conservative and strict operators will demand you write verbose code that checks for those nulls. Decisive operators work better in this enviroment. For example, SQL aggregation operators are decisive: 

	AVG(42, 24, null) => (42+24)/2 => 33

## Decisive Operators

the C language has `<when> ? <then> : <else>` trinary operator, 



## Summary

The conservative equality `==`, which is used in SQL, is not useful when performing structural comparisons. The decisive `eq` is a more effective choice.