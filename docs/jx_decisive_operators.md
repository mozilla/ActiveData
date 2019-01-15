# Null-aware operators in JSON Expressions

JSON expressions are null aware; which is important for dealing with JSON from an unclean data lake.


## Definitions

We will deliberately avoid the meaning of `null`. Instead, we will categorize operator behaviour on `null`, realizing that the behaviour depends on the meaning.

The different operator variations can depend on the meaning `null`. For example, Javascript has both `null`, and `undefined`; Arguably, a single operator can have two different behaviours, depending on the type of null used in its operands.

Although we define three clear categories, operators can overlap these categories and be inconsistent.


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


## Choosing a category for JSON Expressions

Strict operators are common in most procedural languages. Conservative operators are used in SQL expressions. In both these cases we can get good practice with the coding style required to operate on unclean JSON.

The decisive operators are rare: Most applications have clear internal data structures; and a decisive operator will only hide coding mistakes. The value of decisive operators is only apparent in domains that have `nulls`, and this does not happen in most applications because the use of `nulls` can always be mitigated with a "better" set of type definitions.

*If* you do not dictate the type system, or the types are changing often, or you deal with types from many different systems; in other words, you are processing "unclean data"; the conservative and strict operators will demand verbose code that checks for those nulls. Decisive operators work better in this enviroment. For example, SQL aggregation operators are decisive: 

```sql
	AVG(42, 24, null) => (42+24)/2 => 33
```

On the other hand, SQL comparision (`=`) is conservative 

    1 = NULL  => NULL

which makes comparing values complicated:

```sql
    SELECT a.name=b.name or (a.name IS NULL AND b.name IS NULL) as is_same FROM my_table 
```

### Conservative Operators are dangerous

Conservative operators make Boolean logic expressions difficult to write: Familiar Boolean identies  

### Strict operators are verbose

*Strict* operators give no answer when dealing with nulls, which forces null handling to be explicit, and therefore verbose.

    if (null + 1 < 0)


## Summary

