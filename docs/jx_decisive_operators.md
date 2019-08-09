# Null-aware operators in JSON Expressions

JSON expressions are null aware; which is important for dealing with JSON from an unclean data lake.


## Definitions

We will deliberately avoid the definition of `null`. Instead, we will categorize operator behaviour on `null`, realizing that the behaviour depends on the definition.

The different operator variations can depend on the definition of `null` and the definition of the operator. For example, Javascript has both `null`, and `undefined`; Arguably, a single operator can have two different behaviours, depending on the type of null used in its operands.

Although we define three clear categories, operators can overlap these categories, and be inconsistent.


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


## Strict Operators for JSON Expressions?

Strict operators are common in most procedural languages. Conservative operators are used in SQL expressions. In both these cases we become well practised with the coding style required to operate on unclean JSON.

The decisive operators are rare: Most applications have clear internal data structures; and a decisive operator will only hide coding mistakes. The value of decisive operators is only apparent in domains that have `nulls`, and this does not happen in most applications because the use of `nulls` can always be mitigated with a "better" set of type definitions.

There are some situations where strict operators demand verbose code:

* If you do not dictate the type system, or 
* the types are changing often, or 
* you deal with types from many different systems, or
* you are processing "unclean data"

The null checks required makes code verbose. Decisive operators work better in these situations. For example, SQL aggregation operators are decisive: 

```sql
	AVG(42, 24, null) => (42+24)/2 => 33
```

On the other hand, SQL comparision (`=`) is conservative; comparing to `NULL` results in `NULL`; a falsey value.

    CASE 
    WHEN NULL = NULL 
    THEN 'never happens` 
    ELSE 'ok'
    END

which makes comparing values complicated:

```sql
    SELECT a.name=b.name or (a.name IS NULL AND b.name IS NULL) as is_same FROM my_table 
```

### Strict operators are verbose

*Strict* operators demand explicit null handling and demand verbose code:

Consider the decisive expression

    return (a + b) < 0

the strict version is:

    if (a == null) {
        if (b == null) return false;
        return b < 0 ;
    }
    if (b == null) return a < 0;
    return (a + b) < 0

## Summary

Decisive operators
