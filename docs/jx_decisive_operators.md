# Null-aware operators in JSON Expressions

JSON expressions are "null aware"; which is important when dealing with JSON from an unclean data lake.

## Overview

We want to clarify the behaviour of operators in the presence of `null`. We realise this depends on both the definition of `null` and definition of the operator; we will be specific when necessary.

For example, Javascript has both `null`, and `undefined`; their slightly different definitions can be see in operator behaviour. For example, [here are two interesting observations on exponentiation in javascript](https://github.com/mozilla-frontend-infra/firefox-health-dashboard/blob/6d571309b927c0a2e02e36b8a7f594fc14e41a04/test/vendor/math.test.js#L210):  

    Math.exp(null)      ⇒ 1
    Math.exp(undefined) ⇒ NaN  

In the case of `Math.exp` we see two different behaviours. 

## Definitions

Let `●` represent an operator. For a given operator, we can classify its behaviour in one of three variations:

* **Strict** - operators that raise an exception, or fail a compile-time type check, when either `a` or `b` are `null`.  For example,

        sum(42, null) ⇒ ¡ABORT!
* **Conservative** - operators that define `a ● b ∉ Dom(●)`; if either `a` or `b` are `null` then the operator returns some value, like `null`, outside the domain. For example, 

        sum(42, null) ⇒ null
* **Decisive** - operators that define `a ● b ∈ Dom(●)`; if either `a` or `b` are not `null` then the operator returns a value, not `null`. For example,

        sum(42, null) ⇒ 42

In the case of `Math.exp` we say it is "decisive" when operating on `null`, and "conservative" when operating on `undefined`.  

    Math.exp(null)      ⇒ 1
    Math.exp(undefined) ⇒ NaN  

In the case of SQL: The `OR` operator is considered conservative, even though there are some decisive combinations:

    False OR NULL ⇒ NULL
    True OR NULL ⇒ True

## Environment for JSON Expressions

JSON expressions are for munging unclean data. They are meant for environments where

* you do not dictate the type system, or 
* the types are changing often, or 
* you deal with types from many different systems, or
* you are processing "unclean data"

## Strict Operators?

The null checks required by strict operators results in verbose code. For example, Python addition is strict; we must be careful to not add integers and `None` together:  

```python
if a is None:
    return default_value
if b is None:
    return default_value
return a + b
```

This code does not happen often in practice because most code has clear internal data structures; and a decisive operator will only hide coding mistakes. This can not be said for unclean data.

## Conservative Operators? 

Conservative operators have a similar problem. Consider SQL comparision (`=`), which is conservative: Comparing to `NULL` always results in `NULL`, which is a falsey value:

    CASE 
    WHEN NULL = NULL 
    THEN 'never happens` 
    ELSE 'ok'
    END

Proper comparision is complicated:

    CASE 
    WHEN a.name=b.name OR (a.name IS NULL AND b.name IS NULL)
    THEN 'same'
    ELSE 'different'
    END 

## Decisive Operators!

When data is not clean decisive operators show their benefit. For example, SQL aggregation operators are decisive: 

```sql
	AVG(42, 24, null) ⇒ (42+24)/2 ⇒ 33
```

The decisiveness comes from assuming the `null` represents "out of class": `null` occupies a slot that should not even exist.

Consider the decisive expression

    return (a + b) < 0

which is defined as

    if (a == null) {
        if (b == null) return false;
        return b < 0 ;
    }
    if (b == null) return a < 0;
    return (a + b) < 0

The above shows two things: It shows the underlying logic in the decisive operations, and contrasts the amount of code required to perform those operations.


## Summary

JSON Expression are designed to be decisive because they are intended to be succinct while acting on unclean data.  
