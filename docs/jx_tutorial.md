# JSON Query Expression Tutorial

JSON query expressions are JSON structures that mimic SQL query semantics;
each property corresponds to a SQL clause. There are some differences from
SQL, especially when it comes to using default clauses, but I hope your
knowledge of SQL can jump-start your use of JSON Expressions.


## Simple `from` Clause

All queries must have a `from` clause, which indicates what data is being
queried. ActiveData has a default container, which it uses to translate names
to explicit data sources that can be queried.

```javascript
{"from": "unittest"}

```

In this case, we will get some records from the `unittest` cube. Please note: ActiveData assigns a default `"limit": 10` on all requests to prevent returning overwhelmingly large results by accident.

### Nested object arrays

You can query a nested object array by putting its full path in the `from` clause

```javascript
{"from":"unittest.result.subtests"}
```

`unittest.result.subtests` is an array that holds metadata on the reasons a test failed. This particular query only shows the subtest metadata, but you can access parent table columns too.


## `format` Clause

The ActiveData Query Tool hides the formatting feature of the ActiveData service. Most responses you get back from the service are data cubes (aka pivot tables), and this may not be the best format for your application. You have three main formats to choose from:

* `list` - service will return a list of JSON objects, which is great if you want to see the original source documents, or iterate through the response.
* `table` - service returns a table - There is a `header` containing the names of the columns, and the `data` which is a list of tuples containing row values. This form is generally more compact than the other two forms.
* `cube` - (default) returns the cube form - This format is good for analysis, charting, and is compact for large, dense, datasets.

```javascript
{
    "from": "unittest",
    "format": "list"
}
```

### Examples

**list**
```json
{"data": [
    {"HeadingA":"row 1 Column A", "HeadingB":"row 1 Column B", "HeadingC":"row 1 Column C"}, 
    {"HeadingA":"row 2 Column A", "HeadingB":"row 2 Column B", "HeadingC":"row 2 Column C"}, 
    {"HeadingA":"row 3 Column A", "HeadingB":"row 3 Column B", "HeadingC":"row 3 Column C"}, 
]}
```

**table**
```json
{
    "header":["Heading A", "Heading B", "Heading C"],
    "data":[
        ["Row 1 Column A", "Row 1 Column B", "Row 1 Column C"],
        ["Row 2 Column A", "Row 2 Column B", "Row 2 Column C"]
    ]
}
```

**cube** (default)
```json
{
    "data":{
        "Heading A": ["Row 1 Column A", "Row 2 Column A"],
        "Heading B": ["Row 1 Column B", "Row 2 Column B"],
        "Heading C": ["Row 1 Column C", "Row 2 Column C"]
    }
}
```


### Inspecting Individual Records

The `"format": "list"` clause is great for extracting specific records from ActiveData. Individual records will give you an idea of what is available, and allow you to drill down while exploring possible anomalies.

```javascript
{
    "from": "unittest",
    "where": {"eq": {
        "run.suite": "mochitest-browser-chrome",
        "result.test": "Main app process exited normally"
    }},
    "format": "list"
}
```

In the above case, I was curious about the test named "Main app process exited normally": It is actually an emission from [the harness attempting to report the last run test](https://hg.mozilla.org/mozilla-central/file/291614a686f1/testing/mochitest/runtests.py#l1824). In this case, the harness could not make that determination because the browser closed without error.  


## `limit` Clause

**The ActiveData service limits responses to 10 rows by default**. To increase this limit (or decrease it) Use the `limit` clause to set an upper bound on the response:

```javascript
{
    "from": "unittest",
    "limit": 100
}

```

## `where` Clause

Use the `where` clause to restrict our results to those that match. 


```javascript
{
    "from": "unittest",
    "where": {"eq": {"build.platform": "linux64"}}
}
```

In this case, we limit ourselves to test results on `linux64` platform. You can see [a full list of `unittest` properties](Unittest Schema.md), and you have a [variety of other expressions available](jx_expressions.md). 


## `select` Clause

The `unittest` records are quite large, and in most cases you will not be interested in all the properties. Let's look at how big some test result files can be; list some files over 600 megabytes! It is best to view the raw JSON response with this query; some files are over a gigabyte, so big the Query Tool interprets the number as a unix timestamp!!

```javascript
{
    "from": "unittest",
    "select": "run.stats.bytes",
    "where": {"and": [
        {"eq": {"build.platform": "linux64"}},
        {"gt": {"run.stats.bytes": 600000000}}
    ]}
}
```

## Grouping

Pulling individual records is unexciting, and it will take forever to get an understanding of the billions records in ActiveData. ActiveData's objective is to provide aggregates quickly.

## `groupby` Clause

How many of these monster files are there?

```javascript
{
    "from": "unittest",
    "groupby": ["build.platform"],
    "where": {"and": [
        {"eq": {"etl.id": 0}},
        {"gt": {"run.stats.bytes": 600000000}}        
    ]}
}
```

A few notes on this query: First, if there is no `select` clause when using `groupby`, it is assumed a `count` is requested. Second, the properties in the `groupby` clause will be included in the result set. This differs from SQL, which only shows columns found in the select clause.

Finally, and most important:

> The `unittest` data cube is a list of **test results** not test runs; each run has multiple results, so if we want to accurately count the number of runs we must pick a specific test result that will act as representative: Your best choice is `etl.id==0`.

How big do these files get?

```javascript
{
    "from": "unittest",
    "select": {"value": "run.stats.bytes","aggregate": "max"},
    "groupby": ["build.platform"],
    "where": {"and": [
        {"eq": {"etl.id": 0}},
        {"gt": {"run.stats.bytes": 600000000}}
    ]}
}
```

At time of this writing we see structured logs of over 1.1 Gigabytes! No wonder my Python processes were running out of memory! 


## `edges` Clause

The `edges` clause works just like `groupby` except its domain is unaffected
by the filter. This means that all parts of the domain will be represented in
the result-set, even in the case when no records are in that part.
Furthermore, every domain has a `null` part representing the records that are
outside the domain. 

```javascript
{
    "from": "unittest",
    "select": {"value": "run.stats.bytes","aggregate": "max"},
    "edges": ["build.platform"],
    "where": {"and": [
        {"eq": {"etl.id": 0}},
        {"gt": {"run.stats.bytes": 600000000}}
    ]}
}
```

### Complex `edges`

Edges can be more than strings, they can be clauses that include an additional
description of the domain.


```javascript
{
    "from": "unittest",
    "edges": [{
        "name": "platform", 
        "value": "build.platform", 
        "domain": {"type": "set", "partitions": ["win32"]}
    }],
    "where": {"and": [
        {"eq": {"etl.id": 0}},
        {"gt": {"run.stats.bytes": 600000000}}
    ]}
}
```

In this case, we only care about "win32". The result will include counts for
both "win32" and the "`null`" part which counts everything else.  



## More Reading

* [General Documentation](jx.md) - Detailed documentation

