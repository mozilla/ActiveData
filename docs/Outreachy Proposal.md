
# Upgrade ActiveData to use ElasticSearch v5.0+

## About ActiveData

ActiveData is a publicly accessible data warehouse holding many billions of records, for some dozen+ datasets concerning Mozilla's testing infrastructure: This includes test results, job results, code coverage, and extracts from other systems. The ActiveData code itself is really only a stateless query translation layer; leaving the hard work of high speed filtering and aggregation to Elasticsearch.

* ActiveData accepts [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md)
* [ActiveData Tutorial](https://github.com/klahnakoski/ActiveData/blob/dev/docs/GettingStarted.md) will help you understand how it is used.


## Background

Elasticsearch is designed for text search, but can also serve as an extremely fast data warehouse. The speed comes from using [inverted indices](https://www.elastic.co/guide/en/elasticsearch/guide/current/inverted-index.html) to provide high performance data filtering and aggregation. Elasticsearch can index almost any JSON document, perform schema merging, and index all its properties, with almost no human intervention. By letting the machine manage the schema, we can query the JSON without transforming it [2]

Elasticsearch does have a drawback: Its query language is designed for text search and is painful to use in a data warehouse context. Hence the need for ActiveData.

## Problem

Elasticsearch 1.7.x was the last version that did a reasonable job of schema merging. Newer versions (2.0+) have disallowed schema merging, preventing ingestion of JSON documents that have a schema that conflicts with previous documents. We would like to use newer, faster, and more stable versions of Elasticsearch, but they can not handle varied data.

## Solution

Build a translator will convert a variety of JSON formats into a single, strictly-typed, schema. The translator will use schema merging and property-renaming to perform a translation on documents before they go Elasticsearch.  

## Benefits

ElasticSearch's schema merging is great, but has always been incomplete:
 
1. It could not merge inner objects `{"a":{"b":1}}` with nested objects `{"a":[{"b":0}]}`, and 
2. Merging numbers `{"a": 1}` with strings `{"b": "1"}` did not cause failure, but did leave the schema ambiguous, and made the queries clumsy.

This upgrade will make ActiveData more flexible, improve service stability, and provide a step towards promoting this project to production.

## Suggested Skills

Some particular experience will make this task easier (most important first):

* Python 
* SQL and query languages
* Database normalization and functional dependencies 
* Denormalization and data warehousing


## References

1. Similar project for smaller data: [Mapping JSON to strict DB schema](https://github.com/klahnakoski/JSONQueryExpressionTests/blob/master/docs/GSOC%20Proposal.md)
2. [ELT](https://en.wikipedia.org/wiki/Extract,_transform,_load) Links: [A](http://hexanika.com/why-shift-from-etl-to-elt/), [B](https://www.ironsidegroup.com/2015/03/01/etl-vs-elt-whats-the-big-difference/)
3. [data cubes](https://en.wikipedia.org/wiki/OLAP_cube) 
4. [fact tables](https://en.wikipedia.org/wiki/Fact_table) in a [data warehouse](https://en.wikipedia.org/wiki/Data_warehouse). 
5. [MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions). 

## Questions

**Does this work on Windows?**

> Yes! If you are a native Windows user you can install Python, install required libraries, run the server, and run the test suite. You do not need a VM, docker, or Linux emulator.

**Does this work on Linux?**

> Absolutely! Python was made to work on Linux! 

**What is the first step?**

>Clone the [master branch of ActiveData](https://github.com/klahnakoski/ActiveData/tree/master), follow the directions and ensure the tests pass. The tests take about 9 minutes on my machine. Feel free to ask questions if something goes wrong.
>
>Once the tests pass. Upgrade your local Elasticsearch from 1.7.x to version 5.x.  Run the tests again to see all the failures. 
>
> Your mission, if you choose to accept it, is to get those tests to pass with the new version of Elasticsearch.

**What IDE should I use?**

> An IDE you are comfortable with is best, but be sure it has an interactive debugger! Here is a video on [how easy debugging should be](https://www.youtube.com/watch?v=QJtWxm12Eo0). Your debugger should be as good, or better. More [PyCharm docs](https://www.jetbrains.com/pycharm/documentation/).

**How much database theory must I know?**

> The skills listed will make the project easier for you, but none are required. This is primarily a Python project translating one query language to another.

**What do you think the main problem will be when upgrading**

>The biggest problem is to solve is how to get JSON into Elasticsearch despite the conflicting schemas. For example, consider these two JSON documents:
>
><pre>    {"a": 1}<br>    {"a": [{"b":1}, {"b":2}]}</pre>
>
> How can we make them fit into one schema? Can we somehow re-write the JSON to a new format so they will fit into ES.  How do we change the Elasticsearch queries to work with the new format? [I have added a test that ensure these schemas can be merged](https://github.com/klahnakoski/ActiveData/blob/dev/tests/test_jx/test_schema_merging.py#L22)

**What branch do I work off of?**

>  Work with the `master` branch; it is working and stable.  The `dev` branch (which you are reading right now) is unstable and has a long way to go before merging back into master. 