# ActiveData Upgrade Postmortem


## About ActiveData

ActiveData is a query translation service; accepting parsed SQL-like expressions, translating to ES query language, and compacting the results.

ActiveData provides two important features inspired by MDX:

1. Nested object arrays can be queried like any other table; as if joined with the parent documents. This simplifies queries avoiding Elasticsearch's "nested" queries, or SQL's join-with-explode idiom.  
2. Ability to query into a dynamically typed JSON document storage; The strict schema is managed by code, and ActiveData's query expressions need little or no change as the schema expands (migrates) over time.

## Short History

ActiveData has been around since 2015 and has been using Elasticsearch v1.4, or v1.7, since that time. These early versions of Elasticsearch allowed us to pour just about any JSON document into it, and it would "just work". ActiveData was a relatively simple translation service that simplified queries, especially ones involving multiple dimensions.   

Elasticsearch 2.x, and beyond, demand a strict JSON schema; you could no longer dump documents into it, and worry about the schema later. Now, the user had to ensure the schema stayed consistent at insert time, rather than waiting for query time. ActiveData would require an upgrade; it must take over the handling of dynamic schemas.

## Understanding the Effort

Elasticsearch is fast because it indexes everything and stores nothing. If it is asked to store data it stores the primitive values as columns. ES does not store objects; so empty objects and `null` are the same thing.  In any case, documents are not treated as objects with properties; rather as a set columns, where each column name is the path to the primitive value. 

Example document

    {"a": {"b": 3}}

ES logical representation:

| a.b |
| --- |
|  3  |

With this understanding, If we include the datatype in the column name, we can store multiple types in the same "property".  In this case we use `~n~` to indicate numbers:

| a.b.~n~ |
| ------- |
|    3    |
  
Which would make our document look like:

    {"a": {"b": {"~n~": 3}}}

but that form is never realized inside Elasticsearch; storing type information in the column names should have no impact on storage size or query speed.

   




## Reality




* Timeline





## Blockers

Typed JSON
Expression evaluation 
Metadata management
Query re-writing


<img alt="chart of hours" src="./ActiveData%20Summary%20181116%20hours.png"/>


## Gritty Details

### Dec 2016

* fix shard balancer
* fix travis testing


### Feb 2017
* fix neglected oranges
* solve shard balancing problem (move shards off full machines)
* fix es replicate
* sql praser test cases

### Apr 2017
* merging branches
* smaller unittests

### May 2017
* redash connector 
* better shard allocation
* upgrade activedata
* sorting in activedata

### Jul 2017
* refactoring expressions
* typed storage for es5

### Q3 2017
* multitype expressions
* activedata upgrade 
* python 3
* slow 90th percentile
* merge upgrade work into other branches
* new push to es5
* spot manager v6

### Jan 2018
* install activedata
* get frontend6 working on travis
* fix push to es6

### Mar 2018
* mohg, with version 6
* sql fixes, various fixes
* Attempt to move to new cluster
* slow etl ingestion problems 
* deploy to new servers
* fix metadata management

### May 2018
* Slow ingestion problem

### Aug 2018
* Support for Bugzilla ETL ActiveData 

### Oct 2018
* Final push to deploy 
* Rewrite query translator

