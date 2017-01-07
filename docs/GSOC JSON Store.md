
# Query JSON Documents using SQLite 


## Overview

Could you write a database schema to store the following JSON?

	{
		"name": "The Parent Trap",
		"released": "29 July 1998",
		"imdb": "http://www.imdb.com/title/tt0120783/",
		"rating": "PG"
		"director": {
			"name": "Nancy Meyers"
			"dob": "December 8, 1949"
		}
	} 

Could you write a query to extract all movies directed by Nancy Meyers?

Can you modify your database schema to additionally store JSON with nested objects?

	{
		"name": "The Parent Trap",
		"released": 901670400,
		"director": "Nancy Meyers"
		"cast": [
			{"name": "Lindsay Lohan"},
			{"name": "Dennis Quaid"},
			{"name": "Natasha Richardson"}
		]
	}

Could you write a query to return all movies with Lindsay Lohan in the cast?
		
Now. Can you program a machine to do this for you!!? Can you modify the database, on the fly, as you receive more documents like the above?  How do your queries change?

## Problem 

JSON is a nice format to store data, and it has become quite prevalent. Unfortunately, databases do not handle it well; often a human is required to declare a schema that can hold the JSON before it can be queried. If we are not overwhelmed by the diversity of JSON now, I expect we soon will be. I expect there to be more JSON, of more different shapes, as the number of connected devices continues to increase.   

## The solution

The easy part is making the schema, and changing it dynamically as new JSON schema are encountered. The harder problem is ensuring the old queries against the new schema have the same meaning. In general this is impossible, but there are particular schema migrations that can leave the meaning of existing queries unchanged.  

By dealing with JSON documents we are limiting ourselves to [snowflake schemas](https://en.wikipedia.org/wiki/Snowflake_schemahierarchical). This limitation reduces the scope of the problem. Let's restrict ourselves further to a subset of schema transformations that can be handled automatically; we will call them "schema expansions":

1.	Adding a property - This is a common migration
2.	Changing the datatype of a property, or allowing multiple types - It is nice if numbers can be queried like numbers and strings as strings.
3.	Change a single-valued property to a multi-valued property - Any JSON property `{"a": 1}` can be interpreted as multi-valued `{"a": [1]}`. Then assigning multiple values is trivial expansion `{"a": [1, 2, 3]}`.
4.	Change an inner object to nested array of objects - Like the multi-valued case: `{"a":{"b":"c"}}`, each inner object can be interpreted as a nested array `{"a": [{"b":"c"}]}`.  Which similarly trivializes schema expansion.

Each of these schema expansions should not change the meaning of old queries. Have no fear! The depths of history gives us a language that is already immutable under all these transformations!

## Schema-Independent Query Language?

Under a changing schema, can we write queries that do not change meaning as the schema expands? For hierarchical data, data that fits in a [snowflake schemas](https://en.wikipedia.org/wiki/Snowflake_schemahierarchical): Yes! Yes we can!!!

Each JSON document can be seen as a single point in a multidimensional Cartesian space; where the properties represent coordinates in that space. Inner objects simply add dimensions, and nested objects represent constellations of points in an even-higher dimensional space. These multidimensional [data cubes](https://en.wikipedia.org/wiki/OLAP_cube) can be represented by [fact tables](https://en.wikipedia.org/wiki/Fact_table) in a [data warehouse](https://en.wikipedia.org/wiki/Data_warehouse). Fact tables can be queried with [MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions). 

With this in mind, we should be able to use MDX as inspiration to query JSON datastores. Seeing data as occupying a Cartesian space give us hints about the semantics of queries, and how they might be invariant over the particular schema expansions listed above.

## Benefits

Having the machine manage the data schema gives us a new set of tools: 

* **Easy interface to diverse JSON** - a query language optimized for JSON documents if you will
* **No schema management** - Schemas, and migrations of schemas, are managed by the machine.
* **Scales well** - Denormalized databases, with snowflake schemas, can be sharded naturally, which allows us to scale.    
* **Handle diverse datasources** - Relieving humans of schema management means we can ingest more diverse data faster. The familiar [ETL process](https://en.wikipedia.org/wiki/Extract,_load,_transform) can be replaced with [ELT](https://en.wikipedia.org/wiki/Extract,_transform,_load) Links: [A](http://hexanika.com/why-shift-from-etl-to-elt/), [B](https://www.ironsidegroup.com/2015/03/01/etl-vs-elt-whats-the-big-difference/)
* **Mitigate the need for a (key, value) table** - Automatic schema management allows us to annotate records, or ORM objects, without manual migrations: This prevents the creation of a (key, value) table (the "junk drawer" found in many database schemas) where those annotations usually reside.  
* **Automatic ingestion of changing relational databases** - Despite the limited set of schema expansions, we can handle more general relational database migrations: Relational databases can [extracted as a series of De-normailzed fact cubes](https://github.com/klahnakoski/MySQL-to-S3) As a relational database undergos migrations (adding columns, adding relations, splitting tables), the extraction process can continues to capture the changes because each fact table is merely a snowflake subset of the relational whole.
* **leverage existing database optimization** - This project is about using MDX-inspired query semantics and translating to database-specific query lanagauge: We leverage the powerful features of the underlying datastore.  

## Existing solutions

* We might be able to solve the problem of schema management by demanding all JSON comes with a formal JSON schema spec. That is unrealistic; it pushes the workload upstream, and is truly unnecessary given the incredible amount of computer power at our fingertips.
* Elasticsearch 1.x has limited, automatic schema detection which has proven useful for indexing and summarizing data of unknown shapes. We would like to generalize this nice feature and and bring machine managed schemas to other datastores. It also indexes nested arrays.  
* Oracle uses [json_*](http://www.oracle.com/technetwork/database/sql-json-wp-2604702.pdf) functions to define views which can operate on JSON. It has JSON path expressions; mimicking MDX, but more elegant. The overall query syntax is clunky. More links: [A](https://docs.oracle.com/database/121/ADXDB/json.htm#ADXDB6246), [B](https://blogs.oracle.com/jsondb/entry/s)
* Spark has [Schema Merging](http://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging) and nested object arrays can be accessed using [explode](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode). Spark is a little more elegant, despite the the fact it exposes *how* the query executes.

These existing solutions solve the hard problems from the bottom up; managing file formats, organizing indexes, managing resources and query planning. Each built their own stack with their own query conventions guided by the limitations of architecture they built. 

This project is about working from the top down: A consistent way to query data; identical no matter the underlying data store; so we can swap them based on scale of the problem, and speed requirements.   


## The Actual Task 

There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into 3 different output formats. Success means passing all tests using a MySQL database. 


## Non-Objectives

* **Translation Speed** - Once we are able to hoist the miserable state of database schema management out of the realm of human intervention, we can worry about optimizing the query translation pipeline.   
* **Record Insert Speed** - Query response time over large data is most important,  insert speed is not. It is a known problem that altering table schemas on big tables can take a long time. Solving this is not a priority (see The Future)
* **Big data support** - We are focusing on data scale that fits on a single machine; around 10million 


## Variations

Even though MySQL is preferred, the choice of datastore is not important to this project. Additional usefulness comes from being able to use the same query language on a diverse set of datastores; each has strengths and weaknesses, swapping one for another gives us flexibility. 

* **ElasticSearch 5.x+** - A connector for ElasticSearch 1.7.* already exists, but later versions of ElasticSearch have stricter schema requirements. Update the query library to use ElasticSearch 5.x. Learning the intricacies of the ES query langauge will be the hard part here.
* **Sqlite** - Use Sqlite instead of 
* **Columnar DB** - Still use a database, but use columnar strategies: Give each JSON property its own table with foreign keys pointing to the document id. Adding new columns will be fast because they are whole new tables.  Queries may be faster because rows are smaller, or queries may be slower because of join costs. Any work on this variation would be experimental. 
* **Numpy** - Use the columnar storage strategy, and use Numpy to store the columns. This could give us a very fast query response, albeit limited to memory.


## The Future

* **Machine Managed Sharding** - Add an extra layer that can broadcast queries to multiple databases (each with different schemas) and merge the responses. When this is possible, adding a column to a big table can be an O(1) operation: Make a new schema (with table having our new columns), store all subsequent records to this new schema, use sharding logic to make the two schemas act like one. 
* **Machine Managed indexes** - Databases indexes act much like a columnar datastore. If we account for the common queries received, we may be able to choose he correct indexes to improve query response.    
* **Hetrogenous Shards** - Being able to send the same query to multiple backends allows us to pick a backend that best meets requirements; very big, or very fast
* **Subqueries** - Allowing heterogeneous datastores also allows us to split queries across platforms so each backend can handle the part it is best at; By dispatching data aggregation and filtering to a cluster we get fast response over big data, while a local database can use previous query results to perform sophisticated cross referencing and window functions.