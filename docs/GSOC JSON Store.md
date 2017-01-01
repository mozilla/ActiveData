
# Machine Managed Schemas

## Prerequisites

I hope you have an appreciation for the powerful list-comprehension capabilities of SQL. If you wonder why SQL is not integrated in every language, [like LINQ](https://en.wikipedia.org/wiki/Language_Integrated_Query), then you are the right person for this project!!


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

Could you write SQL to extract all movies directed by Nancy Meyers?

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

Could you write SQL to return all movies with Lindsay Lohan in the cast?
		
Now. Can you program a machine to do this for you!!? Can you modify the database, on the fly, as you receive more documents like the above?    

## Problem 

JSON is a nice format to store data, but from a database perspective it requires human intervention to enable SQL queries over the data. The current JSON database technology requires you to state the schema before you start querying, and this is painful.

If we are not currently swamped by the plethora of JSON schemas. The data coming from the IOT space or from small and medium business needs, certainly will. 

We might be able to solve this problem by dictating all JSON comes with a formal JSON schema spec, but that is unrealistic, pushes the workload upstream, and truly unnecessary given the incredible amount of computer power at our fingertips.


## Existing solutions

ActiveData is a stateless query translator from SQL parse trees to ElasticSeach's query lanaguage. Elasticsearch has limited, but useful, automatic schema detection. Between the two, This has proven useful for indexing and summarizing data that we do not intimately know the shape of. We would like to bring machine managed schemas to other datastores  

https://docs.oracle.com/database/121/ADXDB/json.htm#ADXDB6246
https://blogs.oracle.com/jsondb/entry/s

Compare Oracle's JSON queries to MDX; you can see the property lookup is approaching the MDX dimension name lookup.  

By dealing with JSON documents we are limiting ourselves to a subset of all possible data structures. This limitation will allow us to focus on what's possible in the 

Spark is capable of this, but lacks nested object management: Which is the one feature the gives Machine Managed Schemas enough power to be useful. 


## Benefits

* Easy interface to diverse JSON - another query language for JSON documents if you will
* No schema management - no database migrations
* Denormalized databases, with snowflake schemas, can be sharded naturally, which allows us to scale.    
* OLTP extracts are impervious to database migrations: Relational databases can [extracted as a series of De-normailzed fact cubes](https://github.com/klahnakoski/MySQL-to-S3) As these databases undergo migrations (adding columns, adding relations, splitting tables), the extraction process continues to capture the changes.
* Handle change on the fly 
* Handle diverse datasources 
* Database optimization 
* Eventual (key, value) table definition to handle data


## The solution

The easy part is making the schema, and changing it dynamically as new JSON schema are discovered. The harder part is translating the SQL so changes in the schema does not change the meaning of the SQL. No fear! All the research has been done!

* Schema changes are one-way, and can be called "schema expansion"


## Schema-independent SQL?

Under a changing schema, can we write queries that do not change meaning as the schema expands? For hierarchical data: Yes! Yes we can!!!

[MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions) is used to query multidimensional fact cubes from a data warehouse. Cubes can be described with [snowflake schemas](https://en.wikipedia.org/wiki/Snowflake_schemahierarchical). Snowflake schemas can be used to hold JSON, no matter how pathological. This means we can use MDX as inspiration to query JSON datastores!! 

For this project we will gloss over the specific syntax of the query language, and use a parse tree of MDX/SQL, and translate those parse trees to SQL. 


The syntax of the query language is not the focus of this project. We will translate MDX parse trees to SQL. 
  

We do this now with ActiveData. 

## Clarity 

There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into 3 different output formats. Success means passing all tests using a MySQL database. 


## Non-Objectives

* **Translation Speed** - Once we are able to hoist the miserable state of database schema management out of the realm of human intervention, we can worry about optimizing the query translation pipeline.   
* **Record Insert Speed** - Response time over large data is most important. It is a known problem that altering table schemas on big tables can take a long time. Solving this is not a priority (see The Future)
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