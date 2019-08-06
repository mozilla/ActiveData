
# ActiveData Capability Overview (August 2019)

With the spectre of ActiveData's retirement; maybe replaced with Google's Big Query: We will summarize ActiveData in terms of problems it solves and the  capabilities it provides. This can serve as a checklist for a successor.


## Problems

The problems we discuss are common in the data management space, and can be split into 3 major categories 


### Data

The shape of the data is important for manipulation. Organization of data is important for discovery. 

* Locating data; required data is sparse
* Shape of the data
* Use cases driving data extraction APIs and declaring schema


Use-case driven analysis vs automated ingestion


### Service

The data must be accessible, which means there is a machine that serves the data. This machine 


The problems relating to maintaining a service 

* Maintaining a data service is work (redundancy, backup and recovery, security updates, scaling)
* Transactional systems can not answer big questions - Even if you have a data service they must work with other data services to be useful.  Consider  Bugzilla x Tasks x Alerts  - Copy the data in, or 
* Low query latency is work - The time between submitting the query and getting back an answer depends on the machine power and data locality and data structure
* Data Timeliness is work - any caching, or warehouse 




### Analysis




Large data extraction

* Support complex queries - Analysis is hard; the data must go through verification, links to other data, transformed for presentation   

Update and fix data

Analytic Functions (window functions)



## Solution: Database as a Service!?

A database server solves many of these problems.  Many micro-service candidates get built into a larger application because a database server solves many of the above problems

### More problems

* Query latency is high - 
* Queries take resources -  
* Queries consume resources, locking database from other transactions
* Human-managed schema
* SQL is not suited for simple queries
* SQL Computationally unbounded 


## Solution: ElasticSearch?

* Another service candidate
* Document database is denomalized, is a hierarchical database
* Everything indexed - low query latency
* Bounded resources per query
* No locking
* Handle multitude of schemas from many sources

### Problems

* Separate service, ETL required
* Query language not suited for analysis


## Solution: ActiveData

* simpler data model (shape reduces choices)
* simpler query language
* ETL ingestion from various sources

ETL is for decoupling from transactional systems




# Denormalization, reverse indexes, nested documents


Problems










always-on-service


effort to get data

low query latency (fast because data does not move)





has everything

standard data

central location of data


sharing


### Operations



offset query load from transactional systems




dynamic schema

all data is array data

json -> matrix

regular query language


large data extraction

bring code to data, not data to the code

no loops, no joins, computationally bounded











denormalized is smaller


|test.id| = number of records in test
|test.name| = number of unique test names


|task.id| = unique tasks
|timestamp| = timestamp
|task.id|


|test.id|*(sizeof(test.id)+sizeof(test.name))

|task.name| -> |task.id| => |task.id| -> |test.id| => |test.id| -> |test.name|

|test.task.name| -> |test.id| => |test.id| -> |test.name|   