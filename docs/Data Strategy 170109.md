Future Data Strategy
====================

As of Oct2016, we are approaching 2 years of ActiveData. The objective is to confirm, or deny, or give feedback on our current path.


## Presentation Results

On [Jan2016 I made a presentation](Data%20Strategy%20170109.pdf) to my team. Here are the points they brought up:

**Questions**

* Telemetry has Presto, what is its cross-datasource querying speed? - I wonder if s.t.m.o can answer this for us? What system is responding to those SQL queries?
* Exactly what are the query differences between the architectures? - I did a poor job of explaining this part. Adding slides to bullet-point these, instead of leaving it to myself to verbalize them, will help.    

**Telemetry Benefits**

* Telemetry uses a standard set of tools, which has more support, and more documentation
* Telemetry will not use ActiveData - If there can be only one, we must conclude it will be whatever Telemetry decides.
* Spark has more expressive capabilities
 
**ActiveData Detriments**

* bespoke query language is hard to use
* The performance for specific queries can be slow compared with acceptable latency for an interactive system
* no SQL support
* ActiveData depends on only Kyle, which is a dangerous single point of failure


## Original Objective

Dec 2014 (Portland): As I remember, the original objective was to ingest the structured logs from our test results and analyze our oranges. I decided that the lightning-fast response time of ES, plus its ability to scale to 100million records easily, should provide us with a quick win.

## Problem

The scope of the problem has gently expanded, without me noticing, to the point where it is nothing like the original problem. It should be evaluated.

## Current Scale (Oct 2016)

ActiveData is a data warehouse meant to provide aggregates fast. It is ingesting structured logs at a rate of [61 thousand files, 327 gigabytes, per day](https://activedata.allizom.org/tools/query.html#query_id=0C1HdSut) and [800 requests per day (30K requests per day for codecoverage)](https://activedata.allizom.org/tools/query.html#query_id=YQIp9BbU) (not including static assets, not include direct-to-cluster requests, most from its own peripheral services) 

| Table         |  Record Count | Description                                                        |        
| ------------- | -------------:|:------------------------------------------------------------------ |
| Test Results  | 2,653,634,853 | 1 month test results counted in 2sec (130+ columns, 5K each) 15T   |
| CodeCoverage* | 3,431,492,266 | 3 days of covered lines counted in 19sec (90+ columns, 1K each) 3T |
| TaskCluster   |    16,504,753 | tasks counted in 0.4sec (300+ columns, 4K each) 700G               |
| Buildbot      |    29,885,568 | jobs counted in 0.4sec (180+ columns, 3K each ) 1T                 |

*Code Coverage reached 100 billion in early October

## Cost

The current scale is good, but should be increased for faster speeds and higher reliability

| Description                                       | Cost        |        
| ------------------------------------------------- | ----------- |
| Approx 28 ElasticSearch nodes (750G mem/150T SSD) | $4000/month | 
| Up to 400 vCPUs for ETL pipeline                  | $1000/month |


## Effort

### Time Consuming Categories

* ETL is a problem - Transformation, denormalization, scrubbing, multiple sources
	* JSON solves the denormalization problem
	  * many2one (a variation on properties)
	  * one2many (nested documents)
	* JSON solves the more common data migrations (add/remove columns)
	* audit trail (multiple sources)
	* <s>Common data time line transformations (slowly changing dimensions)</s> (not relevant)
* Liveliness is a problem
	* Monitoring processes 
	* Managing server
	* Machine replacement
	* CPU/Memory costs
	* **Uptime is not a priority** reduces effort and cost
* Big is a problem
	* Think global, act local 
	* build to fail
	* distributed transactions
	* streaming, memory/disk limits

### Time Consuming Combinations

* Big ETL
	* Data/Processing anomalies
	* Schema diversity
	* Constant change
* Big and Lively
	* <s>Manage loosing machines</s> (solved by ES, and opens up using spot instances)
	* <s>Fast response time</s> (solved by ES) 
	* Automate management of machines
	* Automate management of cost
	* Automate load balancing
* Live ETL
	* <s>Optimizations to process fast</s> (not attempted)
	* <s>Responsiveness to deal with failure</s> (not attempted)

¿Using Telemetry?
-----------------

It is an open question whether we move the data to Telemetry: As of **London Jun2016**, their tools are sufficiently powerful to support ActiveData use cases. Open questions remain:

- How strict must the schemas be? How much ETL work is required to move data to Telemetry? ActiveData is a semi-structured "data lake" that does not fit into explicit-schema databases.  Will it look like the crash-stats migration? [Publish public crash stats to the data platform (1273657)](https://bugzilla.mozilla.org/show_bug.cgi?id=1273657)
- What is the query latency; is it fast enough to drive dashboards?
- What is the size limit, or is there a cost barrier?

Answering these questions will not provide us with resolution: The BI strategy team is looking into large-data solutions from vendors that can support Telemetry and ActiveData in a centralized way. The technology may change again.


Architectural Options
----------------------

There are three architectural classes that can solve this problem. I believe it is better we decide on the architectural class, rather than a specific solution. This will guide our choice of technology for at least a few years.

### Current architecture - Big/Hot

A single warehouse with low latency access to raw data and aggregates. Young-company BI vendors offer this type of solution to trillions of records, or more.  

* Problems listed above
* Are they good investments? 
* Are we getting value? Do we believe there is future potential value?
* Costly:  We should properly fund this. 

### Alternate Architecture - Big/Cold with Small/Hot extracts

Central data warehouse with fast, focused, extracts to satellite datamarts. Similar to Telemetry's architecture, and the most common BI architecture that balances resources. 

* Two extract stages: Inevitably one will require table schemas, transformation scripts, and schedule. 
* Adds resistance to ad-hoc analysis

### Alternate Architecture - Small/Hot

Small focused apps: OrangeFactor, Perfherder, AreWeFastYet  

* Proactive ETL focused on answering specific questions (limiting scope) 
* Can not scale
* Anomalies are invisible
* No Adhoc queries - turn around time will be days, at best. We can assume it will not be done.


## Other Mozilla Datasets

What else is out there? How big are they? How fast do they respond?

- Soccoro **CrashStats** 60M records/6 months x 10Kb
- Telemetry **synch_stats.rollup**  192M records
- Telemetry **synch\_stats.device_counts** 174M records
- Telemetry **presto.crash_aggregates** 315M records (count in 4sec, small schema, stats)


Pricing Review 
--------------

Looking at market rates for comparative pricing. Enough money may solve the problem.

 * ES Pricing [link](https://www.elastic.co/cloud/as-a-service/pricing) $4000/month for 256G mem/6T SSD
 * ActiveData [link](https://docs.google.com/spreadsheets/d/1lb6yQdIZZVOggd_0pAt__NHleNtv3XSZ_PZ6IEkMbFs/edit) $4000/month for 750G mem/150T SSD
 * Require $1000/month for ETL pipeline in either case                


## History of ActiveData

Generally, this is a story about scope creep.

* The ETL effort required $4/hour to keep up with the log volume. Some logs were gigabytes when expanded; and Python is terrible at catching OoM errors.  SpotManager was made to actively bid, and setup, ETL machines to reduce cost by 10x, or more. 
* As soon as we could visualize the Pass/Fail, and calculate fail rates, the next request was to expose the reasons for failure (not ingested at the time). This broke most assumptions: ES would be required to hold a billion records, and the query lanaguage was especially complicated when dealing with annotations (effectively joins).
* Interest in test results waned for interest Buildbot test times, build times, and the individual steps, then OrangeFactor, and then TaskCluster. Hg was also included to include push info to all records; which required a cache to prevent crushing hg.m.o.
* Reftests started using structured logs, Taskcluster test results were then ingested.  First doubling, and then doubling again. Now at 10billion records; ES could not handle both the size and diversity of indexes; constant crashing because load would land on single machine. ShardBalancer was not completed in time...
* CodeCoverage gets turned on for most try builds: 20billion records ingested in a single day, 100billion records in total before turned off. ETL array could not keep up.


## Previous Projects

ActiveData is one part of an overall arc towards a queryable datastore that does not require ETL to load. 

### BugzillaETL

What was learnt

* Elasticsearch is a real-time replacement for Hadoop
* Common data timeline transformations (slowly changing dimensions)
* JSON solves the denormalization problem
  * many2one (a variation on properties)
  * one2many (nested documents)
* JSON solves the more common data migrations (add/remove columns)

### DatazillaAlerts

What was learnt

* Single instance MySQL can not handle data at scale, while still being normalized
* ES can handle 100M records, on single instance, because perf data fits in snowflake data model
* Tracking Alert lifecycles is the complex problem

Dashboarding Problem 

* BI Tools - Graphical or SQL - both are dead ends to further automation
* 



## Past Work

A review of past work to help characterize where the time went.  Only includes the ETL, and does not include the ActiveData service, or the few other satellite applications required. 

**PORTLAND** 

- Jan 2015 dealing with gigabyte logs
- Feb 2015 drive space limits 
- Feb 2015 robust logging to deal with volume 300,000,000 per week
- Mar 2015 Redshift strict typing, slow: Leart that a new database takes time to tune
- Apr 2015 SpotManager to reduce cost of ETL jobs 
- May 2015 Too much work in one queue, split by task type
- May 2015 Import talos
- May 2015 import hg
- May 2015 Add etl property to all records for easier tracing ETL pipeline mistakes
- Jun 2015 Fix timeouts in mozilla pulse (add as vendor project and wrap)
- Jun 2015 Hg caching
- Jun 2015 Add subtests - Add deep query ability to ActiveData

**WHISTLER**  

- Jul 2015 Add beta branch
- Jul 2015 Shard balancing problems
- Jul 2015 coordinator node to send requests to spot zone, not backup zone
- Aug 2015 Fix shutdown, many log enhancements and special cases 
- Aug 2015 Add performance to ActiveData
- Sep 2015 Upgrade ES from 1.4.* to 1.7.* - Change from MVEL to jRuby
- Oct 2015 Add buildbot and mozharness steps
- Nov 2015 Start taskcluster import - Test Infrastructure metrics
- Dec 2015 Perfherder import changes

**ORLANDO**

- Jan 2016 Dealing with data volume 
- Feb 2016 Add code coverage
- Mar 2016 Optimize memory usage in ETL machines
- Mar 2016 Handle DAG paths of ETL Pipeline
- Apr 2016 Fix ETL pipeline with multipaths
- May 2016 Taskclsuter importing, long tail of odd tasks
- May 2016 Dealing with 1,000,000,000 per week
- Jun 2016 Make manager machine for CodeCoverage, TestFailures, SpotManager, and buildbot jobs

**LONDON** - Telemetry has usable data tools

- Jul 2016 Vacation
- Aug 2016 Import TH, Deal with long-time problem of incomplete files
- Sep 2016 TH Import failed, OFv2 on pause, Resume End-to-End times project. Add Shard Balancer 
- Oct 2016 TC properties all changed, updated ETL to deal.  
- Oct 2016 CodeCoverage 100,000,000,000 per week



