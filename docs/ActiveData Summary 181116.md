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

**Example document**

    {"a": {"b": 3}}

**ES logical representation**

| `a.b` |
| ----- |
|   3   |

With this understanding, If we include the datatype in the column name, we can store multiple types in the same "property".  In this case we use `~n~` to indicate numbers:

| `a.b.~n~` |
| --------- |
|     3     |
  
Which would make our document look like:

    {"a": {"b": {"~n~": 3}}}

but that form is never realized inside Elasticsearch: Storing type information in the column names should have no impact on storage size or query speed.

### One tiny detail

While we rewrite column names, we can do the same for nested object arrays:

**Example document**

    {"a": [{"b": 3}]}

**ES logical representation**

| `a.~N~.b` |
| --------- |
|     3     |

we use the `~N~`, for "nested", to distinguish between the inner object `{"b":3}` and same-named nested array `[{"b":3}]`.  Again, this would have little impact on Elasticsearch, but gives use the ability to, truely, store any document, of any shape, and have it properly indexed.

## The Great Leap Forward!

On July 2017, work begun to upgrade ActiveData from Elastisearch 1.7 to 5.x. The work was packaged as an Outreachy project: [Change ActiveData so it can handle the extra type information in the column names](Outreachy%20Proposal.md). There was already a primitive JSON rewriter; converting JSON into "typed JSON"; which is used to insert the correct JSON documents into ES; and can be used by the test harness to leverage the existing test suite.

And then it got complicated

## Reality

**HTTP protocol is more strict** - ESv5 required mimetype headers, and distinguished between GET/POST in ways the old version did nt care. This is not a big deal, except the student had to start with debugging the test harness and startup rather than fixing tests. 

**Query Translation** - The known work progressed as expected. There was going to be some complication with nested queries, but they were not a priority for the Outreachy student.

**New expression language** - ElasticSearch is on its 3rd (?4th?), scripting language. The first was MVEL (version 0.9), next was Groovy (version 1.7), and the latest is called Painless. There are other scripting languages, but they are not important to this story. This was more than enough work for the student; the script translator was copied and changed to handle the new language. By having two, but similar script translators we can use a text `diff` to ensure fixes in one can be translated to the other.

**Rejecting constants!?!** - The script translator showed an interesting problem; [the following script does not compile](https://github.com/elastic/elasticsearch/issues/25729):

    false ? 0 : 1 
  
Such code would never be written by a human, but the ActiveData script translator simply compounded parametric code strings to generate code. Elasticseach's Painless would recognizing the constant, and reject it. I was surprised such code was added to make Painless *harder* to use. If ActiveData was going to generate Painless, it would have to perform constant propagation. That's a problem. 

### Retrospective

Transpiling comes in 4 major forms, each has benefits and detriments. ActiveData did not pick the correct form

* **String Concatenation** - Transpiling can be super simple: Simply concatenate code to achieve the desired code. For example in SQL:<br>`sql = "SELECT id FROM "+quote_column(my_table)+" WHERE name = "+quote(my_name)`<br> 
* **Code Templates** - When string concatenation gets arduous, routine, or dangerous we can use parametric code templates that help with correctness and complexity:
```sql
template = "SELECT id FROM {{table}} WHERE name = {{name}}"
sql = expand(template, table=my_table, name=my_name)
```
this is what ActiveData did, and it worked well; the only problem is the resulting code can be obtuse. But that was for the machines to worry about.
* **First order expressions** - to perform constant propagation we need to rearrange instructions. To rearrange instructions we need an object representation of each instruction for the code to manipulate. The ActiveData script translator added a class for each instruction. This was not complicated, just arduous, as there is a lot of boilerplate for each operation. It was necessary for the few constant propagation that Painless required. Of course, this form opened up allure of expression simplification in general; not just for code optimization, but to generate code that is less obtuse, which made it easier to verify correctness during debugging.
* **Optimizing Compiler** - Beyond expression simplification are a host of optimization strategies and the data structures used to support them. This is not done in ActiveData.   

When writing a transpiler consider the level of complexity you require. The jump from code templates to expression simplification is large enough to demand pause: Consider if there would be another strategy for the overall problem. In this case I should have considered if ActiveData should be retired. My search for an ActiveData replacement was more pronounced now: Can we use Amplitude? Can we use Spark? Why do all the Dremel encoding libraries suck a nested object encodings? There appeared to be no good solutions 

## Upgrade resumes! (Q3 2017)

September, October and November was spent passing tests that required Painless scripting.

As 2017 came to a close ActiveData was ready for deployment. A few nodes of the new cluster were setup, and ingestion was started.  The various services were connected to find the long tail of production bugs 


## AWS - now worse!

The new cluster was showing poor performance despite handling a small fraction of the data. Ingestion was so slow it could not keep up with the ETL pipeline. Moving 10gig shards of data between nodes took days instead of minutes.  

The new cluster and the old cluster had the same hardware: Same instance types, same ephemeral drives, same EBS drives.  The new cluster showed no noticeable CPU usage, no noticeable network usage, no drive usage. Random drive tests showed they performed as expected. Still ingestion was slow.  

* **Could it be the new Typed JSON?** - No, the actual JSON was larger, but the old cluster was not showing more disk usage then he old cluster, like theory predicted.  And that would not explain the very slow shard movement, which seemed to work out to a very low kilobytes per second.
* **Could it be the EBS drives?** - No, they are the same drives as the old cluster used. 

After reviewing the esoteric Elasticsearch settings, turning off JFS on linux, attempting different ingestion techniques, and in desperation, as the months go by with no upgrade, I tested `d2` instances with thier large, local, ephemeral storage: Performance was acceptable!

Was it the EBS drives? Yes, and no: Amazon had changed the billing structure on EBS sometime during 2017; ***new drives were billed according to the new rules and new performance characteristics, while the old drives maintained their legacy billing and legacy performance***. Old EBS magnetic drives did not impact network usage; either the drives were on a separate NAS network, or their network usage was not metered. The new EBS usage showed up in network usage, was bounded by network limits, and had new pricing limits based on request rate or data volume.     

We could no longer use EBS with Elasticsearch. In theory, it should never have been used on EBS, but the Magnetic EBS drives were a sweet deal while it lasted.

## Dockerize Bugzilla-ETL with ActiveData

ActiveData is a query translation service, and it works on any Elasticsearch cluster. Mozilla had a Elasticsearch v0.9 cluster, which stored all Bugzilla bug snapshots over all time, that required upgrading.  Summer 2018 was spent dockerizing the ETL pipeline, and ActiveData to work on a Elasticsearch-as-a-service.  

The biggest blocker, noticed during the Bugzilla-ETL deploy, was the metadata management in ActiveData was too slow. A database was required to save data between instances and runs because it was proving too expensive to accumulate at startup. Metadata management was turned off on the Bugzilla-ETL instance to ensure it was performant; it caused test breakage, but it was a breakage we can live with in the short term.    

During this time, ActiveData was deployed: Not officially, and it still did not pass all tests, but it was good enough for Coverage queries and good enough to support the ETL pipeline. 

## Upgrade ActiveData. No, Really.

The fourth quarter of 2018 was to start. The ActiveData upgrade was looking like a failure. So, with the cluster working for months now, and other projects being done, I was ready for the final deploy.

A database was added to metadata management. But the production deployment showed it was still too slow.

The IP was redirected on Sunday October 21st.

## Everything breaks

The IP redirect revealed the number of services and dashboards and services that were using ActiveData, and it showed the range of queries it was failing to process properly.  Since going backward is more work than going forward, I started a two week, intense, post-deploy fire fighting operation. 

Essentially, ActiveData's test suite did not cover all use cases, and the real word exposed them.

* The queries into nested object arrays is more prevalent, and diverse, than imagined. With Typed JSON working, Elasticsearch queries on nested documents was now giving correct results, and were more complicated than before. I added the required tests, but for each test I would pass, another would break. I was spending too much time trying to make the code templates work before I realized  the problem: The query translator had to move from using code templates to using first order expressions so that they could be rearranged to get the correct result.
* Elasticsearch is strictly typed; it can store Boolean columns. This broke a number of tests. Elasticsearch v1.7 stored Booleans as strings `"T"` and `"F"`. As a result, there were queries that used the following logic:
```
{"when": {"eq": {"result.ok": "F"}}, "then": 1}
```
The query translator had to identify `result.ok` as a Boolean column, and `"F"` as equivalent to `false`.
* Elasticsearch aggregation over a Boolean column results in `1` and `0` not `true` and `false` like would be expected. More logic was added to ensure ActiveData did not make the same error.
  

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
* Gunicorn calling convention has changed;


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

