# ActiveData Upgrade Postmortem


## About ActiveData

ActiveData is a query translation service; accepting parsed SQL-like expressions, translating to Elasticsearch query language, and compacting the results coming from Elasticsearch.

ActiveData provides two important features inspired by [MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions):

1. Nested object arrays can be queried like any other table; as if joined with the parent documents. This simplifies queries; avoiding Elasticsearch's "nested" queries, or SQL's join-with-explode idiom.  
2. Ability to query into a dynamically typed JSON document storage; The strict schema is managed by code, and ActiveData's query expressions need little or no change as the schema expands (migrates) over time.

## Short History

ActiveData has been around since 2015 and has been using Elasticsearch v1.4, or v1.7, since that time. These early versions of Elasticsearch allowed us to pour just about any JSON document into it, and it would "just work". ActiveData was a relatively simple translation service that simplified queries, especially ones involving multiple dimensions or complex expressions.   

Elasticsearch v2.x and beyond, demand a strict JSON schema; these later versions can no longer accept documents dumps. Now, the user must ensure the schema stays consistent at insert time, rather than waiting for query time. ActiveData would require an upgrade; it must take over the handling of dynamic schemas.

## Understanding the Effort

Elasticsearch is fast because it indexes everything and stores nothing. If it is asked to store data it stores the primitive values as columns. ES does not store objects; so empty objects and `null` are the same thing.  In any case, documents are not treated as objects with properties; rather as a set columns, where each column name is the path to the primitive value. 

**Example document**

    {"a": {"b": 3}}

**ES logical representation**

| `a.b` |
|:-----:|
|   3   |

With this understanding, If we include the datatype in the column name, we can store multiple types in the same "property".  In this case we use `~n~` to indicate numbers:

| `a.b.~n~` |
|:---------:|
|     3     |
  
Which would make our document look like:

    {"a": {"b": {"~n~": 3}}}

but that form is never realized inside Elasticsearch: Storing type information in the column names should have no impact on storage size or query speed.

### Nested Object Arrays

While we rewrite column names, we can do the same for nested object arrays:

**Example document**

    {"a": [{"b": 3}]}

**ES logical representation**

| `a.~N~.b` |
| --------- |
|     3     |

we use the `~N~`, for "nested", to distinguish between the inner object `{"b":3}` and same-named nested array `[{"b":3}]`.  Again, this would have little impact on Elasticsearch, but gives use the ability to, truely, store any document, of any shape, and have it properly indexed.

[Read more about the original project plan](Outreachy%20Proposal%20170223.md)

## Upgrade Overview

<img alt="chart of hours" src="./ActiveData%20Summary%20181116%20hours.png"/>

## Start Upgrade

In May 2017, work begun to upgrade ActiveData from Elastisearch 1.7 to 5.x. The work was packaged as an Outreachy project: [Change ActiveData so it can handle the extra type information in the column names](Outreachy%20Proposal%20170223.md). There was already a primitive JSON rewriter; converting JSON into "typed JSON"; which is used to insert the correct JSON documents into ES; and can be used by the test harness to leverage the existing test suite.

And then ...

### Reality

**HTTP protocol is more strict** - ESv5 required mimetype headers, and distinguished between GET/POST in ways the old version did not care. This is not a big deal, except the student had to start by learning the details of HTTP to debug the test harness and startup rather than working on the project. 

**Query Translation** - The known work progressed as expected. There was going to be some complication with nested queries, but they were not a priority for the Outreachy student until late in the project.

**New expression language** - ElasticSearch is on its 3rd (?4th?), scripting language. The first was MVEL (version 0.9), next was Groovy (version 1.7), and the latest is called Painless. There are other scripting languages, but they are not important to this story. This was more than enough work for the student; the script translator was copied, and then changed to handle the new language. By having two, but similar, script translators we can use a text `diff` to ensure fixes in one can be translated to the other.

**Rejecting constants!?!** - The script translator showed an interesting problem; [the following script does not compile](https://github.com/elastic/elasticsearch/issues/25729):

    false ? 0 : 1 
  
Such code would never be written by a human, but the ActiveData script translator simply compounded parametric code strings to generate code. Elasticseach's Painless would recognizing the constant, and reject it. I was surprised such a check was added to make Painless *harder* to use. If ActiveData was going to generate Painless, it must also perform constant propagation. That's a problem. 

### Retrospective Tangent

Implementing constant propagation was a problem; ActiveData was not designed for it, and required a rewrite. To better explain, I must go off on a tangent. 

Transpiling comes in 4 major forms, each has benefits and detriments:

* **String Concatenation** - Transpiling can be super simple: Concatenate strings to achieve the desired code. For example in SQL:<br>&nbsp;&nbsp;&nbsp;&nbsp;`sql = "SELECT id FROM " + quote_column(my_table) + " WHERE name = " + quote(my_name)`
* **Code Templates (macros)** - When string concatenation gets arduous, routine, or dangerous we can use parametric code templates that help with correctness and complexity:<br>&nbsp;&nbsp;&nbsp;&nbsp;`template = "SELECT id FROM {{table}} WHERE name = {{name}}"`<br>&nbsp;&nbsp;&nbsp;&nbsp;`sql = expand(template, table=my_table, name=my_name)`<br> this is what ActiveData did, and it worked well; the only problem is the resulting code can be obtuse. But that was for the machines to worry about.
* **First order expressions** - to perform constant propagation we need to rearrange operations. To rearrange operations we need an object representation of each operation for the code to manipulate. The ActiveData script translator added a class for each operation. This was not complicated, just arduous, as there is a lot of boilerplate for each operation. It was necessary for the few constant propagations that Painless required. Of course, this form opened up the allure of expression simplification in general; not just for code optimization, but to generate code that is less obtuse, which made it easier to verify correctness during debugging.
* **Optimizing Compiler** - Beyond expression simplification are a host of optimization strategies and the data structures used to support them. This is not done in ActiveData.   

When writing a transpiler consider the level of complexity you require. The jump from code templates to expression simplification is large enough to demand pause: Consider if there would be another strategy for the overall problem. In this case I should have considered if ActiveData should be retired. My search for an ActiveData replacement was more pronounced: Can we use Amplitude? Can we use Spark? Why do all the Dremel encoding libraries suck at nested object encodings? There appeared to be no good solutions 

## 2nd Attempt (Q3 2017)

September, October and November was spent passing tests that required Painless scripting.

As 2017 came to a close ActiveData was ready for deployment. A few nodes of the new cluster were setup, and ingestion was started.  The various services were connected to find the long tail of production bugs 

## Slow Cluster Problems

The new cluster was showing poor performance despite handling a small fraction of the data. Ingestion was so slow it could not keep up with the ETL pipeline. Moving 10gig shards of data between nodes took days instead of minutes.  

The new cluster and the old cluster had the same hardware: Same instance types, same ephemeral drives, same EBS drives.  The new cluster showed no noticeable CPU usage, no noticeable network usage, no drive usage. Random drive tests showed they performed as expected. Still ingestion was slow.  

* **Could it be the new Typed JSON?** - No, the actual JSON was larger, but the old cluster was not showing more bytes stored than the old cluster, like theory predicted. Plus, it would not explain the very slow shard movement, which seemed to work out to kilobytes per second.
* **Could it be the EBS drives?** - No, they are the same drives as the old cluster used. 

After reviewing the esoteric Elasticsearch settings, turning off JFS on linux and attempting different ingestion techniques, the months go by with no solution. In desperation, I decided to brute force a solution: I setup a new cluster using `d2` instances with their large local storage. Performance was acceptable!

Was it the EBS drives? Yes, and no: Amazon had changed the billing structure on EBS sometime during 2017; ***new drives were billed according to the new rules and new performance characteristics, while the old drives maintained their legacy billing and legacy performance***. Old EBS magnetic drives did not impact network usage; either the drives were on a separate NAS network, or their network usage was not metered. The new EBS usage showed up in network usage, was bounded by network limits, and had new pricing limits based on request rate or data volume.     

We could no longer use EBS with Elasticsearch. In theory, Elasticsearch should never have been used on EBS, but the Magnetic EBS drives were a sweet deal while it lasted.

## Dockerize Bugzilla-ETL with ActiveData

ActiveData is a query translation service, and it works on any Elasticsearch cluster. Mozilla had a Elasticsearch v0.9 cluster, which stored all Bugzilla bug snapshots over all time, and it required upgrading.  Summer 2018 was spent dockerizing the ETL pipeline, and ActiveData to work on a Elasticsearch-as-a-service.  

The biggest blocker, noticed during the Bugzilla-ETL deploy, was the metadata management in ActiveData was too slow. A database was required to save data between instances and runs because it was proving too expensive to accumulate at startup. Metadata management was turned off on the Bugzilla-ETL instance to ensure it was performant; it caused test breakage, but it was a breakage we can live with in the short term.    

During this time, the main ActiveData instance was deployed: Not officially, and it still did not pass all tests, but it was good enough for CodeCoverage queries and good enough to support the ETL pipeline. 

## Final Upgrade

AS the fourth quarter of 2018 was about to start, the ActiveData upgrade was looking like a failure. So, with the cluster working for months now, and other projects being done, I was ready for the final deploy.

The IP was redirected on Sunday October 21st.

### Everything breaks

Production deployment showed metadata management, backed by a database was still too slow. It was turned off, except for a couple of columns, to ensure production queries still work

The IP redirect revealed the number of services and dashboards that were using ActiveData, and it showed the range of queries that were failing. Since going backward is more work than going forward: I started a two week, post-deploy, fire fighting operation. 

Essentially, ActiveData's test suite did not cover all use cases; moving to production exposed the suite's deficiencies.

* Queries into nested object arrays is more prevalent, and diverse, than imagined. With Typed JSON working, Elasticsearch queries on nested documents was now giving correct results, but were more complicated than before. I added the required tests, but for each test I would pass, another would break. I was spending too much time trying to make the code templates work before I realized  the problem: The query translator had to move from using code templates to using first order expressions so that they could be rearranged to get the correct result.
* Elasticsearch is strictly typed; it can store Boolean columns. This broke a number of tests. Elasticsearch v1.7 stored Booleans as strings `"T"` and `"F"`. As a result, there were queries that used the following logic:<br>&nbsp;&nbsp;&nbsp;&nbsp;`{"when": {"eq": {"result.ok": "F"}}, "then": 1}`<br>The query translator had to identify `result.ok` as a Boolean column, and `"F"` as equivalent to `false`.
* Elasticsearch aggregation over a Boolean column results in `1` or `0` not `true` or `false`. More logic was added to ensure ActiveData did not make the same error.
  
## Success &#9785;

The ActiveData Upgrade was an upgrade disguised as a complete rewrite. Elasticsearch v1.7 had almost nothing in common with Elasticsearch v5.x. I imagine that this is a good example of what changing a backend data store in any application can look like. The upgrade took so long there is no justification for pride or pleasure.

## Things Learnt

If you got here, then please [email me with your thoughts](mailto:klahnakoski@mozilla.com)

Here are some things that could be improved

* **Always start with the most powerful hardware** - The slow ingestion problem was a time sink that could have been avoided if the most powerful hardware was deployed first; then scaled back to match demand. Now that I have arrived at this strategy, I see it stated elsewhere in Elasticsearch documents: Start big to avoid confounding scaling problems with configuration problems; then scale back until you measure a performance impact.<br>There is a cost to deploying expensive hardware, and there is a cost to measurement, especially if you are measuring a large cluster.
* **Replay production queries on new cluster** - It simply did not occur to me to replay the produciton queries on the new cluster until it was too late. I think a dumb mistake like this can be avoided by having a second person being intimately familiar with the project, and verifying the upgrade checklist.<br>This has two costs: There would be no firefight at the end, but that development time would have spread over more weeks, delaying the upgrade even more. Plus, having a second person to ask the right questions and catch this type of error is also a cost, either in training or expertise.

Here things that felt bad, and I have no solution for: 

* **Why did I spend time on constant propagation?** - This took a lot of time, and feels bad because this has been solved many times before: Why can I not download code, like an abstract compiler, to do this for me? If I can, then why is it harder to use than just coding my own solution? This problem is a example of a class of problems I experience quite often: Solutions to common problems can be found in a multitude of software, yet no one has written a library to solve the problem in isolation. Here is a small example: Topological ordering of nodes in a cyclic graph, is something I must write for every new language I learn.
* **Why did I not see the need for first order expressions earlier?** - After the final deploy, and more test cases were added, I was still attempting to use code templates to compose complex nested Elasticsearch queries. This was separate from the script translation, despite it sharing a similar story. Much of my time was spent fixing one test, just to cause a regression somewhere else; a game of wack-a-mole if you will. The code got more complex, but I was always able to get to a green build, eventually. Recognizing I had the wrong abstraction would have helped: How do you know when to stop coding and rethink the problem?



----------

## Notes

Point form notes as I review my timesheets

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

