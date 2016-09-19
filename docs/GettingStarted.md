Getting Started with ActiveData
===============================

ActiveData has over 10billion records available to you live, but now what?


ActiveData allows you to form queries over a dynamically-typed database.  This means the schema can be changing and evolving over time.   This "migrating" 


The biggest hurdle is learning the various schema intricacies relevant to the data that will answer your question. This will take time   


There are only two paths forward:

### Best Option: Please Ask a Question

Please ask someone, [like myself](klahnakoski@mozilla.com), how to pull he information you need. This is the most efficient use of both our time.  You are probably came to ActiveData because you have a question that data may answer for you.  If you only have a question every couple of months, you should not be wasting your time re-learning the schema, and the query language.  By asking a question, you will get the data you need faster. 

Asking just one question is not enough; the schemas are enormous and and growing; trying to find what you need will take time;  only after several questions you will have a constellation of queries that are relevant to you.  These queries will serve as meaningful examples upon which you can learn the query lanaguage, and will give you hints about the topology of data you are most interested in.   

   
### Second Option: Explore the schemas yourself

If you plan to interact with ActiveData over an extended period of time, then it will be worth your time to learn the query language and the schema that's relevant to your project.  If you have used raw SQL in the past to explore the contents of a database, then you will be familiar with the process of exploring ActiveData.  In general, a database schema is only a superficial description of the data; you will need to know much more about the data before you can use it effectively.  For example, you must be familiar with the domain of values for given columns, their cardinality, and which values are most common. There will be business-specific meaning given to he values, and there will be important correlations between columns you can leverage.

Ready to explore?

### 1) Choose a table

As of this writing (Septermber 2016) the following tables are available. 

	{"from": "meta.tables"}  # only available in dev branch 

* **unittest** - All individual test results (approx. 10billion from the past 3weeks)
* **jobs** - All buildbot jobs 
* **repo** - All revisions found in hg.mozilla.org
* **task** - All Taskcluster takss
* **coverage** - line level code coverage information
* **coverage-summary** - Function - level summary of `coverage`
* **orange_factor** - A almost-live copy of the Orange Factor database 
* **perf** - All performance measures 
* **saved_queries** - All queries that have been sent to ActiveData
* **test_failures** - A summary of test failures 
* **treeherder** - A sample of Treeherder data 

###2) Investigate the schema

Knowing the columns names and types is not enough, knowing the set-of-values that columns take on is often more important.  The first step is to show some records:

	{"from": "jobs"} 

<div style="align-right">http://activedata.allizom.org/tools/query.html#query_id=IkrCzx5d</div>

The above query is the simplest query you can send ActiveData, and it will return (limited to 10) JSON documents found in the `jobs` table. ActiveData calls these JSON documents "records" to stay consistent with database terminology.

The records are complex, so you should use a large-screen JSON formatting tool to view the query result.  There is a page that has [details about what is in a `jobs` record](https://github.com/klahnakoski/ActiveData/blob/dev/docs/Jobs%20Schema.md), but we will focus on just one property now:

	{
		"from": "jobs",
		"groupby": "build.platform",
		"limit": 1000
	}

<div style="align-right">http://activedata.allizom.org/tools/query.html#query_id=EUeEubeR</div>
  


 


  