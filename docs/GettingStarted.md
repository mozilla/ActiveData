Getting Started with ActiveData
===============================

> **me:** ActiveData has over 10billion records available to you live!!<br>
> **you:** Yea, so, now what?

You probably got here from the [ActiveData Query Tool](http://activedata.allizom.org/tools/query.html), which is a bare-bones tool for send queries to the ActiveData service endpoint. At best it is hard to use, and at worst it is barrier to understanding. 

To understand ActiveData you have two paths forward:

## Best Option: Please Ask a Question

If you an occasional user, please [ask me, `:ekyle`](mailto://klahnakoski@mozilla.com), for a query that will pull the information you need; this is the most efficient use of your time. You probably came to ActiveData because you have a question that data may answer for you. If you only have a question every couple of months, you should not be wasting your time (re)learning the schema, and the query language. By asking a question, you will get the data you need faster, and I get to learn what people are interested in and make dashboards that present that information in a meaningful way.

Asking just one question is not enough. The schemas are enormous and and growing; trying to find the needle in the haystack is not a good use of your time. After asking several questions, you will have a constellation of queries that are relevant to your role. These queries will serve as meaningful examples upon which you can learn the query language, and will give you hints about the topology of data you are most interested in.
   
## Second Option: Explore the schemas yourself

If you plan to interact with ActiveData over an extended period of time, then it will be worth your time to learn the query language and the relevant schema. If you have used raw SQL in the past to explore the contents of a database, then you will be familiar with the process of exploring ActiveData. In general, a database schema is only a superficial description of the data; you will need to know many more details before you can use it effectively. For example, you must be familiar with the domain-of-values for given columns, their cardinality, and which values are most common. There will be business-specific meaning given to the values, and there will be important correlations between columns that you can use to reduce apparent complexity.

### 0) Prerequisites

You will need

1. Read the [unittest tutorial](jx_tutorial.md), it touches on the query language, while this document covers exploration.
2. A large-screen JSON formatting tool.  I made [my own formatting tool](http://people.mozilla.org/~klahnakoski/JSON-Formatter/) because I wanted my JSON packed tight.
3. Access to [the ActiveData Query Tool](http://activedata.allizom.org/tools/query.html)   
4. Analysis tools, dashboarding tools, or something to handle the data and make a finished product.  ActiveData gives you access to data; you will be responsible for making that data meaningful.
5. Motivation, or patience. The biggest hurdle is learning the various schema intricacies relevant to the data that will answer your question. This will take time.


### 1. Choose a table

As of this writing (September 2016) the following tables are available. 

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

### 2. Investigate the schema

Knowing the column names and types is not enough.  Knowing the set-of-values that columns take on is often more important. The first step is to show some records so you can get a feeling for both:

	{"from": "jobs"} 

<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=IkrCzx5d">http://activedata.allizom.org/tools/query.html#query_id=IkrCzx5d</a></div>

ActiveData accepts [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md), which are JSON objects with multiple properties, called "clauses" in tribute to the SQL language it tries to mimic. The above query is the simplest query you can send ActiveData: It has one clause, the `from` clause, and it returns (limited to 10) JSON documents found in the `jobs` table. In this case, ActiveData returns JSON documents, but we will always call them "records" to stay consistent with database terminology.   

The records returned from ActiveData are usually quite large, so you will need a large-screen JSON formatting tool to view the query result.  There is a page that has [details about what is in a `jobs` record](https://github.com/klahnakoski/ActiveData/blob/dev/docs/Jobs%20Schema.md), but we will focus on just a few columns for now.

Here is an example result, with the `data` property collapsed:
  
	{
		"meta":{
			"timing":{
				"total":3.13,
				"jsonification":0.006246,
				"es":1.3581440448760986
			},
			"saved_as":"bi0bGAWF",
			"es_query":{
				"query":{"filtered":{"filter":{"match_all":{}}}},
				"from":0,
				"size":1
			},
			"content_type":"application/json",
			"format":"cube"
		},
		"edges":[{
			"domain":{"max":10,"interval":1,"type":"rownum","min":0},
			"name":"rownum"
		}],
		"select":[{
			"name":".",
			"value":"."
		}],
		"data":{...}
	}

There are four major properties:

* **meta** - details about what was sent to the backend ES, how long it took, **and the format of the data**
* **edges** - a fully specified edge, with all defaults made explicit. This summarizes the dimensions of `data` returned. In this case there is only one dimension. We will talk more about this later.
* **select** - a fully specified `select` clause, with all defaults made explicit. Notice the `"value": "."`, which means the-value-is-the-whole-record.
* **data** - the result of the query, in the format specified by the `meta.format` property.

> ActiveData's columns are not limited to primitive types; JSON objects are treated as
> values too. You can read more about [how to use the `select` clause to shape data](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx_clause_select.md), 
> but it is not necessary for the purpose of this guide.

Expanding the `data` property a couple of levels, we can see: 

	"data":{".":[
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{}
	]}

The `data` is in `cube` format, which is a columnar format: Each key-value pair is the name of the column, and a (multidimensional) array of values, respectively. This particular cube has only one output column with name "." (dot), and one dimension: Which turns out to be a complicated way to return a simple list of results.

You can request the query result be returned in `list` form by adding `"format":"list"` to your query.

	{
		"from":"jobs",
		"format":"list"
	}

<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=NHNx3_IS">http://activedata.allizom.org/tools/query.html#query_id=NHNx3_IS</a></div>

which will give you `data` without the obtuse "." property:

	"data":[
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{},
		{"run":{},"repo":{},"other":{},"build":{},"action":{},"properties":{},"etl":{}
	]}

> The [ActiveData Query Tool](http://activedata.allizom.org/tools/query.html) attempts to 
> re-format the raw JSON response as a familiar database table; which often hides the true 
> format. Use the Raw JSON view to see the effect of `format`.      

### 3. Inspect the domain of columns

Examples, only give hints about the variety of records available. Most of your time will be spent understanding the domain of the columns, so you can filter out what is not relevant to your needs.

	{
		"from": "jobs",
		"groupby": "build.platform",
		"limit": 1000
	}

<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=EUeEubeR">http://activedata.allizom.org/tools/query.html#query_id=EUeEubeR</a></div>

The `build.platform` property is used heavily when querying `jobs` because most jobs have platform-specific behaviour. This query shows all the available platforms.  The `limit` clause is used to ensure we get enough rows to get a good feel for the domain, but not so many that we break the browser.  For example, the `build.date` field has over 380K unique values, which you probably do not want all returned:

	{
		"from":"jobs",
		"groupby":"build.date",
		"limit":1000
	}

<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=xNGik2of">http://activedata.allizom.org/tools/query.html#query_id=xNGik2of</a></div>

### 4. Restrict your queries by adding to the `where` clause

As you refine what you are looking for, you will build up a `where` clause to exclude the "junk" you are not interested in. In that light, let us restrict ourselves to Windows jobs, and let's pull some examples from the past week:

	{
		"from":"jobs",
		"where":{"and":[
			{"prefix":{"build.platform":"win"}},
			{"gt":{"build.date":"{{today-week}}"}}
		]},
		"limit":10
	}
 
<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=Bfj9mU+f">http://activedata.allizom.org/tools/query.html#query_id=Bfj9mU+f</a></div>

As you can see, I am pulling more examples. Examples are important; they give good hints about the schema for the specific records you are interested in. ActiveData is a dynamically typed datastore; the schema depends on what records you are looking at. It is important to go back, periodically, and pull some examples so you can verify your assumptions about the data.

For instance, Windows `jobs` can be split by 3 major categories:

	{
		"from":"jobs",
		"groupby":"action.type",
		"where":{"and":[
			{"prefix":{"build.platform":"win"}}
		]},
		"limit":10
	}

<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=XXbKWY5A">http://activedata.allizom.org/tools/query.html#query_id=XXbKWY5A</a></div>

Here is the body of the response 

	"header":[{"name":"action.type"},{"name":"count"}],
	"data":[
		["test",8103826],
		["talos",1234390],
		["build",260094],
		[null,186840]
	]

Using the "groupby" clause forces ActiveData to assume you want a `table` format. Tables have `data`, plus an additional `header` to help decode the row tuples.  You can use `"format": "list"` to get the same data in list format:

	"data":[
        {"action":{"type":"test"},"count":8103826},
        {"action":{"type":"talos"},"count":1234390},
        {"action":{"type":"build"},"count":260094},
        {"count":186840}
    ]

No matter the format, the [ActiveData Query Tool](http://activedata.allizom.org/tools/query.html) will interpret all three forms the same, as a table:

|action.type  |count   |
|-------------|-------:|
|build        |  260094|
|talos        | 1234390|
|test         | 8103826|
|             |  186840|

Looking at the table, there appears to be a significant number of no-type jobs. We could look at examples of those, `{"missing":"action.type"}`, and I admit I am curious too, but we will focus on tests.

Both `test` and `talos` jobs have a `run.suite` property that is not used by builds, which can be seen in this query, which lists all `action.type`, `run.suite` pairs:

	{
		"from":"jobs",
		"groupby":["action.type","run.suite"],
		"where":{"and":[{"prefix":{"build.platform":"win"}}]},
		"limit":100
	}

### 5. Rinse, and Repeat...

We continue to add to the `where` clause: Restricting ourselves to `mochitest`. We also notice jobs run off of multiple branches:

	{
		"from":"jobs",
		"groupby":"build.branch",
		"where":{"and":[
			{"prefix":{"build.platform":"win"}},
			{"eq":{"run.suite":"mochitest"}},
			{"gt":{"build.date":"{{today-week}}"}}
		]},
		"limit":1000
	}
 


<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=RwTskAQx">http://activedata.allizom.org/tools/query.html#query_id=RwTskAQx</a></div>

Let us restrict ourselves to `mozilla-inbound`, the principal branch for Firefox:

	{	
		"from":"jobs",
		"where":{"and":[
			{"prefix":{"build.platform":"win"}},
			{"eq":{
				"run.suite":"mochitest",
				"build.branch":"mozilla-inbound"
			}},
			{"gt":{"build.date":"{{today-week}}"}}
		]},
		"limit":10
	}


<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=gXIycgdT">http://activedata.allizom.org/tools/query.html#query_id=gXIycgdT</a></div>
 

### 6. Request Aggregates

Individual records are useful for understanding the data and building a query. They are also useful to the people that are familiar with the events that produced those records. But, you will need aggregate statistics to get a better holistic understanding of trends.

Once you are confident your `where` clause is focused on the records you're interested in, you can start requesting aggregates. Up to this point in this guide, ActiveData has either returned individual records, or provided a `count` (which is the default aggregate). [We can request many other aggregates](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx_reference.md#selectaggregate-subclause)

In the following query we are going to use `edges`, which acts much like `groupby`, [see more](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx_clause_edges.md), but accepts an explicit `domain` to join-and-group the data. In this case, we are bucketing the past month of `action.start_time` to days.  We also define a `select` clause to give us the `average` duration, and the `count` for each of those buckets. 

	{
		"from":"jobs",
		"select":[
			{"value":"action.duration","aggregate":"average"},
			{"aggregate":"count"}
		],
		"edges":{
			"value":"action.start_time",
			"domain":{
				"type":"time",
				"min":"today-month",
				"max":"today",
				"interval":"day"
			}
		},
		"where":{"and":[
			{"prefix":{"build.platform":"win"}},
			{"eq":{"run.suite":"mochitest","build.branch":"mozilla-inbound"}},
			{"gt":{"build.date":"{{today-month}}"}}
		]},
		"limit":10
	}

<div style="text-align:right;"><a href="http://activedata.allizom.org/tools/query.html#query_id=G92jrelZ">http://activedata.allizom.org/tools/query.html#query_id=G92jrelZ</a></div>
 

### 7. Further processing

Whether you are pulling long streams of raw records, or pulling compact aggregates, your adventure with ActiveData is over, but your job is not complete. The most enlightening answers come from data that has undergone a complex series of transformations; required to properly correlate it to other data, and organize it for presentation. ActiveData does not do that, it is not an analysis tool, its responsibilities are to make a large amount of data searchable, provide basic aggregates, and allow data reshaping. Analysis and charting is better done by other tools.

ActiveData is a service. Once you have a prototype query that gets the data you want, you must go to your analysis tool and import that data. ActiveData is fast enough that you can make your request at presentation time, so your presentation is near-real time.

You can pull information into the browser:

```javascript
	$.post(
   		"https://activedata.allizom.org/query",
   		'{"from":"jobs"}',
   		function(data, status){
   			console.log(JSON.stringify(data));
   		}
   	);
```

You can pull information into your (Python) server:

```python
	response = requests.post(
		"https://activedata.allizom.org/query", 
		data=json.dumps({"from":"jobs"})
	)
	print response.content
```

Similarly for other languages and tools.

## Summary

I hope I have given you a good sense for how to explore the ActiveData schema, and how to build a query that will give you the data you are interested in.  More specific documentation regarding the intricacies of JSON Query Expressions, and ActiveData, is all centrally linked on the [main documentation page](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md)
