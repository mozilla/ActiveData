
Search Bugs Fast with ElasticSearch
===================================

Running Examples (Query Tool)
-----------------------------

[ElasticSearch Head](https://github.com/mobz/elasticsearch-head) is a simple
tool for sending general queries.  [Query Tool](../html/QueryTool.html) can be used to prototype Qb
queries, and see their equivalent ES query.  Please ```git clone``` both of these projects and open in your browser.

Schema
------

The history of each bug is stored as a set of documents.  Each document is a
snapshot of the bug between ```modified_ts``` and ```expires_on```.  All times
are in milliseconds since epoch (GMT).

The current schema can be pulled using ElasticSearch Head.  You can view the simpler [schema used by the ETL](https://github.com/klahnakoski/Bugzilla-ETL/blob/df89c80428ae78fd53b4a05bd94c5949130e6898/resources/json/bug_version.json#L105)

Query Current State of All Bugs
-------------------------------

It is common to query the current bug state.  To do this you take advantage of
the fact that current documents have ```expires_on``` set to the deep future.
It is a simple matter to ensure all you queries include

    {"range":{"expires_on":{"gte":NOW}}}

where ```NOW``` is in milliseconds since epoch (GMT).  For example,
1389453465000 == 11-Jan-2014 15:17:45 (GMT) (notice the extra three zeros
indicating milliseconds)

Search for Current Bugs
------------------------

Lets look at all the bugs in a project called **KOI**.  This project is tracked
using the Blocking B2G flag in Bugzilla.  Both Bugzilla and the ElasticSearch
use a ```cf_``` prefix on tracking flags; Our filter looks like
```{"term":{"cf_blocking_b2g":"koi+"}}```.

<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
  "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[
    {"match_all":{}},
    {"and":[
        {"range":{"expires_on":{"gte":1389389493271}}},
        {"term":{"cf_blocking_b2g":"koi+"}}
      ]}
    ]}
  }},
  "from":0,
  <strong>"size":200000,  # Number of documents to return</strong>
  "sort":[]
}</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
  "from":"public_bugs",
  <strong>"select":"_source",  # magic word '_source'</strong>
  "esfilter":{"and":[
    {"range":{"expires_on":{"gte":1389389493271}}},
    {"term":{"cf_blocking_b2g":"koi+"}}
  ]}
}</pre><br>
<i>Qb queries are intended to be more like SQL, with familiar clauses, and
simpler syntax.  Benefits will be more apparent as we push the limits of ES's
query language: Qb queries will isolate us from necessary scripting,
multifaceting, and nested queries</i>
</td>
</tr>
</table>

Just Some Fields
----------------

The bug version documents returned by ElasticSearch can be big.  If you are
interested in larger sets of bugs, and not interested in every detail, you can
restrict your query to just the fields you desire.

<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
  "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[
      {"match_all":{}},
      {"and":[
        {"range":{"expires_on":{"gte":1389389493271}}},
        {"term":{"cf_blocking_b2g":"koi+"}}
      ]}
    ]}
  }},
  "from":0,
  "size":200000,
  "sort":[],
  <strong>"fields":[  # field list
        "bug_id",
        "bug_state",
        "modified_ts"
  ]</strong>
}</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
  "from":"public_bugs",
  <strong>"select":[   # field list
    "bug_id",
    "bug_status",
    "modified_ts"
  ],</strong>
  "esfilter":{"and":[
    {"range":{"expires_on":{"gte":NOW}}},
    {"term":{"cf_blocking_b2g":"koi+"}}
  ]}
}</pre><br>
<i>An array in the <code>select</code> clause will have the query return JSON objects.  No array means the query
returns values only.

</i>
</td>
</tr>
</table>

Aggregation
-----------

ElasticSearch has limited options when it comes to aggregates, but the ones it
does do are very fast.  Here is a count of all KOI bugs by product:

<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
  "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[
      {"match_all":{}},
      {"and":[
        {"range":{"expires_on":{"gte":1389389493271}}},
        {"term":{"cf_blocking_b2g":"koi+"}}
      ]}
    ]}
  }},
  "from":0,
  "size":0,
  "sort":[],
  <b>"facets":{"default":{"terms":{
    "field":"product",
    "size":200000
  }}}</b>
}
</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
  "from":"public_bugs",
  <b>"select":{
    "name":"num_bugs",
    "value":"bug_id",
    "aggregate":"count"
  },</b>
  "esfilter":{"and":[
    {"range":{"expires_on":{"gte":1389389493271}}},
    {"term":{"cf_blocking_b2g":"koi+"}}
  ]},
  <b>"edges":["product"]</b>
}
</pre><br>
<i>In this case, the <code>edges</code> clause is simply a list of columns to group by.</i>
</td>
</tr>
</table>


Open Bugs
---------

The Bugzilla database is dominated by closed bugs.  It is often useful to limit
our requests to open bugs only.  Personally, my strategy is to find bugs *not*
marked closed.  This way new bug states (open or closed) will reveal themselves.
Here is a count of all open bugs by product.


<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
  "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[
      {"match_all":{}},
      {"and":[
        {"range":{"expires_on":{"gte":1389389493271}}},
        <b>{"not":{"terms":{"bug_status":[
          "resolved",
          "verified",
          "closed"
        ]}}}</b>
      ]}
    ]}
  }},
  "from":0,
  "size":0,
  "sort":[],
  "facets":{"default":{"terms":{
    "field":"product",
    "size":200000
  }}}
}
</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
  "from":"public_bugs",
  "select":{
    "name":"num_bugs",
    "value":"bug_id",
    "aggregate":"count"
  },
  "esfilter":{"and":[
    {"range":{"expires_on":{"gte":1389389493271}}},
    <b>{"not":{"terms":{
      "bug_status":["resolved","verified","closed"]
    }}}</b>
  ]},
  "edges":["product"]
}</pre>
</td>
</tr>
</table>


Historical Query
----------------

To query a point in time, we look for records that straddle the point in time
we are interested in (```modified_ts <= sometime < expires_on```).  Here is
look at the number of open bugs back in Jan 1st, 2010:

<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
    "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[
      {"match_all":{}},
      {"and":[
        <b>{"range":{
          "expires_on":{"gt":1262304000000}  # Jan 1st, 2010
        }},
        {"range":{
          "modified_ts":{"lte":1262304000000}
        }},</b>
        {"not":{"terms":{"bug_status":[
          "resolved",
          "verified",
          "closed"
        ]}}}
      ]}
    ]}
  }},
  "from":0,
  "size":0,
  "sort":[],
  "facets":{"default":{"terms":{
    "field":"product",
    "size":200000
  }}}
}</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
    "from":"public_bugs",
  "select":{
        "name":"num_bugs",
        "value":"bug_id",
        "aggregate":"count"
    },
  "esfilter":{"and":[
    <b>{"range":{
      "expires_on":{"gt":1262304000000}   # Jan 1st, 2010
    }},
    {"range":{"modified_ts":{"lte":1262304000000}}},</b>
    {"not":{"terms":{"bug_status":[
      "resolved",
      "verified",
      "closed"
    ]}}}
  ]},
  "edges":["product"]
}</pre>
</td>
</tr>
</table>

Group By
--------

ElasticSearch's has a limited form of GroupBy called "facets".  Facets are
strictly one dimensional, so grouping by more than one column will require
MVEL scripting or many facets.  Furthermore, facets are limited to using the
unique values of the data.

In this example, we are simply count the number of bug version records for each
block of 50K bug_ids:

<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
  "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[{"match_all":{}}]}
  }},
  "from":0,
  "size":0,
  "sort":[],
  "facets":{
    "0":{
      "terms":{"field":"bug_id","size":0},
      "facet_filter":{"and":[
        {"range":{"bug_id":{"gte":0,"lt":50000}}}
      ]}
    },
    ...snip 19 others ...
    "20":{
      "terms":{"field":"bug_id","size":0},
      "facet_filter":{"and":[
        {"range":{"bug_id":{"gte":1000000,"lt":1050000}}}
      ]}
    }
  }
}</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
  "from":"public_bugs",
  "select":{
    "name":"num",
    "value":"bug_id",
    "aggregate":"count"
  },
  "edges":[{
    "value":"bug_id",
    "domain":{
      "type":"numeric",
      "min":0,
      "max":1000000,
      "interval":50000,
      "isFacet":true
    }
  }]
}</pre><br>
Qb queries allow you to specify how to group data by using the <code>domain</code>
sub-clause.  The number of unique parts in the domain must be known at request
time.
</td>
</tr>
</table>


Private Bugs
------------

The public cluster does not contain Mozilla's confidential bugs.  Most of
these are internal network and infrastructure bugs, product security bugs, and
administrative "bugs".  Mozilla has an VPN-accessible private cluster with
those additional private bugs, but is handicapped by having no comments or
descriptions.  When querying aggregates you must be cognisant of this
difference.

If you have access to the private cluster you can call up the private bugs with
```{"not":{"missing":{"field":"bug_group"}}}``` - which means any bug that
belongs to a bug_group is a private bug.

This example pulls the current number of open private bugs by product.  If you
run this on the public cluster, you will get zeros.

<table>
<tr>
<td>
<b>ElasticSearch</b><br>
<pre>{
  "query":{"filtered":{
    "query":{"match_all":{}},
    "filter":{"and":[
      {"match_all":{}},
      {"and":[
        {"range":{"expires_on":{"gte":1389389493271}}},
        {"not":{"terms":{"bug_status":[
            "resolved","verified","closed"
        ]}}},
        {"not":{"missing":{"field":"bug_group"}}}
      ]}
    ]}
  }},
  "from":0,
  "size":0,
  "sort":[],
  "facets":{"default":{"terms":{
    "field":"product",
    "size":200000
  }}}
}</pre>
</td>
<td>
<b>Qb Query</b>
<pre>{
  "from":"private_bugs",
  "select":{
    "name":"num_bugs",
    "value":"bug_id",
    "aggregate":"count"
  },
  "esfilter":{"and":[
    {"range":{"expires_on":{"gte":1389389493271}}},
    {"not":{"terms":{"bug_status":[
      "resolved","verified","closed"
    ]}}},
    <b>{"not":{"missing":{"field":"bug_group"}}}</b>
  ]},
  "edges":["product"]
}</pre>
</td>
</tr>
</table>



More
----

[More sophisticated queries are next](MVEL_Tutorial.md)




ElasticSearch Features
-----------------------

  * [ElasticSearch Head](https://github.com/mobz/elasticsearch-head) - for general ES access
  * [QueryTool](people.mozilla.org/~klahnakoski/QueryTool.html) - better if you ```git clone``` to get the latest version
  * [Date Histogram](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets-date-histogram-facet.html) - Group a timestamp by year, quarter, month, week, day, hour, minute.
  * [Relations and Joins](http://blog.squirro.com/post/45191175546/elasticsearch-and-joining) - Setup parent/child relations and query both in single request.
  * [General Joins](https://github.com/elasticsearch/elasticsearch/issues/2674) - Cache a query result and then use it in subsequent queries.



