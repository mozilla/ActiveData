MVEL Tutorial (incomplete)
=============

About MVEL
----------

All of the queries in this section require writing scripts in a [scripting
language called MVEL](http://mvel.codehaus.org/). MVEL is powerful because it
is ES's default scripting language, it is run server-side, and can be used to
further analyse documents before the query results are returned.

MVEL is fine for short scripts, but becomes tricky as the code logic becomes
more complex, mainly because of poor documentation.  I chose MVEL over other
pluggable scripting languages, because I wanted ES installation to remain easy
for others.

**MVEL has been disabled on the public cluster** because it is has access to
all the references inside the Java virtual machine.  If you want to perform
the queries in this tutorial you must [setup your own cluster](https://github.com/klahnakoski/Bugzilla-ETL/blob/master/docs/Replication.md)
and replicate the public ES cluster.



Open Bugs, Over Time
--------------------

ElasticSearch has the [Date Histogram](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-facets-date-histogram-facet.html)
which can be used to group documents by their timestamps.  This does not work
for the Bugzilla data in ES; Bug version records are valid for a time range,
there can be multiple records for any given time interval, and there can
multiple time intervals covered by a single version document.  Because of this
many-many relation, we use one facet for each interval we are interested in.
In this case, we are interested in 26 weeks starting end of June and ending end of
December.


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
        {"term":{"cf_blocking_b2g":"koi+"}},
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
  "facets":{
    "0":{
      "terms":{"script_field":"1","size":200000},
      "facet_filter":{"and":[{"and":[
        {"range":{"modified_ts":{"lte":1372550400000}}},
        {"or":[
          {"missing":{"field":"expires_on"}},
          {"range":{"expires_on":{"gte":1372550400000}}}
        ]}
      ]}]}
    },
    <b>... snip 24 entries ...</b>
    "25":{
      "terms":{"script_field":"1","size":200000},
      "facet_filter":{"and":[{"and":[
        {"range":{"modified_ts":{"lte":1387670400000}}},
        {"or":[
          {"missing":{"field":"expires_on"}},
          {"range":{"expires_on":{"gte":1387670400000}}}
        ]}
      ]}]}
    }
  }
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
    {"term":{"cf_blocking_b2g":"koi+"}},
    {"not":{"terms":{"bug_status":[
      "resolved",
      "verified",
      "closed"
    ]}}}
  ]},
  "edges":[{
    "name":"date",
    "range":{"min":"modified_ts","max":"expires_on"},
    "allowNulls":false,
    "domain":{
      "type":"time",
      "min":1372550400000,
      "max":1388275200000,
      "interval":"week"
    }
  }]
}</pre><br>
<i>The edges clause defines how the data is grouped (aka partitioned)
before the aggregate is calculated.  The Qb result will contain data for evey
partition in the domain, even if it is empty.</i>
</td>
</tr>
</table>

Closing Bugs
------------

The bug version records have extra redundancy to support queries on change.
There are two main substructures you can use:

### changes ###

```changes``` is a nested property that contains the difference from the
previous version document.  Each is

Here is an example:

    changes: [{
        new_value: 1
        field_name: everconfirmed
      },{
        new_value: assigned
        field_name: bug_status
        old_value: unconfirmed
      },{
        new_value: m20
        field_name: target_milestone
        old_value: ---
      }
    ]


### previous_values ###

This structure holds any

    previous_values: {
        bug_status_change_to_ts: 956161485000
        bug_status_value: unconfirmed
        bug_status_change_away_ts: 956186704000
        bug_status_duration_days: 0
        everconfirmed_change_to_ts: 956161485000
        everconfirmed_change_away_ts: 956186704000
        everconfirmed_duration_days: 0
        target_milestone_change_to_ts: 956161485000
        target_milestone_value: ---
        target_milestone_change_away_ts: 956186704000
        target_milestone_duration_days: 0
    }





TODO: MAKE changes nested
{
    "from":"private_bugs.changes",
	"select":"private_bugs.bug_id",
	"where":{"and":[{"term":{"private_bugs.changes.field_name":"product"}}]}
}
full path of all variables is required
```where``` clause is used in MVEL

<pre>
{
    "query":{"filtered":{
		"query":{"match_all":{}},
		"filter":{"and":[{"match_all":{}}]}
	}},
	"from":0,
	"size":0,
	"sort":[],
	"facets":{"mvel":{"terms":{
		"script_field":"""

  var get = function(hash, key){
if (hash==null) null; else hash[key];
};
var Value2Pipe = function(value){
if (value==null){ “0” }else if (value is ArrayList || value is org.elasticsearch.common.mvel2.util.FastList){var out = ““;
foreach (v : value) out = (out==““) ? v : out + “|” + Value2Pipe(v);
'a'+Value2Pipe(out);
}else
if (value is Long || value is Integer || value is Double){ 'n'+value; }else
if (!(value is String)){ 's'+value.getClass().getName(); }else
“s”+value.replace(“\\”, “\\\\”).replace(“|”, “\\p”);};
var _1000 = function(private_bugs){
output=““;
if (private_bugs.?changes!=null){ for(private_bugs1 : private_bugs.?changes){
if (private_bugs1.?field_name==“product”){if (output!=““) output+=“|”;
output+=Value2Pipe(private_bugs.?bug_id)
;
}

}}
output;
};
_1000(_source)


    """,
		"size":200000
	}}}
}
</pre>




Opening Bugs

Closing Bugs


Reviews
