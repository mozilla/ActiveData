/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("Rest.js");

ElasticSearch={};

//ElasticSearch.pushURL="http://elasticsearch-private.bugs.scl3.mozilla.com:9200";
//ElasticSearch.pushURL="http://elasticsearch7.metrics.scl3.mozilla.com:9200";
ElasticSearch.pushURL="http://klahnakoski-es.corp.tor1.mozilla.com:9200";


ElasticSearch.search=function*(index, esquery){
	yield (ESQuery.loadColumns(index));
	var meta = ESQuery.INDEXES[index];
	if (meta.host===undefined) Log.error("must have host defined");
	var url = meta.host+meta.path+"/_search";

	var output=yield (Rest.post({
		url: url,
		data: convert.value2json(esquery),
		dataType: "json"
	}));

	yield (output);
};

ElasticSearch.setRefreshInterval=function*(destination, rate){
	var data=yield (Rest.put({
		"url": joinPath(destination.host, destination.path, "_settings"),
		"data":{"index":{"refresh_interval":"1s"}}
	}));
	Log.note("Refresh Interval to "+rate+": "+convert.value2json(data));
	yield (data);
};//method


//EXPECTING THE DATA ARRAY TO ALREADY HAVE ODD ENTRIES STARTING WITH { "create":{ "_id" : ID } }
ElasticSearch.bulkInsert=function*(destination, dataArray){
//	try{
		yield (Rest.post({
			"url":joinPath(destination.host, destination.path, "_bulk"),
			"data":dataArray.join("\n")+"\n",
			dataType: "text"
		}));
//	} catch(e){
//		Log.warning("problem with _bulk", e)
//	}//try
};

//ONLY BECAUSE I AM TOO LAZY TO ENHANCE THE ESQuery WITH MORE FACETS (A BATTERY OF FACETS PER SELECT COLUMN)
//RETURN ALL BUGS THAT MATCH FILTER ALONG WITH THE TIME RANGE THEY MATCH
//EXPECTING esfilter
ElasticSearch.getMinMax=function*(esfilter){

	//MUST CALL ES TWICE BECAUSE WE CAN ONLY HAVE ONE SELECT COLUMN IF WE HAVE EDGES
	var u1=null;
	var minThread=Thread.run(function*(){
		u1 = yield(ESQuery.run({
			"from":"bugs",
			"select":{"name":"min", "value":"modified_ts", "aggregate":"minimum"},
			"edges":[
				"bug_id"
			],
			"esfilter":esfilter
		}));

	});

	var u2 = yield(ESQuery.run({
		"from":"bugs",
		"select":{"name":"max", "value":"expires_on", "aggregate":"maximum"},
		"edges":[
			"bug_id"
		],
		"esfilter":esfilter
	}));

	yield (Thread.join(minThread));

	//CONVERT TO DATE VALUES (ALSO CONVERTS HIGH DATE VALUE TO null)
	u1.cube.forall(function(v, i){
		u1.cube[i]=Date.newInstance(v);
		if (u1.cube[i]==null) u1.cube[i]=undefined;
	});
	u2.cube.forall(function(v, i){
		u2.cube[i]=Date.newInstance(v);
		if (u2.cube[i]==null) u2.cube[i]=undefined;		//NULL MEANS UNKNOWN, WHEREAS undefined MEANS NOT DEFINED
	});

	var u = Qb.merge([
		{"from":u1, "edges":["bug_id"]},
		{"from":u2, "edges":["bug_id"]}
	]);
	yield u;
};//method





// RETURN min AND max FOR EACH BUG DURING WHICH IT WAS OPEN
// ALSO GIVE CURRENT VALUES OF selects
ElasticSearch.getOpenMinMax=function*(esfilter, timeDomain, selects){
	var details = yield(ESQuery.run({
		"from":"bugs",
		"select":selects.union(["bug_id", "modified_ts", "expires_on", "bug_status"]),
		"esfilter":{"and":[
			{"range":{"expires_on":{"gte":timeDomain.min.getMilli()}}},
			esfilter
		]}
	}));

	var allSelects = selects.map(function(s){
		return {"name":s, "value":'expires_on>Date.now().getMilli() ? '+s+' : null', "aggregate":"minimum"};  //aggregate===minimum due to es corruption
	}).appendArray([
		{"name":"min", "value":'["new", "assigned", "unconfirmed", "reopened"].contains(bug_status) ? modified_ts : null', "aggregate":"minimum"},
		{"name":"max", "value":'["new", "assigned", "unconfirmed", "reopened"].contains(bug_status) ? expires_on  : null', "aggregate":"maximum"}
	]);

	var summary = yield(Q({
		"from":details,
		"select": allSelects,
		"edges":[
			"bug_id"
		]
	}));

	yield summary;
};//method


////////////////////////////////////////////////////////////////////////////////
// THE REST OF THIS FILE IS DEPRECIATED
////////////////////////////////////////////////////////////////////////////////


ElasticSearch.makeBasicQuery=function(esfilter){
	return {
		"query":{
			"filtered":{
			  "query":{"match_all":{}},
			  "filter":{"and": [esfilter]}
			}
		},
		"from": 0,
		"size": 0,
		"sort": [],
		"facets":{}
	};
};

ElasticSearch.injectFilter=function(esquery, filter){
	if (esquery.query.filtered === undefined){
		var filtered = {};
		filtered.filtered = {};
		filtered.filtered.query = esquery.query;
		filtered.filtered.filter = esquery.filter;

		esquery.query = filtered;
		esquery.filter = undefined;
	}//endif

	var and = esquery.query.filtered.filter.and;
	and.push(filter);
};


