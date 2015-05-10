/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("aLibrary.js");
importScript("Dimension-Bugzilla.js");


var Hierarchy = {};


// CONVERT FROM AN ARRAY OF OBJECTS WITH A parent_field DEFINED TO A TREE OF
// THE SAME, BUT WITH child_field CONTAINING AN ARRAY OF CHILDREN
// ALL OBJECTS MUST HAVE id_field DEFINED
// RETURNS AN ARRAY OF ROOT NODES.
Hierarchy.fromList = function(args){
	ASSERT.hasAttributes(args, ["id_field", "parent_field", "child_field"]);

	var childList = {};
	var roots = [];

	args.from.forall(function(p, i){
		if (p[args.parent_field] != null) {
			var peers = childList[p[args.parent_field]];
			if (!peers) {
				peers = [];
				childList[p[args.parent_field]] = peers;
			}//endif
			peers.push(p);
		} else {
			roots.push(p);
		}//endif
	});

	//WE MAY HAVE CYCLES!! (REMOVE CYCLES UP TO 2 IN LENGTH)
	var deleteMe = [];
	Map.forall(childList, function(parent, children){
		children.forall(function(child){
			if (child.id == parent) {
				deleteMe.append([parent, child]);
				return;
			}//endif

			var grandchildren = childList[child.id];
			if (grandchildren) grandchildren.forall(function(grand){
				if (grand.id == parent) {
					deleteMe.append([parent, child]);
				}//endif
			});
		});
	});
	deleteMe.forall(function(pair){
		childList[pair[0]] = childList[pair[0]].filter({"not" : {"term" : {"id" : pair[1].id}}});
	});
	roots.appendArray(deleteMe.select("1"));


	var heir = function(children){
		children.forall(function(child, i){
			var grandchildren = childList[child[args.id_field]];
			if (grandchildren) {
				child[args.child_field] = grandchildren;
				heir(grandchildren);
			}//endif
		});
	};
	heir(roots);

	return roots;
};


//EXPECTING CERTAIN PARAMETERS:
// from - LIST OF NODES
// id_field - USED TO ID NODE
// fk_field - NAME OF THE CHILDREN ARRAY, CONTAINING IDs
//WILL UPDATE ALL BUGS IN from WITH A descendants_field
Hierarchy.addDescendants = function addDescendants(args){
	ASSERT.hasAttributes(args, ["from", "id_field", "fk_field", "descendants_field"]);

	var from = args.from;
	var id = args.id_field;
	var fk = args.fk_field;
	var descendants_field = args.descendants_field;
	var DEBUG = coalesce(args.DEBUG, false);
	var DEBUG_MIN = 1000000;

	//REVERSE POINTERS
	var allParents = new Relation();
	var allDescendants = new Relation();
	var all = from.select(id);
	from.forall(function(p){
		var children = p[fk];
		if (children) for (var i = children.length; i--;) {
			var c = children[i];
			if (c == "") continue;
			if (! all.contains(c)) continue;
			allParents.add(c, p[id]);
			allDescendants.add(p[id], c);
		}//for
	});

	//FIND DESCENDANTS


	var workQueue = new aQueue(Object.keys(allParents.map));

	while (workQueue.length() > 0) {      //KEEP WORKING WHILE THERE ARE CHANGES

		if (DEBUG) {
			if (DEBUG_MIN > workQueue.length() && workQueue.length() % Math.pow(10, Math.round(Math.log(workQueue.length()) / Math.log(10)) - 1) == 0) {
				Log.actionDone(a);
				a = Log.action("Work queue remaining: " + workQueue.length(), true);
				DEBUG_MIN = workQueue.length();
			}//endif
		}//endif
		var node = workQueue.pop();

		var desc = allDescendants.get(node);

		var parents = allParents.get(node);
		for (var i = parents.length; i--;) {
			var parent = parents[i];

			var original = allDescendants.getMap(parent);


			if (original === undefined) {
				for (var d = desc.length; d--;) {
					allDescendants.add(parent, desc[d]);
				}//for
				workQueue.add(parent);
			} else {
				for (var d = desc.length; d--;) {
					if (original[desc[d]]) continue;
					allDescendants.add(parent, desc[d]);
					workQueue.add(parent);
				}//for
			}//endif
		}//for
	}//while

	from.forall(function(p){
		p[descendants_field] = allDescendants.get(p[id]);
	});


};


// HEAVILY ALTERED FROM BELOW
// STILL NO GOOD BECAUSE CAN NOT HANDLE CYCLES

// Copyright 2012 Rob Righter (@robrighter)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//REQUIRES from BE A MAP FROM id_field TO OBJECT
//children_id_field IS THE FIELD THIS LIST OF IDs
Hierarchy.topologicalSort = function(args){
	Map.expecting(args, ["from", "id_field", "children_id_field"]);


	var graph = args.from;
	var id_field = args.id_field;
	var children_field = args.children_id_field;
//	var children_field="_EDGES";

	//ADD EDGES SO FOLLOWING ALGORITHM WORKS
//	Map.forall(graph, function(k, v){
//		v[children_field]=[];
//		v[children_id_field].forall(function(v, i){
//			v[children_field].push(graph[v]);
//		});
//	});


	var numberOfNodes = Object.keys(graph).length;
	var processed = [];
	var unprocessed = [];
	var queue = [];

	function processList(){
		while (processed.length < numberOfNodes) {
			for (var i = 0; i < unprocessed.length; i++) {
				var nodeid = unprocessed[i];
				if (graph[nodeid].indegrees === 0) {
					queue.push(nodeid);
					unprocessed.splice(i, 1); //Remove this node, its all done.
					i--;//decrement i since we just removed that index from the iterated list;
				}//endif
			}//for

			{//HACK
				//THIS IS AN UNPROVEN HACK TO SOLVE THE CYCLE PROBLEM
				//IF A PARENT OF unprocessed NODE HAS BEEN VISITED, THEN THE NODE WILL
				//HAVE __parent DEFINED, AND IN THEORY WE SHOULD BE ABLE CONTINUE
				//WORKING ON THOSE
				if (queue.length == 0 && unprocessed.length > 0) {
					var hasParent = unprocessed.map(function(v, i){
						if (graph[v].__parent !== undefined) return v;
					});
					if (hasParent.length == 0) Log.error("Isolated cycle found");
					queue.appendArray(hasParent);
				}//endif
			}//END OF HACK

			processStartingPoint(queue.shift());
		}//while
	}//method


	function processStartingPoint(nodeId){
		if (nodeId == undefined) {
			throw "You have a cycle!!";
		}
		graph[nodeId][children_field].forall(function(child){
			graph[child].indegrees--;
			graph[child].__parent = graph[nodeId];		//MARKUP FOR HACK
		});
		processed.push(graph[nodeId]);
	}


	function populateIndegreesAndUnprocessed(){
		Map.forall(graph, function(nodeId, node){
			unprocessed.push(nodeId);
			if (node.indegrees === undefined) node.indegrees = 0;
			if (node[children_field] === undefined) node[children_field] = [];

//			if (nodeId=="836963"){
//				Log.note("");
//			}//endif

			node[children_field].forall(function(e){
				if (graph[e] === undefined) {
					graph[e] = Map.newInstance(id_field, e);
				}//endif

//				if (nodeId==831910 && e==831532) return;	//REMOVE CYCLE (CAN'T HANDLE CYCLES)

				if (graph[e].indegrees === undefined) {
					graph[e].indegrees = 1
				} else {
					graph[e].indegrees++;
				}//endif
			});
		});
	}

	populateIndegreesAndUnprocessed();
	processList();

	if (processed.length != numberOfNodes) Log.error("broken");
	return processed;
};//method


function* allOpenDependencies(esfilter, dateRange, selects, allOpen){
	var data = yield (getRawDependencyData(esfilter, dateRange, selects));
	var output = yield (getDailyDependencies(data, esfilter, allOpen));
	yield (output);
}//method


//EXPECTING
// esfilter - TO GENERATE THE TOP-LEVEL BUG IDS
// dateRange - A time DOMAIN DEFINITION
// selects - A LIST OF COLUMNS ALSO REQUIRED BY THE CALLER
//RETURNS CUBE OF bug X date
function* getRawDependencyData(esfilter, dateRange, selects){

	var a = Log.action("Get dependencies", true);
	var topBugs = (yield(ESQuery.run(
		{
			"from" : "bugs",
			"select" : "bug_id",
			"esfilter" : {"and" : [
				esfilter
			]}
		}
	))).list;
	topBugs = [].union(topBugs);

	var possibleTree = (yield(ESQuery.run(
		{
			"from" : "bug_hierarchy",
			"select" : [
				"bug_id",
				"descendants"
			],
			"esfilter" : {"terms" : {"bug_id" : topBugs}}
		}
	))).list;
	possibleTree = possibleTree.select("descendants");
	possibleTree.append(topBugs);
	possibleTree = Array.union(possibleTree);
	Log.actionDone(a);

	var allSelects = selects.union(["bug_id", "dependson", "bug_status", "modified_ts", "expires_on"]);

	var a = Log.action("Pull details");
	var raw_data = yield (ESQuery.run(
		{
			"name" : "Open Bug Count",
			"from" : "bugs",
			"select" : allSelects.copy(),
			"esfilter" : {"and" : [
				{"terms" : {"bug_id" : possibleTree}},
				{"range" : {"modified_ts" : {"lt" : dateRange.max.getMilli()}}},
				{"range" : {"expires_on" : {"gte" : dateRange.min.getMilli()}}}
			]}
		}
	));
	Log.actionDone(a);

	//ORGANIZE INTO DATACUBE: (DAY x BUG_ID)
	var a = Log.action("Fill raw cube");
	var data = yield (Q(
		{
			"from" : raw_data,
			"select" : allSelects.subtract(["bug_id"]).map(
				function(v){
					return {"value" : v, "aggregate" : "minimum"};  //aggregate==min BECAUSE OF ES CORRUPTION
				}
			),
			"edges" : [
				{"name" : "date", "range" : {"min" : "modified_ts", "max" : "expires_on"}, "domain" : Map.copy(dateRange)},
				"bug_id"
			]
		}
	));
	Log.actionDone(a);

	//ADDING COLUMNS AS MARKUP
	data.columns.append({"name" : "counted"});
	data.columns.append({"name" : "churn"});
	data.columns.append({"name" : "date"});

	//ADD date AND bug_id TO ALL RECORDS
	var days = data.edges[0].domain.partitions;
	var bugs = data.edges[1].domain.partitions;
	for (var day = 0; day < data.cube.length; day++) {
		var bug = data.cube[day];
		var day_part = days[day];
		bug.forall(
			function(detail, i){
				detail.date = day_part.value;    //ASSIGN DATE TO AGGREGATE RECORD
				detail.churn = 0;
				detail.bug_id = bugs[i].value;   //ASSIGN BUG ID TO AGGREGATE RECORD
			}
		);
	}//for
	return data;
}

//EXPECTING
// data - DATA CUBE OF bugs X date
// topBugFilter - FILTER TO DETERMINE EACH DAY'S TOP-LEVEL BUG IDS (function, or esfilter)
// allOpen - true IF WE COUNT OPEN DEPENDENCIES OF CLOSED BUGS
// REWRITES THE .counted ATTRIBUTE TO BE "Open", "Closed", or "none"
// REWRITES THE .churn ATTRIBUTE TO BE +1 (to Open), -1 (from Open), 0 (no change)
function* getDailyDependencies(data, topBugFilter){
	if (typeof(topBugFilter) != "function") topBugFilter = convert.esFilter2function(topBugFilter);

	//FOR EACH DAY, FIND ALL DEPENDANT BUGS
	var yesterdayBugs = null; var yesterdayOpenDescendants = null; var yesterdayOpenBugs
	for (var day = 0; day < data.cube.length; day++) {
		var todayBugs = data.cube[day];
		var allTopBugs = todayBugs.filter(topBugFilter);

		Hierarchy.addDescendants({
			"from" : todayBugs,
			"id_field" : "bug_id",
			"fk_field" : "dependson",
			"descendants_field" : "dependencies"
		});	//yield (Thread.YIELD);

		var allDescendantsForToday = Array.union(allTopBugs.select("dependencies")).union(allTopBugs.select("bug_id")).map(function(v){
			return v - 0;
		});
		todayBugs.forall(function(detail, i){
			if (allDescendantsForToday.contains(detail.bug_id)) {
				detail.counted = "Closed"
			} else {
				detail.counted = "none";
			}//endif
		});

		var openTopBugs = [];
		var openBugs = todayBugs.map(function(detail){
			if (["new", "assigned", "unconfirmed", "reopened"].contains(detail.bug_status)) {
				if (topBugFilter(detail)) openTopBugs.append(detail);
				return detail;
			} else {
				return undefined;
			}//endif
		});

		Hierarchy.addDescendants({
			"from" : openBugs,
			"id_field" : "bug_id",
			"fk_field" : "dependson",
			"descendants_field" : "dependencies"
		});	//yield (Thread.YIELD);

		var openDescendantsForToday = Array.union(openTopBugs.select("dependencies")).union(openTopBugs.select("bug_id")).map(function(v){
			return v - 0;
		});
		var todayTotal = 0;  // FOR DEBUG
		var todayDiff=0;
		todayBugs.forall(function(detail, i){
			var yBug = {};
			if (yesterdayBugs) yBug = yesterdayBugs[i];

			if (openDescendantsForToday.contains(detail.bug_id) && ["new", "assigned", "unconfirmed", "reopened"].contains(detail.bug_status)) {
				detail.counted = "Open";
				todayTotal++;
				if (yBug.counted == "Open") {
					yBug.churn = 0;
				} else {
					yBug.churn = 1;
				}//endif
			} else {
				if (yBug.counted != "Open") {
					yBug.churn = 0;
				} else {
					yBug.churn = -1;
				}//endif
			}//endif
			todayDiff += yBug.churn;
		});

		yesterdayBugs = todayBugs;
		yesterdayOpenBugs = openBugs;
		yesterdayOpenDescendants = openDescendantsForToday;
	}//for

	yield (data);
}//function


