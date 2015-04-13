/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


if (Qb===undefined) var Qb = {};



importScript("../util/convert.js");
importScript("../util/aDate.js");
importScript("../util/aUtil.js");
importScript("../debug/aLog.js");
importScript("../collections/aMatrix.js");
importScript("MVEL.js");
importScript("Qb.aggregate.js");
importScript("Qb.column.js");
importScript("Qb.cube.js");
importScript("Qb.domain.js");
importScript("Qb.analytic.js");

importScript("../threads/thread.js");

var Q;   //=Q


function splitField(fieldname){
	try{
		return fieldname.replaceAll("\\.", "\b").split(".").map(function(v){return v.replaceAll("\b", ".");});
	}catch(e){
		Log.error("Can not split field", e);
	}//try
}//method

function joinField(path){
	return path.map(function(v){return v.replaceAll(".", "\\.");}).join(".");
}//method


(function(){

	var DEBUG=true;


Qb.reverse=function reverse(list){
	var output = list.copy();
	output.reverse();
	return output;
};



Qb.compile = function(query, sourceColumns, useMVEL){
//COMPILE COLUMN CALCULATION CODE
	var columns = [];
	var uniqueColumns={};

	if (sourceColumns===undefined){
		Log.error("It seems we can't get schema from cluster?")
	}//endif

	if (query.edges === undefined) query.edges=[];

	var edges = query.edges;
	for(var g = 0; g < edges.length; g++){
		var e=edges[g];
		if (e.partitions!==undefined){
			Log.error("An edge can not have partitions!  Did you give a domain to an edge?")
		}//endif

		if (typeof(e)=='string'){
			e={"value":e}; //NOW DEFINE AN EDGE BY ITS VALUE
			edges[g]=e;
		}//endif
		if (e.name===undefined) e.name=e.value;
		if (e.allowNulls === undefined) e.allowNulls = false;
		e.columnIndex=g;

		if (uniqueColumns[e.name]!==undefined) Log.error("All edges must have different names");
		columns[e.columnIndex] = e;
		uniqueColumns[e.name]=e;

		//EDGES DEFAULT TO A STRUCTURED TYPE, OTHER COLUMNS DEFAULT TO VALUE TYPE
		if (e.domain === undefined) e.domain={"type":"default"};
		Qb.column.compile(e, sourceColumns, undefined, useMVEL);
		e.outOfDomainCount = 0;
	}//for


	if (!(query.select instanceof Array)){
		if (typeof(query.select)=="string") query.select={"value":query.select};
	}//endif

	var select = Array.newInstance(query.select);
	for(var s = 0; s < select.length; s++){
		if (typeof(select[s])=="string") select[s]={"value":select[s]};
		if (select[s].name===undefined) select[s].name=splitField(select[s].value).last();
		if (uniqueColumns[select[s].name]!==undefined)
			Log.error("Column with name "+select[s].name+" appeared more than once");
		select[s].columnIndex=s+edges.length;
		columns[select[s].columnIndex] = select[s];
		uniqueColumns[select[s].name] = select[s];
		Qb.column.compile(select[s], sourceColumns, edges, useMVEL);
		Qb.aggregate.compile(select[s]);
	}//for

	query.columns=columns;
	return columns;
};


function getAggregate(result, query, select){
	//WE NEED THE select TO BE AN ARRAY
	var edges=query.edges;

	//FIND RESULT IN tree
	var agg = query.tree;
	var i = 0;
	for(; i < edges.length - 1; i++){
		var part=result[i];
		var v = edges[i].domain.getKey(part);
		if (agg[v] === undefined) agg[v] = {};
		agg = agg[v];
	}//for



	part=result[i];
	v = edges[i].domain.getKey(part);
	if (agg[v] === undefined){
		agg[v]=[];
		//ADD SELECT DEFAULTS
		for(var s = 0; s < select.length; s++){
			agg[v][s] = select[s].defaultValue();
		}//for
	}//endif

	return agg[v];
}//method


function calcAgg(row, result, query, select){
	var agg = getAggregate(result, query, select);
	for(var s = 0; s < select.length; s++){
		//ADD TO THE AGGREGATE VALUE
		agg[s] = select[s].aggFunction(row, result, agg[s]);
	}//endif
}//method


function* calc2Tree(query){
	if (query.edges.length == 0)
		Log.error("Tree processing requires an edge");
	if (query.where=="bug!=null")
		Log.note("");

	var sourceColumns  = yield (Qb.getColumnsFromQuery(query));
	if (sourceColumns===undefined){
		Log.error("Can not get column definitions from query:\n"+convert.value2json(query).indent(1))
	}//endif
	var from = query.from.list;

	var edges = query.edges;
	query.columns = Qb.compile(query, sourceColumns);
	var select = Array.newInstance(query.select);
	var _where = Qb.where.compile(coalesce(query.where, query.esfilter), sourceColumns, edges);
	var numWhereFalse=0;


	var tree = {};
	query.tree = tree;
	FROM: for(var i = 0; i < from.length; i++){
		yield (Thread.yield());

		var row = from[i];
		//CALCULATE THE GROUP COLUMNS TO PLACE RESULT
		var results = [[]];
		for(var f = 0; f < edges.length; f++){
			var edge = edges[f];


			if (edge.test || edge.range){
				//MULTIPLE MATCHES EXIST
				var matches= edge.domain.getMatchingParts(row);

				if (matches.length == 0){
					edge.outOfDomainCount++;
					if (edge.allowNulls){
						for(t = results.length; t--;){
							results[t][f] = edge.domain.NULL;
						}//for
					} else{
						continue FROM;
					}//endif
				} else{
					//WE MULTIPLY THE NUMBER OF MATCHES TO THE CURRENT NUMBER OF RESULTS (SQUARING AND CUBING THE RESULT-SET)
					for(t = results.length; t--;){
						result = results[t];
						result[f] = matches[0];
						for(var p = 1; p < matches.length; p++){
							result = result.copy();
							results.push(result);
							result[f] = matches[p];
						}//for
					}//for
				}//endif
			} else{
				var v = edge.calc(row, null);

				//STANDARD 1-1 MATCH VALUE TO DOMAIN
				var p = edge.domain.getPartByKey(v);
				if (p === undefined){
					Log.error("getPartByKey() must return a partition, or null");
				}//endif
				if (p == edge.domain.NULL){
					edge.outOfDomainCount++;
					if (edge.allowNulls){
						for(t = results.length; t--;){
							results[t][f] = edge.domain.NULL;
						}//for
					} else{
						continue FROM;
					}//endif
				} else{
					for(t = results.length; t--;) results[t][f] = p;
				}//endif
			}//endif
		}//for

		for(var r = results.length; r--;){
			var pass = _where(row, results[r]);
			if (pass){
				calcAgg(row, results[r], query, select);
			}else{
				numWhereFalse++;
			}//for
		}//for

	}//for

	for(var g = 0; g < edges.length; g++){
		if (edges[g].outOfDomainCount > 0)
			Log.warning(edges[g].name + " has " + edges[g].outOfDomainCount + " records outside domain " + edges[g].domain.name);
	}//for
	if (DEBUG) Log.note("Where clause rejected "+numWhereFalse+" rows");


	yield query;
}




Qb.listAlert=false;

Qb.calc2List = function*(query){
	if (!Qb.listAlert){
//		Log.alert("Please do not use Qb.calc2List()");
		Qb.listAlert=true;
	}//endif


	if (query.edges===undefined) query.edges=[];
	var select = Array.newInstance(query.select);

	//NO EDGES IMPLIES NO AGGREGATION AND NO GROUPING:  SIMPLE SET OPERATION
	if (query.edges.length==0){
		if (select.length==0){
			yield (noOP(query));
			yield (query);
		}else if (select[0].aggregate===undefined || select[0].aggregate=="none"){
			yield (setOP(query));
			yield (query);
		}else{
			yield (aggOP(query));
			yield (query);
		}//endif
	}//endif

	if (query.edges.length == 0)
		Log.error("Tree processing requires an edge");

	yield (calc2Tree(query));

	var edges=query.edges;

	var output = [];
	Tree2List(output, query.tree, select, edges, {}, 0);
	yield (Thread.yield());

	//ORDER THE OUTPUT
	query.sort = Array.newInstance(query.sort);
	output = Qb.sort(output, query.sort, query.columns);

	//COLLAPSE OBJECTS TO SINGLE VALUE
	for(var ci=0;ci<query.columns.length;ci++){
		var col=query.columns[ci];
		if (col.domain===undefined){
			Log.error("expecting all columns to have a domain");
		}//endif
		var d = col.domain;
		if (d.end === undefined) continue;

		//d.end() MAY REDEFINE ITSELF (COMPILE GIVEN CONTEXT)
		for(var i = 0; i < output.length; i++){
			var o = output[i];
			o[col.name] = d.end(o[col.name]);
		}//for
	}//for

	query.list = output;

	Qb.analytic.run(query);

	Map.copy(Qb.query.prototype, query);

	yield (query);
};//method



function* calc2Cube(query){
	if (query.edges===undefined) query.edges=[];

	if (query.edges.length==0){
		var isAgg=false;
		Array.newInstance(query.select).forall(function(s){
			if (s.aggregate!==undefined && s.aggregate!="none"){
				isAgg=true;
			}//endif
		});
		if (isAgg){
			var oA=yield (aggOP(query));
			yield (oA);
		}else{
			var oS=yield (setOP(query));
			yield (oS);
		}//endif
	}//endif

	yield (calc2Tree(query));

	//ASSIGN dataIndex TO ALL PARTITIONS
	var edges = query.edges;
	edges.forall(function(edge){
		edge.domain = Map.copy(edge.domain);
		if (edge.domain.type=="default"){
			edge.domain.partitions.sort(edge.domain.compare);
		}//endif
		var p = 0;
		for(; p < (edge.domain.partitions).length; p++){
			edge.domain.partitions[p].dataIndex = p;
		}//for
		edge.domain.NULL.dataIndex = p;
	});

	//MAKE THE EMPTY DATA GRID
	query.cube = Qb.cube.newInstance(edges, 0, query.select);

	Tree2Cube(query, query.cube, query.tree, 0);

	Qb.analytic.run(query);


	Map.copy(Qb.query.prototype, query);

	yield (query);
}//method



//CONVERT LIST TO Qb
Qb.List2Cube=function(query){

	if (query.list!==undefined) Log.error("Can only convert list to a cube at this time");

	//ASSIGN dataIndex TO ALL PARTITIONS
	var edges = query.edges;
	edges.forall(function(edge){
		edge.domain = Map.copy(edge.domain);
		for(var p = 0; p < edge.domain.partitions.length; p++){
			edge.domain.partitions[p].dataIndex = p;
		}//for
		edge.domain.NULL.dataIndex = p;
	});//for

	//MAKE THE EMPTY DATA GRID
	query.cube = Qb.cube.newInstance(edges, 0, query.select);


	for(var i=query.list.length;i--;){
		var cube=query.cube;
		var e=0;
		for(;e<query.edges.length-1;e++){
			cube=cube[edges[e].dataIndex];
		}//for
		if (query.select instanceof Array){
			cube[edges[e].dataIndex]=query.list[i];
		}else{
			cube[edges[e].dataIndex]=query.list[i][query.select.name];
		}//endif
	}//for

	return query;
};//method


////////////////////////////////////////////////////////////////////////////////
//  REDUCE ALL DATA TO ZERO DIMENSIONS
////////////////////////////////////////////////////////////////////////////////
function* aggOP(query){
	var select = Array.newInstance(query.select);

	var sourceColumns = yield(Qb.getColumnsFromQuery(query));
	var from=query.from.list;
	var columns = Qb.compile(query, sourceColumns);
	var where = Qb.where.compile(query.where, sourceColumns, []);

	var result={};
	//ADD SELECT DEFAULTS
	for(var s = 0; s < select.length; s++){
		result[select[s].name] = select[s].defaultValue();
	}//for

	for(var i = 0; i < from.length; i++){
		var row = from[i];
		if (where(row, null)){
			for(var s = 0; s < select.length; s++){
				var ss = select[s];
				var v = ss.calc(row, result);
				result[ss.name] = ss.add(result[ss.name], v);
			}//for
		}//endif
	}//for

	//TURN AGGREGATE OBJECTS TO SINGLE NUMBER
	for(var c=0;c<columns.length;c++){
		var s=columns[c];
		if (s.domain===undefined){
			Log.error("expectin all columns to have a domain");
		}//endif
		var r = columns[c].domain.end;
		if (r === undefined) continue;

		result[s.name] = r(result[s.name]);
	}//for

	query.list = [result];
	query.cube = result;
	yield (query);
}



////////////////////////////////////////////////////////////////////////////////
//  DO NOTHING TO TRANSFORM LIST OF OBJECTS
////////////////////////////////////////////////////////////////////////////////
function* noOP(query){
	var sourceColumns = yield(Qb.getColumnsFromQuery(query));
	var from = query.from.list;


	var output;
	if (query.where===undefined){
		output=from;
	}else{
		output = [];
		var where = Qb.where.compile(query.where, sourceColumns, []);

		var output = [];
		for(t = from.length;t--;){
			if (where(from[t], null)){
				output.push(from[t]);
			}//endif
		}//for
	}//endif
	query.list = output;

	query.columns=sourceColumns.copy();
	Qb.analytic.run(query);

	//ORDER THE OUTPUT
	if (query.sort === undefined) query.sort = [];

	query.list = Qb.sort(query.list, query.sort, query.columns);

	yield (query);

}//method




////////////////////////////////////////////////////////////////////////////////
//  SIMPLE TRANSFORMATION ON A LIST OF OBJECTS
////////////////////////////////////////////////////////////////////////////////
function* setOP(query){
	var sourceColumns = yield (Qb.getColumnsFromQuery(query));
	var from=query.from.list;

	var select = Array.newInstance(query.select);
	var columns = select;

	select.forall(function(s, i){
		if (typeof(s)=='string') select[i]={"value":s};
		Qb.column.compile(select[i], sourceColumns, undefined);
	});
	var where = Qb.where.compile(query.where, sourceColumns, []);

	var output = [];
	for(t = 0; t < from.length; t++){
		var result = {};
		for(var s = 0; s < select.length; s++){
			var ss = select[s];
			result[ss.name] = ss.calc(from[t], null);
		}//for
		if (where(from[t], result)){
			output.push(result);
		}//endif
	}//for


	//ORDER THE OUTPUT
	if (query.sort === undefined) query.sort = [];
	output = Qb.sort(output, query.sort, columns);

	query.columns=columns;

	if (query.select instanceof Array || query.analytic){
		query.list = output;
		Qb.analytic.run(query);
	}else{
		//REDUCE TO ARRAY
		query.list=output.map(function(v, i){return v[select[0].name];});
	}//endif



	yield (query);

}//method


////////////////////////////////////////////////////////////////////////////////
// TABLES ARE LIKE LISTS, ONLY ATTRIBUTES ARE INDEXED BY COLUMN NUMBER
////////////////////////////////////////////////////////////////////////////////
Qb.toTable=function(query){

	if (query.cube===undefined) Log.error("Can only turn a cube into a table at this time");
	if (query.edges.length!=2) Log.error("can only handle 2D cubes right now.");

	var columns=[];
	var parts=[];
	var f="<CODE>";
	var param=[];
	var coord="";
	for(var w=0;w<query.edges.length;w++){
		f=f.replace("<CODE>","for(var p"+w+"=0;p"+w+"<parts["+w+"].length;p"+w+"++){\n<CODE>}\n");
		param.push("parts["+w+"][p"+w+"]");
		coord+="[p"+w+"]";

		columns[w]=query.edges[w];
		var d=query.edges[w].domain;
		if (d.end===undefined) d.end=function(part){return part;};
		parts[w]=[];
		d.partitions.forall(function(v,i){parts[w][i]=d.end(v);});
		if (query.edges[w].allowNulls) parts[w].push(d.end(d.NULL));
	}//for

	Array.newInstance(query.select).forall(function(s, i){
		columns.push(s);
		param.push("query.cube"+coord+((query.select instanceof Array) ? "["+i+"]" : ""));
	});

	var output=[];
	f=f.replace("<CODE>", "var row=["+param.join(", ")+"];\noutput.push(row);\n");
	eval(f);

	return {"columns":columns, "rows":output};

};//method


Qb.ActiveDataCube2List=function(query, options){
	//ActiveData CUBE IS A MAP OF CUBES   {"a": [[]], "b":[[]]}
	//MoDevLib CUBE IS A CUBE OF MAPS   [[{"a":v, "b":w}]]  which I now know as wrong

	//PRECOMPUTE THE EDGES
	var edges = Array.newInstance(query.edges);
	var domains = edges.select("domain");
	var parts=domains.map(function(d, i){
		if (d.type=="rownum"){
			return Array.newRange(d.min, d.max);
		}else {
			return d.partitions;
		}//endif
	});
	var edge_names=edges.select("name");

	endFunction = edges.map(function(e){
		if (e.domain.key===undefined){
			return function(part){
				return part;
			};
		}else if (MVEL.isKeyword(e.domain.key)){
			return function(part){
				if (part===undefined || part==null){
					return null;
				}else{
					return part[e.domain.key];
				};
			};
		}else{
			Log.error("Can not support domains without keys at this time");
		}//endif
	});

	if (edge_names.length==0){
		if (query.select instanceof Array){
			return [query.cube];
		}else{
			return [query.cube];
		}//endif
	}//endif


	var select = Array.newInstance(query.select);
	if (select.length == 0){
		//MAYBE IT IS A ZERO-CUBE?
		for(var i=0;i<edges.length;i++){
			e = edges[i];
			if (Array.newInstance(e.domain.partitions).length==0){
				return [];
			}//endif
		}//for
		Log.error("Do not know how to listify cube with no select");
	}//endif
	var select_names = select.select("name");

	//DO NOT SHOW THE FIELDS WHICH ARE A PREFIX OF ANOTHER
	select_names = select_names.map(function(v){
		var is_prefix=v;
		select_names.forall(function(u){
			if (u.startsWith(v+".")) is_prefix=undefined;
		});
		return is_prefix;
	});

	var m = new Matrix({"data": coalesce(query.cube[select_names[0]], [])});

	var output = [];
	m.forall(function(v, c){
		var o = {};
		for (var e = 0; e < c.length; e++) {
			if (edges[e].allowNulls && parts[e].length == c[e]) {
				o[edge_names[e]] = endFunction[e](domains[e].NULL);
			} else {
				o[edge_names[e]] = endFunction[e](parts[e][c[e]]);
			}//endif
		}//for
		for (var s = 0; s < select_names.length; s++) {
			var val = query.cube[select_names[s]];
			for (var i = 0; i < c.length; i++) val = val[c[i]];
			o[select_names[s]] = val;
		}//for
		output.append(o);
	});
	return output;
};

Qb.Cube2List=function(query, options){
	if (query.meta) { //ActiveData INDICATOR
		return Qb.ActiveDataCube2List(query, options);
	}//endif

	//WILL end() ALL PARTS UNLESS options.useStruct==true OR options.useLabels==true
	options=coalesce(options, {});
	options.useStruct=coalesce(options.useStruct, false);
	options.useLabels=coalesce(options.useLabels, false);

	//PRECOMPUTE THE EDGES
	var edges = Array.newInstance(query.edges);
	var domains = edges.select("domain");
	var endFunction=domains.select("end");
	if (options.useStruct){
		endFunction=edges.map(function(e){ return function(v){return v;};});
	}else if (options.useLabels){
		endFunction=domains.select("label");
	}//endif
	var parts=domains.select("partitions");
	var edge_names=edges.select("name");

	if (edge_names.length==0){
		if (query.select instanceof Array){
			return [query.cube];
		}else{
			return [Map.newInstance(query.select.name, query.cube)];
		}//endif
	}//endif

	var m = new Matrix({"shape":[], "data":query.cube});

	var output = [];
	if (query.select instanceof Array){
		m.forall(function(v, c){
			var o = Map.copy(v);
			for(var e=0;e<c.length;e++){
				if (edges[e].allowNulls && parts[e].length==c[e]){
					o[edge_names[e]]=endFunction[e](domains[e].NULL);
				}else{
					o[edge_names[e]]=endFunction[e](parts[e][c[e]]);
				}//endif
			}//for
			output.append(o);
		});
	}else{
		m.forall(function(v, c){
			var o = Map.newInstance(query.select.name, v);
			for(var e=0;e<c.length;e++){
				if (edges[e].allowNulls && parts[e].length==c[e]){
					o[edge_names[e]]=endFunction[e](domains[e].NULL);
				}else{
					o[edge_names[e]]=endFunction[e](parts[e][c[e]]);
				}//endif
			}//for
			output.append(o);
		});
	}//endif
	return output;
};//method



////////////////////////////////////////////////////////////////////////////////
// ASSUME THE FIRST DIMESION IS THE COHORT, AND NORMALIZE (DIVIDE BY SUM(ABS(Xi))
////////////////////////////////////////////////////////////////////////////////
Qb.normalizeByCohort=function(query, multiple){
	if (multiple===undefined) multiple=1.0;
	if (query.cube===undefined) Log.error("Can only normalize a cube into a table at this time");

//	SELECT
//		count/sum(count over Cohort) AS nCount
//	FROM
//		query.cube

	for(var c=0;c<query.cube.length;c++){
		var total=0;
		for(var e=0;e<query.cube[c].length;e++) total+=aMath.abs(query.cube[c][e]);
		if (total!=0){
			for(e=0;e<query.cube[c].length;e++) query.cube[c][e]*=(multiple/total);
		}//endif
	}//for
};//method

////////////////////////////////////////////////////////////////////////////////
// ASSUME THE SECOND DIMESION IS THE XAXIS, AND NORMALIZE (DIVIDE BY SUM(ABS(Ci))
////////////////////////////////////////////////////////////////////////////////
Qb.normalizeByX=function(query, multiple){
	if (multiple===undefined) multiple=1;
	if (query.cube===undefined) Log.error("Can only normalize a cube into a table at this time");

//	SELECT
//		count/sum(count over Cohort) AS nCount
//	FROM
//		query.cube

	for(var e=0;e<query.cube[0].length;e++){
		var total=0;
		for(var c=0;c<query.cube.length;c++){
			if (query.cube[c][e]===undefined) query.cube[c][e]=0;
			total+=aMath.abs(query.cube[c][e]);
		}//for
		if (total!=0){
			for(var c=0;c<query.cube.length;c++) query.cube[c][e]*=(multiple/total);
		}//endif
	}//for
};//method


Qb.normalize=function(query, edgeIndex, multiple){
	if (multiple===undefined) multiple=1;
	if (query.cube===undefined) Log.error("Can only normalize a cube into a table at this time");

	var totals=[];
	var m=new Matrix({"data":query.cube});

	m.forall(edgeIndex, function(v, i){
		totals[i] = coalesce(totals[i], 0) + aMath.abs(v);
	});
	m.map(query.edges.length, function (v, i) {
		if (totals[i]!=0){
			return v/totals[i];
		}else{
			return v;
		}//endif
	});
};


//selectValue - THE FIELD TO USE TO CHECK FOR ZEROS (REQUIRED IF RECORDS ARE OBJECTS INSTEAD OF VALUES)
Qb.removeZeroParts = function(query, edgeIndex, selectValue){
	if (query.cube === undefined) Log.error("Can only normalize a cube into a table at this time");
	if (selectValue === undefined) Log.error("method now requires third parameter");

	var domain = query.edges[edgeIndex].domain;
	var zeros = domain.partitions.map(function(){
		return true;
	});

	//CHECK FOR ZEROS
	var m = new Matrix({"data" : query.cube});
	if (query.select instanceof Array) {
		m.forall(function(v, c){
			if (v[selectValue] !== undefined && v[selectValue] != null && v[selectValue] != 0) zeros[c[edgeIndex]] = false;
		});
	} else {
		m.forall(function(v, i){
			if (v !== undefined && v != null && v != 0) zeros[edgeIndex] = false;
		});
	}//endif

	//REMOVE ZERO PARTS FROM EDGE
	var j = 0;
	domain.partitions = domain.partitions.map(function (part, i) {
		if (zeros[i]) return undefined;
		var output = Map.copy(part);
		output.dataIndex = j;
		j++;
		return output;
	});

	//REMOVE ZERO PARTS FROM CUBE
	query.cube = m.filter(edgeIndex, function (v, i) {
		if (!zeros[i]) return v;
		return undefined;
	});

};


// CONVERT THE tree STRUCTURE TO A FLAT LIST FOR output
function Tree2List(output, tree, select, edges, coordinates, depth){
	if (depth == edges.length){
		//FRESH OBJECT
		var obj={};
		Map.copy(coordinates, obj);
		for(var s=0;s<select.length;s++){
			obj[select[s].name]=tree[s];
		}//for
		output.push(obj);
	} else{
		var keys = Object.keys(tree);
		for(var k = 0; k < keys.length; k++){
			coordinates[edges[depth].name]=edges[depth].domain.getPartByKey(keys[k]);
			Tree2List(output, tree[keys[k]], select, edges, coordinates, depth + 1)
		}//for
	}//endif
//	yield (null);
}//method




// CONVERT THE tree STRUCTURE TO A cube
function Tree2Cube(query, cube, tree, depth){
	var edge=query.edges[depth];
	var domain=edge.domain;

	if (depth < query.edges.length-1){
		var keys=Object.keys(tree);
		for(var k=keys.length;k--;){
			var p=domain.getPartByKey(keys[k]).dataIndex;
			if (cube[p]===undefined){
				p=domain.getPartByKey(keys[k]).dataIndex;
			}//endif
			Tree2Cube(query, cube[p], tree[keys[k]], depth+1);
		}//for
		return;
	}//endif

	if (query.select instanceof Array){
		var keys=Object.keys(tree);
		for(var k=keys.length;k--;){
			var p=domain.getPartByKey(keys[k]).dataIndex;
			//I AM CONFUSED: ARE Qb ELEMENTS ARRAYS OR OBJECTS?
//			var tuple=[];
//			for(var s = 0; s < query.select.length; s++){
//				tuple[s] = tree[keys[k]][s];
//			}//for
			var tuple={};
			for(var s = 0; s < query.select.length; s++){
				tuple[query.select[s].name] = query.select[s].domain.end(tree[keys[k]][s]);
			}//for
			cube[p]=tuple;
		}//for
	} else{

		var keys=Object.keys(tree);
		for(var k=keys.length;k--;){
			try{
				var p=domain.getPartByKey(keys[k]).dataIndex;
				cube[p]=query.select.domain.end(tree[keys[k]][0]);
			}catch(e){
				Log.error(domain.getPartByKey(keys[k]).dataIndex, e)
			}//try
		}//for
	}//endif

}//method




////ADD THE MISSING DOMAIN VALUES
//Qb.nullToList=function(output, edges, depth){
//	if ()
//
//
//};//method

//RETURN THE COLUMNS FROM THE GIVEN QUERY
//ALSO NORMALIZE THE ARRAY OF OBJECTS TO BE AT query.from.list
Qb.getColumnsFromQuery=function*(query){
	//FROM CLAUSE MAY BE A SUB QUERY

	try{
		var sourceColumns;
		if (query.from instanceof Array){
			sourceColumns = Qb.getColumnsFromList(query.from);
			query.from.list = query.from;	//NORMALIZE SO query.from.list ALWAYS POINTS TO AN OBJECT
		} else if (query.from.list){
			sourceColumns = query.from.columns;
		} else if (query.from.cube){
			query.from.list = Qb.Cube2List(query.from);
			sourceColumns = query.from.columns;
		}else if (query.from.from!=undefined){
			query.from=yield (Qb.calc2List(query.from));
			sourceColumns=yield (Qb.getColumnsFromQuery(query));
		}else{
			Log.error("Do not know how to handle this");
		}//endif
		yield (sourceColumns);
	}catch(e){
		Log.error("not expected", e);
	}//try

};//method


// PULL COLUMN DEFINITIONS FROM LIST OF OBJECTS
Qb.getColumnsFromList = function(data){
	if (data.length==0 || typeof(data[0])=="string")
		return [];

	var output = [];
	for(var i = 0; i < data.length; i++){
		//WHAT ABOUT LISTS OF VALUES, WHAT IS THE COLUMN "NAME"
//		if (typeof data[i] != "object") return ["value"];
		var keys = Object.keys(data[i]);
		kk: for(var k = 0; k < keys.length; k++){
			for(var c = 0; c < output.length; c++){
				if (output[c].name == keys[k]) continue kk;
			}//for
			var column={"name":keys[k], "domain":Qb.domain.value};
			output.push(column);
		}//for
	}//for
	return output;
};//method




//
// EXPECTING AN ARRAY OF CUBES, AND THE NAME OF THE EDGES TO MERGE
// THERE IS NO LOGICAL DIFFERENCE BETWEEN A SET OF CUBES, WITH IDENTICAL EDGES, EACH CELL A VALUE AND
// A SINGLE Qb WITH EACH CELL BEING AN OBJECT: EACH ATTRIBUTE VALUE CORRESPONDING TO A Qb IN THE SET
//	var chart=Qb.merge([
//		{"from":requested, "edges":["time"]},
//		{"from":reviewed, "edges":["time"]},
//		{"from":open, "edges":["time"]}
//	]);
Qb.merge=function(query){
	//MAP THE EDGE NAMES TO ACTUAL EDGES IN THE from QUERY
	query.forall(function(item){
		if (item.edges.length!=item.from.edges.length) Log.error("do not know how to join just some of the edges");

		item.edges.forall(function(pe, i, edges){
			item.from.edges.forall(function(pfe, j){
				if (pfe.name==pe)
					edges[i]=pfe;
			});//for
			if (typeof(edges[i])=="string")
				Log.error(edges[i]+" can not be found");
		});
	});

	var commonEdges=query[0].edges;

	var output={};
	output.name=query.name;
	output.from=query;
	output.edges=[];
	output.edges.appendArray(commonEdges);
	output.select=[];
	output.columns=[];
	output.columns.appendArray(commonEdges);

	output.cube=Qb.cube.newInstance(output.edges, 0, []);
	Map.copy(Qb.query.prototype, output);

	query.forall(function(item, index){
		//COPY SELECT DEFINITIONS
		output.select.appendArray(Array.newInstance(item.from.select));
		output.columns.appendArray(Array.newInstance(item.from.select));


		//VERIFY DOMAINS ARE IDENTICAL, AND IN SAME ORDER
		if (item.edges.length!=commonEdges.length) Log.error("Expecting all partitions to have same number of (common) edges declared");
		item.edges.forall(function(edge, i){
			if (typeof(edge)=="string") Log.error("can not find edge named '"+edge+"'");
			if (!Qb.domain.equals(commonEdges[i].domain, edge.domain))
				Log.error("Edges domains ("+item.from.name+", edge="+edge.name+") and ("+query[0].from.name+", edge="+commonEdges[i].name+") are different");
		});


		//CONVERT TO Qb FOR COPYING
		if (item.from.cube!==undefined){
			//DO NOTHING
		}else if (item.from.list!==undefined){
			item.cube=Qb.List2Cube(item.from).cube;
		}else{
			Log.error("do not know how to handle");
		}//endif


		//COPY ATTRIBUTES TO NEW JOINED
		var parts=[];
		var num=[];

		var depth=output.edges.length;
		for(var d=0;d<depth;d++){
			domain = item.edges[d].domain;
			parts[d] = output.edges[d].domain.partitions.map(function(newpart, i){
				var oldpart = domain.getPartByKey(domain.getKey(newpart));  //OLD PARTS IN NEW ORDER
				if (oldpart.dataIndex!=newpart.dataIndex){
					Log.error("partitions have different orders, check this code works");
				}//endif
				return oldpart;
			});
			num[d]=parts[d].length;
		}//for

		var copy=function(from, to , d){
			for(var i=num[d];i--;){
				if (d<depth-1){
					copy(from[parts[d][i].dataIndex], to[i], d+1);
				}else{
					if (item.from.select instanceof Array){
						Map.copy(from[parts[d][i].dataIndex], to[i])
					}else{
						to[i][item.from.select.name]=from[parts[d][i].dataIndex]
					}//endif
				}//endif
			}//for
			if (output.edges[d].allowNulls){
				copy(from[num[d]], to[num[d]], d+1);
			}//endif
		};
		copy(item.from.cube, output.cube, 0);
	});

//	output.select=query.partitions[0].from.select;	//I AM TIRED, JUST MAKE A BUNCH OF ASSUMPTIONS

	return output;
};//method





////////////////////////////////////////////////////////////////////////////////
// ORDERING
////////////////////////////////////////////////////////////////////////////////
//TAKE data LIST OF OBJECTS
//sortOrder IS THE sort CLAUSE
//columns ARE OPTIONAL, TO MAP SORT value TO column NAME
Qb.sort = function(data, sortOrder, columns){
	if (sortOrder instanceof Function){
		try{
			data = data.copy();
			data.sort(sortOrder);
			return data;
		}catch(e){
			Log.error("bad sort function", e)
		}//try
	}//endif



	sortOrder = Array.newInstance(sortOrder);
	if (sortOrder.length==0) return data;
	var totalSort = Qb.sort.compile(sortOrder, columns, true);
	try{
		data = data.copy();
		data.sort(totalSort);
		return data;
	}catch(e){
		Log.error("bad sort function", e)
	}//try
};//method


Qb.sort.compile=function(sortOrder, columns, useNames){
	var orderedColumns;
	if (columns===undefined){
		orderedColumns = sortOrder.map(function(v){
			if (v.value!==undefined && v.sort!==undefined){
				return {"name": v.value, "sortOrder":coalesce(v.sort, 1), "domain":Qb.domain.value};
			}else{
				return {"name":v, "sortOrder":1, "domain":Qb.domain.value};
			}//endif
		});
	}else{
		orderedColumns = sortOrder.map(function(v){
			if (v.value!==undefined){
				for(var i=columns.length;i--;){
					if (columns[i].name==v.value && !(columns[i].sortOrder==0)) return {"name": v.value, "sortOrder":coalesce(v.sort, 1), "domain":Qb.domain.value};
				}//for
			}else{
				for(var i=columns.length;i--;){
					if (columns[i].name==v && !(columns[i].sortOrder==0)) return {"name":v, "sortOrder":1, "domain":Qb.domain.value};
				}//for
			}
			Log.error("Sorting can not find column named '"+v+"'");
		});
	}//endif

	var f="totalSort = function(a, b){\nvar diff;\n";
	for(var o = 0; o < orderedColumns.length; o++){
		var col = orderedColumns[o];
		if (orderedColumns[o].domain === undefined){
			Log.warning("what?");
		}//endif


		var index;
		if (!useNames){
			index = col.columnIndex;
		}else if (MVEL.isKeyword(col.name)){
			index=splitField(col.name).map(convert.String2Quote).join("][");
		}else if (columns.select("name").contains(col.name)){
			index=convert.String2Quote(col.name);
		}else{
			Log.error("Can not handle");
		}//endif

		f+="diff = orderedColumns["+o+"].domain.compare(a["+index+"], b["+index+"]);\n";
		if (o==orderedColumns.length-1){
			if (col.sortOrder===undefined || col.sortOrder==1){
				f+="return diff;\n";
			}else{
				f+="return "+col.sortOrder+" * diff;\n";
			}//endif
		}else{
			if (col.sortOrder===undefined || col.sortOrder==1){
				f+="if (diff != 0) return diff;\n";
			}else{
				f+="if (diff != 0) return "+col.sortOrder+" * diff;\n";
			}//endif
		}//endif
	}//for
	f+="\n}\ntotalSort;";

	var totalSort=null;
	try{
		totalSort = eval(f);
		return totalSort;
	}catch(e){
		Log.error("eval gone wrong", e)
	}//try
};//method



//RETURN A NEW QUERY WITH ADDITIONAL FILTERS LIMITING VALUES
//TO series AND category SELECTION *AND* TRANSFORMING TO AN SET OPERATION
Qb.specificBugs=function(query, filterParts){

	var newQuery=Qb.drill(query, filterParts);
	newQuery.edges=[];

	newQuery.select={"name":"bug_id", "value":"bug_id"};
	return newQuery;
};


//parts IS AN ARRAY OF PART NAMES CORRESPONDING TO EACH QUERY EDGE
Qb.drill=function(query, parts){
	if (query.analytic) Log.error("Do not know how to drill down on an analytic");

	var newQuery={};
	Map.copy(query, newQuery);
	newQuery.cube=undefined;
	newQuery.list=undefined;
	newQuery.index=undefined;			//REMOVE, MAY CAUSE PROBLEMS
	if (query.esfilter){
		if (query.esfilter.and){
			newQuery.esfilter={"and":query.esfilter.and.copy()};
		}else{
			newQuery.esfilter={"and":[query.esfilter]};
		}//endif
	}else{
		newQuery.esfilter={"and":[]};
	}//endif

	if (parts.length==2 && query.edges.length==1){

	}

	query.edges.forall(function(edge, e){
		if (parts[e]==undefined) return;

		for(p=0;p<edge.domain.partitions.length;p++){
			var part=edge.domain.partitions[p];
			if (
				(edge.domain.type=="time" && part.value.getMilli()==parts[e].getMilli()) ||  //CCC VERSION 2 (TIME ONLY)
				(part.name==parts[e])  //CCC VERSION 1
			){
				var filter=ESQuery.buildCondition(edge, part, query);
				newQuery.esfilter.and.push(filter);
				return;  //CONTINUE
			}//endif
		}//for
		if (edge.domain.NULL.name==parts[e]){
			var filter={"script":{"script":MVEL.compile.expression(ESQuery.compileNullTest(edge), newQuery)}};
			newQuery.esfilter.and.push(filter);
			return;  //CONTINUE
		}//endif
		Log.error("No drilling happening!");
	});

	return newQuery;

};

	//SELECT ARE JUST ANOTHER DIMENSION (ALTHOUGH DIMENSION OF MANY TYPES)
	//HERE WE CONVERT IT EXPLICITLY
	Qb.stack=function(query, newEdgeName, newSelectName){
		//ADD ANOTHER DIMENSION TO EDGE, AND ALTER Qb
		if (!query.select instanceof Array) Log.error("single cube with no objects does not need to be stacked");

		//GET select NAMES
		var parts=Array.newInstance(query.select);
		var output={"edges":query.edges.copy()};
		output.select={"name":newSelectName};
		output.edges.append({
			"name":newEdgeName,
			"domain":{"type":"set", "partitions":parts, "key":"name", "end":function(p){return p.name;}}
		});
		output.columns=output.edges.copy();
		output.columns.append(output.select);

		function stackSelect(arr){
			if (arr instanceof Array){
				return arr.map(stackSelect);
			}//endif

			//CONVERT OBJECT INTO ARRAY
			var a=[];
			for(var p=parts.length;p--;){
				var part=parts[p];
				a[p]=arr[part.name];
			}//for
			return a;
		}//method

		output.cube=stackSelect(query.cube);
		return output;
	};



	Qb.query={};
	Qb.query.prototype={};
	//GET THE SUB-Qb THE HAD name=value
	Qb.query.prototype.get=function(name, value){
		if (value===undefined && typeof(name)=="object"){
			//EXPECTING A SET OF TERMS TO FILTER BY
			var term=this.cube;
			var complicated=false;
			this.edges.forall(function(edge, i){
				var val=name[edge.name];
				if (val===undefined){
					complicated=true;
					return;
				}else if (complicated){
					Log.error("can not handle a a subset of dimensions, unless they are in order (for now)")
				}//endif
				term=term[edge.domain.getPartByKey(val).dataIndex];
			});
			return term;
		}//endif

		if (this.edges.length>1)
			//THIS ONLY INDEXES INTO A SINGLE DIMENSION
			Log.error("can not handle more than one dimension at this time");

		var edge=this.getEdge(name);
		return this.cube[edge.domain.getPartByKey(value).dataIndex];
	};

	Qb.query.prototype.indexOf=function(name, value){
		var edge=this.getEdge(name);
		return edge.domain.getPartByKey(value).dataIndex;
	};

	Qb.query.prototype.getEdge=function(name){
		return this.edges.map(function(e, i){ if (e.name==name) return e;})[0];
	};

	Q=calc2Cube;




})();

