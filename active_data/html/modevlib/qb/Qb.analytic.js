/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
if (Qb===undefined) var Qb = {};

Qb.analytic={};

Qb.analytic.ROWNUM="__rownum";
Qb.analytic.ROWS="__rows";


Qb.analytic.run=function(query){
	if (query.analytic){
		if (!(query.analytic instanceof Array)) query.analytic = [query.analytic];
		//ANALYTIC COLUMNS ARE ADDED IN ORDER SO EACH CAN REFER TO THE PREVIOUS
		for(var a = 0; a < query.analytic.length; a++){
			Qb.analytic.add(query, query.analytic[a]);
		}//for
	}//endif
};//method


//from IS AN ARRAY OF OBJECTS
//sourceColumns IS AN ARRAY OF COLUMNS DEFINING THE TUPLES IN from
Qb.analytic.add=function(query, analytic){

	var edges = analytic.edges;		//ARRAY OF COLUMN NAMES
	if (edges===undefined) Log.error("Analytic expects 'edges' to be defined, even if empty");
	var sourceColumns=query.columns;

	var copyEdge=false;         //I WISH THE ELEMENT DID NOT HAVE TO BE POLLUTED WITH THE EDGE VALUE
	var parts=null;
	var from;
	if (query.edges.length==1 && query.cube!=undefined){
		from=query.cube;
		copyEdge=true;
		parts=query.edges[0].domain.partitions;
	}else if (query.list!=undefined){
		from=query.list;
	}else{
		Log.error("Analytic not defined, yet");
	}//endif

	//FILL OUT THE ANALYTIC A BIT MORE
	if (analytic.name===undefined) analytic.name=splitField(analytic.value).last();
	sourceColumns.forall(function(v){ if (v.name==analytic.name)
		Log.error("All columns must have different names");});
	analytic.columnIndex=sourceColumns.length;
	sourceColumns[analytic.columnIndex] = analytic;
	analytic.calc=Qb.analytic.compile(sourceColumns, analytic.value);
	analytic.domain=Qb.domain.value;

	if (analytic.where===undefined) analytic.where="true";
	var where=Qb.analytic.compile(sourceColumns, analytic.where);

//	sourceColumns=sourceColumns.copy();

//	var columns={};
//	sourceColumns.forall(function(v, i){
//		columns[v.name]=v;
//	});

	var nullGroup=[];
	var allGroups=[];
	if (edges.length==0){
		allGroups.push([]);
		for(i = from.length; i --;){
			var row = from[i];
			if (copyEdge) row[query.edges[0].name]=parts[i];
//			if (copyEdge) row[query.edges[0].name]=query.edges[0].domain.end(parts[i]);  CONFLICTS WITH Qb.sort.compile(), WHICH EXPECTS A PARTITION OBJECT
			if (!where(null, -1, row)){
				nullGroup.push(row);
				continue;
			}//endif
			allGroups[0].push(row);
		}//for
	}else{
		var tree = {};  analytic.tree=tree;
		for(i = from.length; i --;){
			var row = from[i];
			if (copyEdge) row[query.edges[0].name]=parts[i];
//			if (copyEdge) row[query.edges[0].name]=query.edges[0].domain.end(parts[i]);  CONFLICTS WITH Qb.sort.compile(), WHICH EXPECTS A PARTITION OBJECT
			if (!where(null, -1, row)){
				nullGroup.push(row);
				continue;
			}//endif

			//FIND RESULT IN tree
			var trunk = tree;
			for(var f = 0; f < edges.length; f++){
				var branch=trunk[row[edges[f]]];

				if (branch===undefined){
					if (f==edges.length-1){
						branch=[];
						allGroups.push(branch);
					}else{
						branch={};
					}//endif
					trunk[row[edges[f]]]=branch;
				}//endif
				trunk=branch;
			}//for
			trunk.push(row);
		}//for
	}//endif
//	yield (Thread.yield());

	//SORT
	var sortFunction;
	if (analytic.sort){
		if (!(analytic.sort instanceof Array)) analytic.sort=[analytic.sort];
		sortFunction=Qb.sort.compile(analytic.sort, sourceColumns, true);
	}//endif

	for(var g=allGroups.length;g--;){
		var group=allGroups[g];
		if (sortFunction) group.sort(sortFunction);

		for(rownum=group.length;rownum--;){
			group[rownum][Qb.analytic.ROWNUM]=rownum;		//ASSIGN ROWNUM TO EVERY ROW
			group[rownum][Qb.analytic.ROWS]=group;		//EVERY ROW HAS REFERENCE TO IT'S GROUP
		}//for
	}//for
	{//NULL GROUP
		if (sortFunction) nullGroup.sort(sortFunction);

		for(rownum=nullGroup.length;rownum--;){
			nullGroup[rownum][Qb.analytic.ROWNUM]=null;
			nullGroup[rownum][Qb.analytic.ROWS]=null;
		}//for
	}


	//PERFORM CALC
	for(var i=from.length;i--;){
		from[i][analytic.name]=analytic.calc(from[i][Qb.analytic.ROWS], from[i][Qb.analytic.ROWNUM], from[i]);
		from[i][Qb.analytic.ROWNUM]=undefined;	//CLEANUP
		from[i][Qb.analytic.ROWS]=undefined;	//CLEANUP
	}//for

};


Qb.analytic.compile = function(sourceColumns, expression){
	var func;


	if (expression === undefined) Log.error("Expecting expression");

//COMPILE THE CALCULATION OF THE DESTINATION COLUMN USING THE SOURCE COLUMNS
	var f = "func=function(rows, rownum, __source){\n";
	for(var s = 0; s < sourceColumns.length; s++){
		var columnName = sourceColumns[s].name;
			if (["rows", "rownum"].contains(columnName)){
				continue;
			}//endif
//ONLY DEFINE VARS THAT ARE USED
		if (expression.indexOf(columnName) != -1){
			f += "var ";
			var parents = splitField(columnName).leftBut(1);
			for (var p=0;p<parents.length;p++){
				f += joinField(parents.left(p+1)) + " = {};\n";
			}//for
			f += columnName + "=__source." + columnName + ";\n";
		}//endif
	}//for
	f +=
		"var output;\n" +
			"try{ " +
			" output = (" + expression + "); " +
			" if (output===undefined) Log.error(\"analytic returns undefined\");\n" +
			" return output;\n" +
			"}catch(e){\n" +
			" Log.error("+
				"\"Problem with definition of value=" + convert.String2Quote(convert.String2Quote(expression)).leftBut(1).rightBut(1) + " when operating on __source=\"+convert.value2json(__source)"+
				"+\" Are you trying to get an attribute value from a NULL part?\"" +
			", e)"+
			"}}";
	try{
		eval(f);
	} catch(e){
		Log.error("can not compile " + f, e);
	}//try
	return func;
};//method
