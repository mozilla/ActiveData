/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

if (Qb===undefined) var Qb = {};

(function(){
// MAKE A Qb OF DEFAULT VALUES
Qb.cube = {};


Qb.cube.newInstance = function(edges, depth, select){
	if (depth == edges.length){
		var element={};
//		var element=[]
		if (select instanceof Array){
			for(var s = 0; s < select.length; s++){
				element[select[s].name] = select[s].domain.end(select[s].defaultValue());
//				element[s] = select[s].defaultValue();
			}//for
		} else{
			element = select.domain.end(select.defaultValue());
		}//endif
		return element;
	}//for

	var data = [];
	var p = 0;
	for(; p < edges[depth].domain.partitions.length; p++){
		data[p] = Qb.cube.newInstance(edges, depth + 1, select);
	}//for
//	if (edges[depth].domain.partitions.length==0 || edges[depth].allowNulls){
	if (edges[depth].allowNulls){
		data[p]= Qb.cube.newInstance(edges, depth + 1, select);
	}//endif
	return data;
};//method

//MAKE THE MAP ARRAY FROM NEW TO OLD COLUMN INDICIES
//newColumns[i]==oldColumns[smap[i]]
function remap(oldColumns, newColumns){
	var smap = [];
	for(var snew = 0; snew < newColumns.length; snew++){
		for(var sold = 0; sold < oldColumns.length; sold++){
			if (newColumns[snew].name == oldColumns[sold].name) smap[snew] = sold;
		}//for
	}//for
	for(var s = 0; s < newColumns.length; s++) if (smap[s]===undefined) Log.error("problem remapping columns, '"+newColumns[snew].name+"' can not be found in old columns");
	return smap;
}//method


//PROVIDE THE SAME EDGES, BUT IN DIFFERENT ORDER
Qb.cube.transpose = function(query, edges, select){
	//MAKE COMBO MATRIX
	var smap = remap(Array.newInstance(query.select), Array.newInstance(select));
	var fmap = remap(query.edges, edges);

	//ENSURE THE Qb HAS ALL DIMENSIONS
	var cube = Qb.cube.newInstance(edges, 0, []);

	var pre = "";
	var loops = "";
	var ends = "";
	var source = "query.cube";
	var dest = "cube";

	for(var i = 0; i < edges.length; i++){
		pre += "var num"+i+"=query.edges[" + i + "].domain.partitions.length;\n";
		loops += "for(var a" + i + "=0;a"+i+"<num"+i+";a"+i+"++){\n";
		ends += "}\n";
		source += "[a"+i+"]";
		dest += "[a"+fmap[i]+"]";
	}//for
	var f =
		pre+
		loops +
		dest + "="+source+";\n" +
//		"for(var i=0;i<select.length;i++) d[i]=s[smap[i]];\n" +
		ends;
	eval(f);

	return {
		"edges":edges,
		"select":select,
		"cube":cube
	}

};//method




Qb.cube.toList=function(query){

	var output=[];
	if (query.edges.length==1){
		var parts=query.edges[0].domain.partitions;
		for(var p=parts.length;p--;){
			var obj={};
			obj[query.edges[0].name]=parts[p].value;

			if (query.select instanceof Array){
				for(var s=query.select.length;s--;){
					//I FORGET IF ELEMENTS IN Qb ARE OBJECTS, OR ARRAYS
					obj[query.select[s].name]=query.cube[p][s];
//					obj[cube.select[s].name]=cube.cube[cube.select[s].name];
				}//for
			}else{
				obj[query.select.name]=query.cube[p];
			}//endif
			output.push(obj);
		}//for
	}else{
		Log.error("can only convert 1D cubes");
	}//endif

	return output;
};//method



// UNION THE CUBES, AND ADD PARTITIONS AS NEEDED
Qb.cube.union=function(cubeA, cubeB){
	//ENSURE NAMES MATCH SO MERGE IS POSSIBLE
	if (cubeA.edges.length!=cubeB.edges.length) Log.error("Expecting cubes to have smae number of edges, with matching names");
	for(var i=cubeA.edges.length;i--;){
		if (cubeA.edges[i].name!=cubeB.edges[i].name) Log.error("Expecting both cubes to have edges in the same order");
	}//for

		





};//method

})();