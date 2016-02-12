////////////////////////////////////////////////////////////////////////////////
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
////////////////////////////////////////////////////////////////////////////////
// Author: Kyle Lahnakoski  (kyle@lahnakoski.com)
////////////////////////////////////////////////////////////////////////////////


function toposort(edges, nodes){
	/*
	edges IS AN ARRAY OF [parent, child] PAIRS
	nodes IS AN OPTIONAL ARRAY OF ALL NODES
	 */
	if (!nodes){
		nodes = uniqueNodes(edges);
	}//endif
	var numNodes = nodes.length;
	var sorted = new Array(numNodes);
	var visited = {};

	function visit(node, i, predecessors){
		if (predecessors.indexOf(node) >= 0) {
			Log.error(node + " found in loop" + JSON.stringify(predecessors))
		}//endif

		if (visited[i]) return;
		visited[i] = true;

		edges.forEach(function(edge, i){
			if (edge[1] !== node) return;
			visit(edge[0], i, predecessors + [node])
		});
		sorted[--numNodes] = node;
	}//function

	for(var i=numNodes;i--;){
		if (!visited[i]) visit(nodes[i], i, [])
	}//for
	return sorted;
}

function uniqueNodes(arr){
	var res = [];
	arr.forEach(function(edge){
		if (!res.contains[edge[0]]) res.push(edge[0]);
		if (!res.contains[edge[1]]) res.push(edge[1]);
	});
	return res;
}//function
