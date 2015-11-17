/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");

if (!Mozilla) var Mozilla = {"name": "Mozilla", "edges": []};

(function(){
	var pastWeek = {"gt": {"build.date": Date.today().subtract(Duration.MONTH).unix()}};

	Dimension.addEdges(false, Mozilla, [
		{"name": "Timings", "index": "jobs.action.timings", "edges": [
			{"name": "Platform", "field": "build.platform", "type": "set", "sample": pastWeek, "limit":100},
			{"name": "Product", "field": "build.product", "type": "set", "sample": pastWeek, "limit":100},
			{"name": "Suite", "field": "run.suite", "type": "set", "sample": pastWeek, "limit":100},
			{"name": "Pool", "field": "run.machine.pool", "type": "set", "sample": pastWeek, "limit":100}
		]}
	]);


})();
