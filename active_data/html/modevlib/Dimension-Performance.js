/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");

if (!Mozilla) var Mozilla = {"name": "Mozilla", "edges": []};

Dimension.addEdges(false, Mozilla, [
	{"name": "Performance", "index": "perf", "edges": [
		{"name": "Product", "field": "build.product", "type": "set", "limit": 1000},
		{"name": "BuildType", "field": "build.type", "type": "set", "limit": 1000},
		{"name": "Branch", "field": "build.branch", "type": "set", "limit": 1000},
		{"name": "Platform", "field": "build.platform", "type": "set", "limit": 1000},
		{
			"name": "OS",
			"field": ["run.machine.os", "run.machine.osversion"],
			"type": "set",
			"limit": 1000
		},
		{"name": "Test", "field": ["run.suite", "result.test"], "type": "set", "limit": 1000, "esfilter": {"range": {"build.date": {"gt": Date.today().subtract(Duration.newInstance("month")).getMilli()}}}},
		{"name": "Revision", "field": "build.revision", "type": "uid"}
	]}
]);
