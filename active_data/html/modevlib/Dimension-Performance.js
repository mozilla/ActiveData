/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");

if (!Mozilla) var Mozilla = {"name": "Mozilla", "edges": []};

Dimension.addEdges(false, Mozilla, [
	{"name": "Talos", "index": "talos", "edges": [
		{"name": "Product", "field": "test_build.name", "type": "set", "limit": 1000},
		{"name": "Branch", "field": "test_build.branch", "type": "set", "limit": 1000},
		{"name": "Platform", "field": "test_machine.platform", "type": "set", "limit": 1000},
		{
			"name": "OS",
			"field": ["test_machine.os", "test_machine.osversion"],
			"type": "set",
			"limit": 1000
		},
		{"name": "Platform", "field": "test_machine.platform", "type": "set", "limit": 1000},
		//{"name": "TestOnly", "field": "result.test_name", "type": "set", "limit": 1000},
		{"name": "Test", "field": ["testrun.suite", "result.test_name"], "type": "set", "limit": 1000, "esfilter": {"range": {"test_build.push_date": {"gt": Date.today().subtract(Duration.newInstance("month")).getMilli()}}}},
		{"name": "Revision", "field": "test_build.revision", "type": "uid"}
	]},
//	{"name": "Eideticker", "index": "eideticker", "edges": [
//		{"name": "Device", "field": "metadata.device", "type": "set", "limit": 1000},
//		{"name": "Branch", "field": "metadata.app", "type": "set", "limit": 1000},
//		{"name": "Test", "field": "metadata.test", "type": "set", "limit": 1000},
//		{"name": "Revision", "field": "revision", "type": "uid"},
//		{"name": "Metric", "type": "set", "partition": ["fps", "checkerboard", "timetostableframe"]}
//	]},
	{"name": "B2G", "index": "public_b2g", "edges": [
		{"name": "Device", "field": "test_machine.type", "type": "set", "limit": 100},
		{"name": "OS", "field": ["test_machine.os", "test_machine.osversion"], "type": "set", "limit": 100},
		{"name": "Branch", "field": "test_build.branch", "type": "set", "limit": 100},
		{"name": "Test", "field": ["testrun.suite", "result.test_name"], "type": "set", "limit": 1000},
		{"name": "Platform", "field": "test_machine.platform", "type": "set", "limit": 100},
		{"name": "Revision", "field": {"gaia": "test_build.gaia_revision", "gecko": "test_build.gecko_revision", "gecko_repository": "test_build.gecko_repository"}, "type": "uid"}
	]}
]);
