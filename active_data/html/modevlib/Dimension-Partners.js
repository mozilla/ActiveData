/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");
importScript("qb/ESQuery.js");

if (!Mozilla) var Mozilla = {"name": "Mozilla", "edges": []};

Dimension.addEdges(true, Mozilla, [
	{"name": "QA", "edges": [
		{
			"name": "Partners",
			"esfilter": {"and": [
				{"term": {"component": "preinstalled b2g apps"}},
				{"terms": {"blocked": [868100,1009908,922475,889951,871656,871661,916312,976821,869192,871238,837298,904129,951752,869212,1009918,871236,869205,977321,869240,1009904,871910]}},
//				{"not": {"regexp": {"short_desc": ".*\\[tracking\\].*"}}}
			]},
			"partitions": [
			{"name": "Accuweather", "index": "bugs", "esfilter": {"term": {"blocked": 868100}}},
			{"name": "Aviary", "index": "bugs", "esfilter": {"term": {"blocked": 1009908}}},
			{"name": "Captain Rogers", "index": "bugs", "esfilter": {"term": {"blocked": 922475}}},
			{"name": "Cut the Rope", "index": "bugs", "esfilter": {"term": {"blocked": 889951}}},
			{"name": "Desert Rescue", "index": "bugs", "esfilter": {"term": {"blocked": 871656}}},
			{"name": "Distant Orbit", "index": "bugs", "esfilter": {"term": {"blocked": 871661}}},
			{"name": "Entanglement", "index": "bugs", "esfilter": {"term": {"blocked": 916312}}},
			{"name": "Evernav", "index": "bugs", "esfilter": {"term": {"blocked": 976821}}},
			{"name": "Facebook", "index": "bugs", "esfilter": {"term": {"blocked": 869192}}},
			{"name": "Gamepack", "index": "bugs", "esfilter": {"term": {"blocked": 871238}}},
			{"name": "HERE Maps", "index": "bugs", "esfilter": {"term": {"blocked": 837298}}},
			{"name": "Napster", "index": "bugs", "esfilter": {"term": {"blocked": 904129}}},
			{"name": "NILE", "index": "bugs", "esfilter": {"term": {"blocked": 951752}}},
			{"name": "Notes", "index": "bugs", "esfilter": {"term": {"blocked": 869212}}},
			{"name": "Pinterest", "index": "bugs", "esfilter": {"term": {"blocked": 1009918}}},
			{"name": "Poppit", "index": "bugs", "esfilter": {"term": {"blocked": 871236}}},
			{"name": "Twitter", "index": "bugs", "esfilter": {"term": {"blocked": 869205}}},
			{"name": "Weather Channel", "index": "bugs", "esfilter": {"term": {"blocked": 977321}}},
			{"name": "Wikipedia", "index": "bugs", "esfilter": {"term": {"blocked": 869240}}},
			{"name": "Yelp", "index": "bugs", "esfilter": {"term": {"blocked": 1009904}}},
			{"name": "YouTube", "index": "bugs", "esfilter": {"term": {"blocked": 871910}}}
		]}
	]}
])
;
