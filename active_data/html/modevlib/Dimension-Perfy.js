/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");

if (!Mozilla) var Mozilla={"name":"Mozilla", "edges":[]};

Dimension.addEdges(false,  Mozilla, [
	{"name":"Perfy", "index":"perfy", "edges":[
		{
			"name":"Date",
			"type":"time",
			"format": "yyyy-MM-dd HH:mm:ss.ffffff",
			"interval":Duration.DAY,
			"field":"info.started"
		},
		{
			"name":"Benchmark",
			"type":"set",
			"field":["info.benchmark", "info.test"],
			"esfilter":{"range":{"started":{"gte":Date.newInstance("Nov 1, 2013").getMilli()}}}
		},
		{
			"name":"Browser",
			"value":"name",
			"partitions":[
				{"name":"Firefox23", "style":{"color":"#f9cb9c"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":23}}]}},
				{"name":"Firefox24", "style":{"color":"#f6b26b"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":24}}]}},
				{"name":"Firefox25", "style":{"color":"#e06666"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":25}}]}},
				{"name":"Firefox26", "style":{"color":"#cc0000"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":26}}]}},
				{"name":"Firefox27", "style":{"color":"#cc0000"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":27}}]}},
				{"name":"Firefox28", "style":{"color":"#cc0000"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":28}}]}},
				{"name":"Firefox29", "style":{"color":"#cc0000"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":29}}]}},
				{"name":"Firefox30", "style":{"color":"#E66000"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.version":30}}]}},
				{"name":"Chrome28", "style":{"color":"#b4a7d6"}, "esfilter":{"and":[{"term":{"browser.name":"Chrome"}}, {"term":{"browser.version":28}}]}},
				{"name":"Chrome29", "style":{"color":"#8e7cc3"}, "esfilter":{"and":[{"term":{"browser.name":"Chrome"}}, {"term":{"browser.version":29}}]}},
				{"name":"Chrome30", "style":{"color":"#8e7cc3"}, "esfilter":{"and":[{"term":{"browser.name":"Chrome"}}, {"term":{"browser.version":30}}]}},
				{"name":"Chrome31", "style":{"color":"#8e7cc3"}, "esfilter":{"and":[{"term":{"browser.name":"Chrome"}}, {"term":{"browser.version":31}}]}}
			]
		},
		{
			"name":"Channel",
			"value":"name",
			"partitions":[
				{"name":"Firefox Release", "style":{"color":"#f9cb9c"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.channel":"Release"}}]}},
				{"name":"Firefox Beta", "style":{"color":"#f6b26b"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.channel":"Beta"}}]}},
				{"name":"Firefox Aurora", "style":{"color":"#e06666"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.channel":"Aurora"}}]}},
				{"name":"Firefox Nightly", "style":{"color":"#cc0000"}, "esfilter":{"and":[{"term":{"browser.name":"Firefox"}}, {"term":{"browser.channel":"Nightly"}}]}},
				{"name":"Chrome Stable", "style":{"color":"#b4a7d6"}, "esfilter":{"and":[{"term":{"browser.name":"Chrome"}}, {"term":{"browser.channel":"Stable"}}]}},
				{"name":"Chrome Beta", "style":{"color":"#8e7cc3"}, "esfilter":{"and":[{"term":{"browser.name":"Chrome"}}, {"term":{"browser.channel":"Beta"}}]}}
			]
		},
		{
			"name":"Platform",
			"value":"name",
			"field":"browser.platform",
			"partitions":[
				{"name":"Linux", "style":{"color":"#de4815"}, "esfilter":{"term":{"browser.platform":"Linux"}}},
				{"name":"Windows 7", "style":{"color":"#136bab"}, "esfilter":{"term":{"browser.platform":"Windows 7"}}},
				{"name":"Android", "style":{"color":"#a4c739"}, "esfilter":{"term":{"browser.platform":"Android"}}},
				{"name":"Firefox OS", "style":{"color":"#ff4e00"}, "esfilter":{"term":{"browser.platform":"Firefox OS"}}}
			]
		}
	]}
]);


