/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");
importScript("qb/ESQuery.js");

if (!Mozilla) var Mozilla={"name":"Mozilla", "edges":[]};

Dimension.addEdges(true,  Mozilla, [
	{"name":"CurrentRecords", esfilter:{"range":{"expires_on":{"gt" :Date.now().addDay(1).getMilli()}}}},

	{"name":"BugStatus", "index":"bugs", "isFacet":true, "partitions":[
		{"name":"Open", "partitions":[
			{"name":"New", "esfilter":{"term":{"bug_status":"new"}}},
			{"name":"Assigned", "esfilter":{"term":{"bug_status":"assigned"}}},
			{"name":"Unconfirmed", "esfilter":{"term":{"bug_status":"unconfirmed"}}},
			{"name":"Reopened", "esfilter":{"term":{"bug_status":"reopened"}}},
			{"name":"Other", "esfilter":{"not":{"terms":{"bug_status":["resolved", "verified", "closed"]}}}}
		]},
		{"name":"Closed", "partitions":[
			{"name":"Resolved",
				"esfilter":{"term":{"bug_status":"resolved"}},
				"field":"resolution", "partitions":[
					{"name":"Fixed", "value":"fixed", "style":{}, "esfilter":{"term":{"resolution":"fixed"}}},
					{"name":"Duplicate", "value":"duplicate", "style":{"visibility":"hidden"}, "esfilter":{"term":{"resolution":"duplicate"}}},
					{"name":"Invalid", "value":"invalid", "style":{"visibility":"hidden"}, "esfilter":{"term":{"resolution":"invalid"}}},
					{"name":"Won't Fix", "value":"wontfix", "style":{"visibility":"hidden"}, "esfilter":{"term":{"resolution":"wontfix"}}},
					{"name":"WorksForMe", "value":"worksforme", "style":{"visibility":"hidden"}, "esfilter":{"term":{"resolution":"worksforme"}}}
				],
				"key":"value",
				"value":"name"
			},
			{"name":"Verified", "esfilter":{"term":{"bug_status":"verified"}}},
			{"name":"Closed", "esfilter":{"term":{"bug_status":"closed"}}}
		]}
	]},

	{"name":"ScrumBugs", "index":"bugs", "edges":[{
		"name": "Points",
		"type":"set",
		"isFacet":true, //MULTIVALUED ATTRIBUTES CAN NOT BE HANDLED BY es09.expressions.Parts2Term()
		"esfilter":ESQuery.TrueFilter,
		"partitions":[
			{"name":"P0", "esfilter":{"or":[
				{"not":{"exists":{"field":"status_whiteboard"}}},
				{"term":{"status_whiteboard.tokenized":"p=0"}}
			]}},
			{"name":"P1", "esfilter":{"term":{"status_whiteboard.tokenized":"p=1"}}},
			{"name":"P2", "esfilter":{"term":{"status_whiteboard.tokenized":"p=2"}}},
			{"name":"P3", "esfilter":{"term":{"status_whiteboard.tokenized":"p=3"}}},
			{"name":"P4", "esfilter":{"term":{"status_whiteboard.tokenized":"p=4"}}}
		]
	}]},

	{"name":"People", "partitions":[
		{"name":"MoCo", "esfilter":{}},
		{"name":"MoFo", "esfilter":{"not":{"term":{"<FIELD>":"nobody@mozilla.org"}}}}
	]},


	{"name":"Projects", "edges":[
		{"name": "B2G 1.2.0 (KOI)", "partitions":[
			{"name":"nominated", "esfilter":{"term":{"cf_blocking_b2g":"koi?"}}},
			{"name":"tracking", "esfilter":{"term":{"cf_blocking_b2g":"koi+"}}}
		]},


		{"name": "B2G 1.0.0 (TEF)", "partitions":[
			{"name":"nominated", "esfilter":{"term":{"cf_blocking_b2g":"tef?"}}},
			{"name":"tracking", "esfilter":{"term":{"cf_blocking_b2g":"tef+"}}, "edges":[
				{"name":"tracking 18", "esfilter":{"not":{"terms":{"cf_status_b2g18":["fixed","verified","unaffected","wontfix"]}}}},
				{"name":"tracking 19", "esfilter":{"not":{"terms":{"cf_status_b2g19":["fixed","verified","unaffected","wontfix"]}}}}
			]}
		]},

		{"name": "B2G 1.0.1 (Shira)",
			"description":"Project was merged with TEF",
			"expire":Date.newInstance("23FEB2013"),
			"partitions":[
				{"name":"nominated", "esfilter":{"term":{"cf_blocking_b2g":"shira?"}}},
				{"name":"tracking", "esfilter":{"term":{"cf_blocking_b2g":"shira+"}}, "edges":[
					{"name":"tracking 18", "esfilter":{"not":{"terms":{"cf_status_b2g18":["fixed","verified","unaffected","wontfix"]}}}},
					{"name":"tracking 19", "esfilter":{"not":{"terms":{"cf_status_b2g19":["fixed","verified","unaffected","wontfix"]}}}}
				]}
			]
		},

		{"name": "B2G 1.0.1 (TEF)",
			"description": "New name of TEF, once Shira was merged",
			"partitions":[
				{"name":"nominated", "esfilter":{"term":{"cf_blocking_b2g":"tef?"}}},
				{"name":"tracking", "esfilter":{"term":{"cf_blocking_b2g":"tef+"}}, "edges":[
					{"name":"tracking 18", "esfilter":{"not":{"terms":{"cf_status_b2g18":["fixed","verified","unaffected","wontfix"]}}}},
					{"name":"tracking 19", "esfilter":{"not":{"terms":{"cf_status_b2g19":["fixed","verified","unaffected","wontfix"]}}}}
				]},
				{"name":"rejected", "esfilter":{"and":[
					{"exists":{"field":"previous_values.cf_blocking_b2g_value"}},
					{"terms":{"previous_values.cf_blocking_b2g_value":["tef?","tef+"]}}
				]}}
			]
		},

		{"name": "B2G 1.0.1 (TEF -NPOTB -POVB)",  "description": "TEF, excluding partner bugs and excluding build bugs",
			"esfilter":	{"and":[
				{"term":{"cf_blocking_b2g":"tef+"}},
				{"not":{"terms":{"status_whiteboard.tokenized":["npotb","povb"]}}}
			]}
		},




		{"name": "B2G 1.1.0 (Leo)", "partitions":[
			{"name":"nominated", "esfilter":{"term":{"cf_blocking_b2g":"leo?"}}},
			{"name":"tracking", "esfilter":{"term":{"cf_blocking_b2g":"leo+"}}, "edges":[
				{"name":"tracking 18", "esfilter":{"not":{"terms":{"cf_status_b2g18":["fixed","verified","unaffected","wontfix"]}}}},
				{"name":"tracking 19", "esfilter":{"not":{"terms":{"cf_status_b2g19":["fixed","verified","unaffected","wontfix"]}}}}
			]}
		]},

		{"name": "Boot2Gecko (B2G)", "partitions":[
			{"name":"nominated", "esfilter":{"term":{"cf_blocking_basecamp": "?"}}},
			{"name":"tracking", "esfilter":{"term":{"cf_blocking_basecamp": "+"}}, "edges":[
				{"name":"tracking 18", "esfilter":{"not":{"terms":{"cf_status_b2g18":["fixed","verified","unaffected","wontfix"]}}}},
				{"name":"tracking 19", "esfilter":{"not":{"terms":{"cf_status_b2g19":["fixed","verified","unaffected","wontfix"]}}}}
			]}
		]},

		{"name": "Metro MVP", "esfilter":{"term":{"status_whiteboard.tokenized": "metro-mvp"}}},

		{"name": "Fennec 1.0", "partitions":[
			{"name":"nominated", "esfilter":{"term":{"cf_blocking_fennec10": "?"}}},
			{"name":"tracking", "esfilter":{"term":{"cf_blocking_fennec10": "+"}}}
		]},

		{"name": "Fennec", "partitions":[
			{"name":"nominated", "esfilter":{"term":{"cf_blocking_fennec": "?"}}},
			{"name":"tracking", "esfilter":{"term":{"cf_blocking_fennec": "+"}}}
		]}


	]},



	{"name": "in-testsuite", "esfilter":
		{"or":[
			{"term":{"status_whiteboard.tokenized": "in-testsuite+"}},
			{"term":{"status_whiteboard.tokenized": "in-testsuite"}}
		]}
	},

	{"name": "testcase", "esfilter":
		{"or":[
			{"term":{"keywords": "testcase"}},
			{"term":{"keywords": "testcase-wanted"}}
		]}
	},

	{"name": "Crash", "partitions":[
		{"name":"Crash", "esfilter":{"term":{"keywords":"crash"}}},//Robert Kaiser PULLS HIS METRICS USING THIS
		{"name":"Top Crash", "esfilter":{"term":{"keywords": "topcrash"}}}
	]},

	{"name": "QA Wanted", "esfilter":{"term":{"keywords": "qawanted"}}},

	{"name": "Regression", "esfilter":
		{"or":[
			{"term":{"status_whiteboard.tokenized": "regression-window-wanted"}},
			{"term":{"status_whiteboard.tokenized": "regressionwindow-wanted"}},
			{"term":{"keywords": "regressionwindow-wanted"}},
			{"term":{"keywords": "regression"}}
		]}
	},

	{"name": "Snappy", "partitions":[
		{"name": "P1", "esfilter":{"term":{"status_whiteboard.tokenized": "snappy:p1"}}},
		{"name": "P2", "esfilter":{"term":{"status_whiteboard.tokenized": "snappy:p2"}}},
		{"name": "P3", "esfilter":{"term":{"status_whiteboard.tokenized": "snappy:p3"}}},
		{"name": "Other", "esfilter":{"prefix":{"status_whiteboard.tokenized": "snappy"}}}//Lawrence Mandel: JUST CATCH ALL SNAPPY
	]},

	{"name": "MemShrink", "partitions":[  //https://wiki.mozilla.org/Performance/MemShrink#Bug_Tracking
		{"name": "P1", "esfilter":{"term":{"status_whiteboard.tokenized": "memshrink:p1"}}},
		{"name": "P2", "esfilter":{"term":{"status_whiteboard.tokenized": "memshrink:p2"}}},
		{"name": "P3", "esfilter":{"term":{"status_whiteboard.tokenized": "memshrink:p3"}}},
		{"name":"other", "esfilter":{"prefix":{"status_whiteboard.tokenized": "memshrink"}}}//Nicholas Nethercote: CATCH memshrink (unconfirmed) AND ALL THE pX TOO
	]},


	{"name":"ReleaseEngineering", "edges":[{
		"name":"TrackingFirefox",
		"esfilter":ESQuery.TrueFilter,
		"edges":[
			{"name": "29", "esfilter":{"term":{"cf_status_firefox29": "+"}}},
			{"name": "28", "esfilter":{"term":{"cf_status_firefox28": "+"}}},
			{"name": "27", "esfilter":{"term":{"cf_status_firefox27": "+"}}},
			{"name": "26", "esfilter":{"term":{"cf_status_firefox26": "+"}}}
//			{"name": "25", "esfilter":{"term":{"cf_status_firefox25": "+"}}},
//			{"name": "24", "esfilter":{"term":{"cf_status_firefox24": "+"}}},
//			{"name": "23", "esfilter":{"term":{"cf_status_firefox23": "+"}}},
//			{"name": "22", "esfilter":{"term":{"cf_status_firefox22": "+"}}},
//			{"name": "21", "esfilter":{"term":{"cf_status_firefox21": "+"}}},
//			{"name": "20", "esfilter":{"term":{"cf_status_firefox20": "+"}}},
//			{"name": "19", "esfilter":{"term":{"cf_status_firefox19": "+"}}},
//			{"name": "18", "esfilter":{"term":{"cf_status_firefox18": "+"}}},
//			{"name": "17", "esfilter":{"term":{"cf_status_firefox17": "+"}}},
//			{"name": "16", "esfilter":{"term":{"cf_status_firefox16": "+"}}},
//			{"name": "15", "esfilter":{"term":{"cf_status_firefox15": "+"}}},
//			{"name": "14", "esfilter":{"term":{"cf_status_firefox14": "+"}}},
//			{"name": "13", "esfilter":{"term":{"cf_status_firefox13": "+"}}},
//			{"name": "12", "esfilter":{"term":{"cf_status_firefox12": "+"}}},
//			{"name": "11", "esfilter":{"term":{"cf_status_firefox11": "+"}}}
		]}
	]},

	{"name":"Security", "edges":[
		{"name":"Priority",
			"type":"set",
			"isFacet":true, //MULTIVALUED ATTRIBUTES CAN NOT BE HANDLED BY es09.expressions.Parts2Term()
			"partitions":[
				{"name":"Critical", "weight":5, "style":{"color":"red"}, "esfilter":
					{"or":[
						{"term":{"status_whiteboard.tokenized": "sg:crit"}},
						{"term":{"status_whiteboard.tokenized": "sg:critical"}},
						{"term":{"keywords": "sec-critical"}}
					]}
				},
				{"name":"High", "weight":4, "style":{"color":"orange"}, "esfilter":
					{"or":[
						{"term":{"status_whiteboard.tokenized": "sg:high"}},
						{"term":{"keywords": "sec-high"}}
					]}
				},
				{"name":"Moderate", "weight":2, "style":{"color":"yellow"}, "esfilter":
					{"or":[
						{"term":{"status_whiteboard.tokenized": "sg:moderate"}},
						{"term":{"keywords": "sec-moderate"}}
					]}
				},
				{"name":"Low", "weight":1, "style":{"color":"green"}, "esfilter":
					{"or":[
						{"term":{"status_whiteboard.tokenized": "sg:low"}},
						{"term":{"keywords": "sec-low"}}
					]}
				}
			]
		},
		{"name":"Teams",
			"type":"set",
			"isFacet":true, //MULTIVALUED ATTRIBUTES CAN NOT BE HANDLED BY es09.expressions.Parts2Term()
			"esfilter": {"match_all":{}},
			"partitions":[
			{"name": "Mobile", "esfilter":
				{"terms":{"product":['Fennec','Firefox for Android']}}
			},
			{"name": "Services", "esfilter":
				{"term":{"product":"Mozilla Services"}}
			},
			{"name": "Boot2Gecko", "esfilter":
				{"term":{"product":"Boot2Gecko"}}
			},
			{"name":  "Mail", "esfilter":
				{"or":[
					{"term":{"product":"mailnews core"}},
					{"term":{"product":"thunderbird"}},
					{"and":[
						{"term":{"product":"core"}},
						{"prefix":{"component":"mail"}}
					]}
				]}
			},
			{"name": "Crypto", "esfilter":
				{"or":[
					{"terms":{"product":['JSS','NSS','NSPR']}},
					{"and":[
						{"term":{"product":"core"}},
						{"terms":{"component":['Security: PSM','Security: S/MIME']}}
					]}
				]}
			},
			{"name": "Networking", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"prefix":{"component":"networking"}}
				]}
			},
			{"name": "Architecture", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"or":[
						{"terms":{"component":['DMD','File Handling','General','Geolocation', '(hal)', 'IPC','Java: OJI','jemalloc','js-ctypes','mozglue','Permission Manager','Plug-ins','Preferences: Backend','String','XPCOM','MFBT','Disibility Access APIs']}},
						{"prefix":{"component":"Embedding"}}
					]}
				]}
			},
			{"name": "Layout", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"or":[
						{"prefix":{"component":"layout"}},
						{"prefix":{"component":"print"}},
						{"terms":{"component":[
							'css parsing and computation',
							'style system (css)',
							'svg',
							'internationalization',
							'mathml'
						]}}
					]}
				]}
			},
			{"name": "Media", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"or":[
						{"prefix":{"component":"webrtc"}},
						{"terms":{"component":['video/audio', 'web audio']}}
					]}
				]}
			},
			{"name":  "GFX", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"or":[
						{"prefix":{"component":"gfx"}},
						{"prefix":{"component":"graphics"}},
						{"prefix":{"component":"canvas"}},
						{"prefix":{"component":"widget"}},
						{"terms":{"component":['ImageLib', 'image: painting']}}
					]}
				]}
			},
			{"name":  "Frontend", "esfilter":
				{"and":[
					{"or":[
						{"term":{"product":"firefox"}},
						{"term":{"product":"toolkit"}},
						{"and":[
							{"term":{"product":"core"}},
							{"terms":{"component":['form manager','identity', 'history: global','installer: xpinstall engine','security: ui','keyboard: navigation']}}
						]}
					]}
				]}
			},
			{"name": "JavaScript", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"or":[
						{"prefix":{"component":"javascript"}},
						{"term":{"component":"Nanojit"}}
					]}
				]}
			},
			{"name": "DOM", "esfilter":
				{"and":[
					{"term":{"product":"core"}},
					{"or":[
						{"prefix":{"component":"dom"}},
						{"prefix":{"component":"xp toolkit"}},
						{"terms":{"component":['document navigation', 'drag and drop', 'editor','embedding: docshell','event handling','html: form submission','html: parser','rdf','security','security: caps','selection','serializers','spelling checker','web services','xbl','xforms','xml','xpconnect','xslt','xul']}}
					]}
				]}
			}


//			{"name" : "Other", "esfilter" : {"terms" : {"product" : ["firefox", "thunderbird", "firefox for android", "firefox for metro", "boot2gecko", "core", "nspr", "jss", "nss", "toolkit"]}}
//				}
		]}
	]}
]);
