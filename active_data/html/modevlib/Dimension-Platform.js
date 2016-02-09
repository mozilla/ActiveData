/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");
importScript("qb/ESQuery.js");
importScript("qb/qb.js");

if (!Mozilla) var Mozilla = {"name": "Mozilla", "edges": []};

(function(){

	var SOLVED = ["fixed", "wontfix", "unaffected", "disabled", "verified"];

	//TODO: PULL RELEASE DATES FROM http://svn.mozilla.org/libs/product-details/json/firefox_history_major_releases.json

	var releaseTracking = {
		"name": "Release",
		"isFacet": true,
		"edges": [
			{
				"name": "Firefox33",
				"version": 33,
				"releaseDate": "13 oct 2014",  //https://wiki.mozilla.org/Releases
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox33": SOLVED}}},
					{"term": {"cf_tracking_firefox33": "+"}}
				]}
			},
			{
				"name": "Firefox34",
				"version": 34,
				"releaseDate": "1 dec 2014",  //https://wiki.mozilla.org/Releases
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox34": SOLVED}}},
					{"term": {"cf_tracking_firefox34": "+"}}
				]}
			},
			{
				"name": "Firefox35",
				"version": 35,
				"releaseDate": "15 jan 2015",  //https://wiki.mozilla.org/Releases
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox35": SOLVED}}},
					{"term": {"cf_tracking_firefox35": "+"}}
				]}
			},
			{
				"name": "Firefox36",
				"version": 36,
				"releaseDate": "24 feb 2015",  //via email
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox36": SOLVED}}},
					{"term": {"cf_tracking_firefox36": "+"}}
				]}
			},
			{
				"name": "Firefox37",
				"version": 37,
				"releaseDate":"March 31, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox37": SOLVED}}},
					{"term": {"cf_tracking_firefox37": "+"}}
				]}
			},
			{
				"name": "Firefox38",
				"version": 38,
				"releaseDate":"May 12, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox38": SOLVED}}},
					{"term": {"cf_tracking_firefox38": "+"}}
				]}
			},
			{
				"name": "Firefox39",
				"version": 39,
				"releaseDate": "July 2, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox39": SOLVED}}},
					{"term": {"cf_tracking_firefox39": "+"}}
				]}
			},
			{
				"name": "Firefox40",   //https://wiki.mozilla.org/RapidRelease/Calendar#Future_branch_dates
				"version": 40,
				"releaseDate": "Aug 11, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox40": SOLVED}}},
					{"term": {"cf_tracking_firefox40": "+"}}
				]}
			},
			{
				"name": "Firefox41",
				"version": 41,
				"releaseDate": "Sep 22, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox41": SOLVED}}},
					{"term": {"cf_tracking_firefox41": "+"}}
				]}
			},
			{
				"name": "Firefox42",
				"version": 42,
				"releaseDate": "Nov 3, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox42": SOLVED}}},
					{"term": {"cf_tracking_firefox42": "+"}}
				]}
			},
			{
				"name": "Firefox43",
				"version": 43,
				"releaseDate": "Dec 15, 2015",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox43": SOLVED}}},
					{"term": {"cf_tracking_firefox43": "+"}}
				]}
			},
			{
				"name": "Firefox44",
				"version": 44,
				"releaseDate": "Jan 26, 2016",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox44": SOLVED}}},
					{"term": {"cf_tracking_firefox44": "+"}}
				]}
			},
			{
				"name": "Firefox45",
				"version": 45,
				"releaseDate": "Mar 8, 2016",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox45": SOLVED}}},
					{"term": {"cf_tracking_firefox45": "+"}}
				]}
			},
			{
				"name": "Firefox46",
				"version": 46,
				"releaseDate": "Apr 19, 2016",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox46": SOLVED}}},
					{"term": {"cf_tracking_firefox46": "+"}}
				]}
			},
			{
				"name": "Firefox47",
				"version": 47,
				"releaseDate": "May 31, 2016",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox47": SOLVED}}},
					{"term": {"cf_tracking_firefox47": "+"}}
				]}
			},
			{
				"name": "Firefox48",
				"version": 48,
				"releaseDate": "Jul 12, 2016",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox48": SOLVED}}},
					{"term": {"cf_tracking_firefox48": "+"}}
				]}
			},
			{//SOURCE https://wiki.mozilla.org/RapidRelease/Calendar
				"name": "Firefox49",
				"version": 49,
				"releaseDate": "Aug 23, 2016",
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox49": SOLVED}}},
					{"term": {"cf_tracking_firefox49": "+"}}
				]}
			}

		]
	};
	releaseTracking.requiredFields = Array.union(releaseTracking.edges.select("esfilter").map(qb.requiredFields));

	{//FIND CURRENT RELEASE, AND ENSURE WE HAVE ENOUGH RELEASES!
		var currentRelease = undefined;
		releaseTracking.edges.forall(function(e, i){
			e.dataIndex = i;  //SET HERE ONLY BECAUSE WE USE IT BELOW
			if (e.releaseDate && Date.newInstance(e.releaseDate) <= Date.today()) {
				currentRelease = e;
			}//endif
			e.retireDate=Map.get(releaseTracking.edges[i+1], "releaseDate")
		});

		if (!currentRelease) Log.error("What's the next release!?  Please add more to Dimension-Platform.js");
	}

	//NOT IN ANY OF THE THREE TRAINS
	var otherFilter = {"or": releaseTracking.edges.map(function(r, i){
		if (currentRelease.version <= r.version && r.version <= currentRelease.version + 2) return undefined;
		return r.esfilter;
	})};

	var trains = [
		{"name": "Release", "columnName": "release", "style": {"color": "#E66000"}},
		{"name": "Beta", "columnName": "beta", "style": {"color": "#FF9500"}},
		{"name": "Aurora", "columnName": "aurora", "style": {"color": "#0095DD"}},
		{"name": "Nightly", "columnName": "nightly", "style": {"color": "#002147"}}
	];


	var trainTrackingAbs = {
		"name": "Release Tracking - Desktop",
		"esFacet": true,
		"requiredFields": releaseTracking.requiredFields,
		"edges": trains.leftBut(1).map(function(t, track){
			var release = releaseTracking.edges[currentRelease.dataIndex + track];
			return Map.setDefault({}, t, release);
		})
	};
	trainTrackingAbs.edges.append({
		"name": trains.last().name,
		"columnName": "nightly",
		"version": trains.last().version,
		"style": trains.last().style,
		"esfilter": otherFilter
	});

	//SHOW TRAINS AS PARTIITONS SO THERE IS NO DOUBLE COUNTING
	var trainTrackingRel = {
		"name": "Train",
		"columnName": "train",
		"isFacet": true,
		"requiredFields": releaseTracking.requiredFields,
		"partitions": trains.leftBut(1).map(function(t, track){
			var release = releaseTracking.edges[currentRelease.dataIndex + track];
			var output = Map.setDefault({}, t, release);
			return output;
		})
	};
	trainTrackingRel.partitions.append({
		"name": trains.last().name,
		"style": trains.last().style,
		"esfilter": otherFilter
	});
	trainTrackingRel.partitions.append({
		"name": "ESR-31",
		"style": trains.last().style,
		"esfilter": {"and": [
			{"regexp": {"cf_tracking_firefox_esr31": ".*?\\+"}},
			{"term": {"cf_status_firefox_esr31": "affected"}}
		]}
	});


	Dimension.addEdges(true, Mozilla, [
		{"name": "Platform", "index": "bugs",
			"esfilter": {"or": [
				{"term": {"product": "core"}}
			]},
			"edges": [
				releaseTracking,
				trainTrackingRel,

				{
					"name": "ReleaseManagementCategories",
					"allowNulls": false,
					"partitions": [
						{
							"name": "Security",
							//"style":{"color":"#ff7f0e"},
							"esfilter":{"or":[
								{"term": {"keywords": "sec-critical"}},
								{"term": {"keywords": "sec-high"}}
							]}
						},
						{
							"name": "Stability",
							//"style": {"color":"#2ca02c"},
							"esfilter": {"or":[
								{"prefix": {"keywords": "topcrash"}},
								{"prefix": {"keywords": "crash"}}
							]}
						},
						{
							"name": "Regressions",
							//"style": {"color": "#d62728"},
							"esfilter": {"term": {"keywords": "regression"}}
						},
						{
							"name": "e10s",
							//"style": {"color": "#d62728"},
							"esfilter": {"regexp": {"cf_tracking_e10s": ".*\\+"}}
						},
						{   "name":"other",
							"style": {"color": "#CCCCCC"},
							"esfilter": {"match_all":{}}
						}
					]
				},
				{
					"name": "Categories",
					"edges": [
						{
							"name": "Security",
							"columnName": "security",
							"partitions": [
								{
									"name": "Sec-Crit",
									"style": {"color": "#d62728"},
									"esfilter": {"term": {"keywords": "sec-critical"}}
								},
								{
									"name": "Sec-High",
									"style": {"color": "#ff7f0e"},
									"esfilter": {"term": {"keywords": "sec-high"}}
								}
							]
						},
						{
							"name": "Stability",
							"columnName": "stability",
							"style": {"color": "#777777"},
							"esfilter": {"or":[
								{"terms": {"keywords": ["topcrash"]}},
								{"prefix": {"keywords": "topcrash"}}
							]}
						},
						{
							"name": "Priority",
							"columnName": "priority",
							"partitions": [
								{
									"name": "P1",
									"esfilter": {"regexp": {"status_whiteboard": ".*js:p1.*"}}
								},
								{
									"name": "P2",
									"esfilter": {"regexp": {"status_whiteboard": ".*js:p2.*"}}
								}
							]
						},

						trainTrackingAbs,

						{
							"name": "Release Tracking - FirefoxOS",
							"requiredFields": ["cf_blocking_b2g"],
							"esfilter": {"regexp": {"cf_blocking_b2g": ".*\\+"}},
							"edges": [
								{
									"name": "2.2",
									"columnName": "b2g2_2",
									"style": {"color": "#00539F"},
									"esfilter": {"term": {"cf_blocking_b2g": "2.2+"}}
								},
								{
									"name": "2.5",
									"columnName": "b2g2_5",
									"style": {"color": "#0095DD"},
									"esfilter": {"terms": {"cf_blocking_b2g": ["2.5+", "3.0+"]}}
								}
							]
						},

						{
							"name": "Pending",
							"columnName": "pending",
							"edges": [
								{
									"name": "needinfo",
									"columnName": "needinfo",
									"esfilter": { "nested": {
										"path": "flags",
										"query": {
											"filtered": {
												"query": {
													"match_all": {}
												},
												"filter": {"and": [
													{"term": { "flags.request_type": "needinfo"}},
													{"term": { "flags.request_status": "?"}},
													{"range": { "flags.modified_ts": {"gte": Date.today().subtract(Duration.newInstance("6week")).milli(), "lt": Date.today().subtract(Duration.newInstance("4day")).milli()}}}
												]}
											}
										}
									}}
								},
								{
									"name": "review?",
									"columnName": "review",
									"esfilter": {"nested": {
										"path": "attachments.flags",
										"query": {"filtered": {
											"query": {"match_all": {}},
											"filter": {"and": [
												{"term": {"attachments.flags.request_status": "?"}},
												{"terms": {"attachments.flags.request_type": ["review", "sec_review"]}},
												{"range": { "attachments.flags.modified_ts": {"gte": Date.today().subtract(Duration.newInstance("6week")).milli(), "lt": Date.today().subtract(Duration.newInstance("4day")).milli()}}}
											]}
										}}
									}}
								}
							]

						}
					]
				},

				{"name": "Bug Assignment", "isFacet": true,
					"partitions": [
						{"name": "Unassigned", "esfilter": {"term": {"assigned_to": "nobody@mozilla.org"}}},
						{"name": "Assigned", "esfilter": {"not": {"term": {"assigned_to": "nobody@mozilla.org"}}}}
					]
				},

				{"name": "Team", "isFacet": true, "esfilter": {"match_all": {}},
					"partitions": [
						{"name": "Desktop", "style": {"color": "#5DA5DA"}, "esfilter": {"or": [
							{"and": [
								{"term": {"product": "firefox"}},
								{"not": {"prefix": {"component": "dev"}}} //Anything not starting with "Developer Tools" (this is not 100% correct but should be a place to start)
							]},
							{"term": {"product": "toolkit"}}
						]}},
						{"name": "Dev Tools", "style": {"color": "#FAA43A"},  "esfilter": {"and": [
							{"term": {"product": "firefox"}},
							{"prefix": {"component": "dev"}} // Component: Anything starting with "Developer Tools"
						]}},
						{"name": "Mobile", "style": {"color": "#60BD68"},  "esfilter": {"and": [
							{"term": {"product": "firefox for android"}},
							{"not": {"prefix": {"component": "graphics"}}}  //All except "Graphics, Panning and Zooming"
						]}},
						{"name": "JS", "style": {"color": "#F17CB0"},  "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"or": [
								{"prefix": {"component": "javascript"}},  //starts with "JavaScript" or "js", "MFBT", "Nanojit"
								{"prefix": {"component": "js"}},
								{"prefix": {"component": "mfbt"}},
								{"prefix": {"component": "nanojit"}}
							]}
						]}},
						{"name": "Layout", "style": {"color": "#B2912F"}, "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"or": [
								{"prefix": {"component": "css parsing"}},  // Component: "CSS Parsing and Computation", starts with "HTML", starts with "Image", starts with "Layout", "Selection"
								{"prefix": {"component": "html"}},
								{"prefix": {"component": "image"}},
								{"prefix": {"component": "layout"}},
								{"prefix": {"component": "selection"}}
							]}
						]}},
						{"name": "Graphics", "style": {"color": "#B276B2"},  "esfilter": {"or": [
							//FROM MILAN: Jan 30th, 2015
							//In Core: Canvas: 2D, Canvas: WebGL, GFX: Color Management, Graphics, Graphics: Layers, Graphics: Text, ImageLib, Panning and Zooming
							//In Firefox for Android: Graphics, Panning and Zooming

							{"and": [
								{"term": {"product": "firefox for android"}},
								{"or": [
									{"prefix": {"component": "graphics"}},
									{"term": {"component": "panning and zooming"}}
								]}
							]},
							{"and": [
								{"term": {"product": "core"}},
								{"or": [
									{"prefix": {"component": "canvas"}},  // Anything starting with "Canvas", "Graphics", "Panning and Zooming", "SVG"
									{"prefix": {"component": "graphics"}},
									{"prefix": {"component": "panning"}},
									{"prefix": {"component": "svg"}},
									{"prefix": {"component": "gfx: color"}},
									{"terms": {"component": ["color management", "imagelib", "panning and zooming"]}}
								]}
							]}
						]}},
						{"name": "Necko", "style": {"color": "#DECF3F"},  "description": "Network", "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"prefix": {"component": "network"}}  // Product: Core, Component: starts with "Networking"
						]}},
						{"name": "Security", "style": {"color": "#F15854"},  "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"prefix": {"component": "security"}}  // Product: Core, Component: starts with "Security"
						]}},
						{"name": "DOM", "style": {"color": "#4D4D4D"},  "esfilter": {"and": [
							//From Andrew  Jan30 2015
							//
							//DOM
							//DOM: Content Processes
							//DOM: Core & HTML
							//DOM: Device Interfaces (but this category is very messy)
							//DOM: Events
							//DOM: IndexedDB
							//DOM: Push Notifications
							//DOM: Workers
							//HTML: Parser
							//IPC

							{"term": {"product": "core"}},
							{"or": [
								{"prefix": {"component": "dom"}},  // Lawrence, Nov 11, 2014: Anything starting with "DOM", "Document Navigation", "IPC", "XBL", "XForms"
								{"prefix": {"component": "document"}},
								{"prefix": {"component": "ipc"}},
								{"prefix": {"component": "xbl"}},
								{"prefix": {"component": "xform"}},
								{"term": {"component": "html: parser"}}
							]}
						]}},
						{"name": "Media", "style": {"color": "#60BDB1"},  "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"or": [
								{"prefix": {"component": "video"}},  // starts with "Video/Audio", "Web Audio", starts with "WebRTC"
								{"prefix": {"component": "web audio"}},
								{"prefix": {"component": "webrtc"}}
							]}
						]}},
						{"name": "AllY", "style": {"color": "#BA7659"},  "description": "Accessibility", "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"prefix": {"component": "disability"}}  // "Disability Access APIs"
						]}},
						{"name": "Platform Integration", "style": {"color": "#757EBA"},  "esfilter": {"and": [
							{"term": {"product": "core"}},
							{"prefix": {"component": "widget"}}  // Component: starts with "Widget"
						]}},
						{
							"name": "Other",
							"style": {"color": "#CCCCCC"},
							"esfilter": {"match_all": {}} // Any tracked bug not in one of the product/component combinations above.
						}
					]
				}
			]
		}
	]);
})();

