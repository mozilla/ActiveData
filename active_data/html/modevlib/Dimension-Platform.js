/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");
importScript("qb/ESQuery.js");

if (!Mozilla) var Mozilla = {"name": "Mozilla", "edges": []};

function requiredFields(esfilter){
	if (esfilter===undefined){
		return [];
	}else if (esfilter.and){
		return Array.union(esfilter.and.map(requiredFields));
	}else if (esfilter.or){
		return Array.union(esfilter.or.map(requiredFields));
	}else if (esfilter.not){
			return requiredFields(esfilter.not);
	}else if (esfilter.term){
		return Object.keys(esfilter.term)
	}else if (esfilter.terms){
		return Object.keys(esfilter.terms)
	}else if (esfilter.regexp){
		return Object.keys(esfilter.regexp)
	}else if (esfilter.missing){
		return [esfilter.missing.field]
	}else if (esfilter.exists){
		return [esfilter.missing.field]
	}else{
		return []
	}//endif
}//method



(function(){

	var SOLVED = ["fixed", "wontfix", "unaffected", "disabled"];

	var releaseTracking = {
		"name": "Release",
		"isFacet": true,
		"edges": [
			{
				"name": "Firefox33",
				"version": 33,
				"releaseDate":"13 oct 2014",  //https://wiki.mozilla.org/Releases
				"esfilter": {"and":[
					{"not": {"terms": {"cf_status_firefox33": SOLVED}}},
					{"term": {"cf_tracking_firefox33": "+"}}
				]}
			},
			{
				"name": "Firefox34",
				"version": 34,
				"releaseDate":"1 dec 2014",  //https://wiki.mozilla.org/Releases
				"esfilter": {"and":[
					{"not": {"terms": {"cf_status_firefox36": SOLVED}}},
					{"term": {"cf_tracking_firefox36": "+"}}
				]}
			},
			{
				"name": "Firefox35",
				"version": 35,
				"releaseDate":"15 jan 2015",  //https://wiki.mozilla.org/Releases
				"esfilter": {"and":[
					{"not": {"terms": {"cf_status_firefox35": SOLVED}}},
					{"term": {"cf_tracking_firefox35": "+"}}
				]}
			},
			{
				"name": "Firefox36",
				"version": 36,
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox36": SOLVED}}},
					{"term": {"cf_tracking_firefox36": "+"}}
				]}
			},
			{
				"name": "Firefox37",
				"version": 37,
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox37": SOLVED}}},
					{"term": {"cf_tracking_firefox37": "+"}}
				]}},
			{
				"name": "Firefox38",
				"version": 38,
				"esfilter": {"and": [
					{"not": {"terms": {"cf_status_firefox38": SOLVED}}},
					{"term": {"cf_tracking_firefox38": "+"}}
				]}
			}
		]
	};
	releaseTracking.requiredFields= Array.union(releaseTracking.edges.select("esfilter").map(requiredFields));

	{//FIND CURRENT RELEASE, AND ENSURE WE HAVE ENOUGH RELEASES!
		var currentRelease=undefined;
		releaseTracking.edges.forall(function(e, i){
			e.dataIndex=i;  //SET HERE ONLY BECAUSE WE USE IT BELOW
			if (e.releaseDate && Date.newInstance(e.releaseDate)<=Date.today()){
				currentRelease=e;
			}//endif
		});

		if (!currentRelease) Log.error("What's the next release!?  Please add more to Dimension-Platform.js");
	}
	var trains = [
		{"name":"Release", "style":{"color":"#E66000"}},
		{"name":"Beta", "style":{"color":"#FF9500"}},
		{"name":"Dev", "style":{"color":"#0095DD"}},
		{"name":"Nightly", "style":{"color":"#002147"}}
	];


	var otherFilter=[];    //NOT IN ANY OF THE THREE TRAINS
	var trainTrackingAbs = {
		"name": "Release Tracking - Desktop",
		"esFacet": true,
		"requiredFields":releaseTracking.requiredFields,
		"edges": trains.leftBut(1).map(function(t,  track){
			var release = releaseTracking.edges[currentRelease.dataIndex+track];
			otherFilter.append(release.esfilter);
			var output =  Map.setDefault({}, t, release);
			return output;
		})
	};
	trainTrackingAbs.edges.append({
		"name": trains.last().name,
		"style": trains.last().style,
		"esfilter": {"or":otherFilter}
	});

	//SHOW TRAINS AS PARTIITONS SO THERE IS NO DOUBLE COUNTING
	var trainTrackingRel = {
		"name": "Train",
		"isFacet": true,
		"requiredFields":releaseTracking.requiredFields,
		"partitions": trains.leftBut(1).map(function(t,  track){
			var release = releaseTracking.edges[currentRelease.dataIndex+track];
			otherFilter.append(release.esfilter);
			var output =  Map.setDefault({}, t, release);
			return output;
		})
	};
	trainTrackingRel.partitions.append({
		"name": trains.last().name,
		"style": trains.last().style,
		"esfilter": {"or":otherFilter}
	});
	trainTrackingRel.partitions.append({
		"name": "ESR-31",
		"style": trains.last().style,
		"esfilter": {"and":[
			{"regexp":{"cf_tracking_firefox_esr31":".*?\\+"}},
			{"term":{"cf_status_firefox_esr31":"affected"}}
		]}
	});


	Dimension.addEdges(true, Mozilla, [
		{"name": "Platform", "index": "bugs",
			"esfilter": {"or": [
				{"term": {"product": "core"}}
			]},
			"edges": [

				{
					"name": "Security",
					"partitions": [
						{
							"name": "Sec-Crit",
							"style":{"color":"#d62728"},
							"esfilter": {"term": {"keywords": "sec-critical"}}
						},
						{
							"name": "Sec-High",
							"style":{"color":"#ff7f0e"},
							"esfilter": {"term": {"keywords": "sec-high"}}
						}
					]
				},
				{
					"name": "Stability",
					"style":{"color":"#777777"},
					"esfilter": {"terms": {"keywords": ["topcrash"]}}
				},
				{
					"name": "Priority",
					"partitions": [
						{
							"name": "P1",
							"esfilter": {"regexp": {"status_whiteboard": ".*js:p1.*"}}
						},
						{
							"name": "P2",
							"esfilter": {"regexp": {"status_whiteboard": ".*js:p2.*"}}
						}
//						{"name": "Triage", "esfilter": {"regexp": {"status_whiteboard": ".*js:t.*"}}}
					]
				},

				releaseTracking,
				trainTrackingAbs,
				trainTrackingRel,

				{
					"name": "Release Tracking - FirefoxOS",
					"requiredFields": ["cf_blocking_b2g"],
					"esfilter": {"regexp": {"cf_blocking_b2g": ".*\\+"}},
					"edges": [
						{
							"name": "2.1",
							"style":{"color":"#00539F"},
							"esfilter": {"term": {"cf_blocking_b2g": "2.1+"}}
						},
						{
							"name": "2.2",
							"style":{"color":"#0095DD"},
							"esfilter": {"term": {"cf_blocking_b2g": "2.2+"}}
						}
					]
				},

				{"name": "Bug Assignment", "isFacet": true,
					"partitions": [
						{"name": "Unassigned", "esfilter": {"term": {"assigned_to": "nobody@mozilla.org"}}},
						{"name": "Assigned", "esfilter": {"not": {"term": {"assigned_to": "nobody@mozilla.org"}}}}
					]
				},

				{"name": "Team", "isFacet": true, "esfilter":{"match_all":{}},
					"partitions":[
						{"name": "Desktop", "esfilter": {"or": [
							{"and": [
								{"term": {"product": "firefox"}},
								{"not": {"prefix": {"component": "dev"}}} //Anything not starting with "Developer Tools" (this is not 100% correct but should be a place to start)
							]},
							{"term": {"product": "toolkit"}}
						]}},
						{"name": "Dev Tools", "esfilter": {"and": [
							{"term": {"product": "firefox"}},
							{"prefix": {"component": "dev"}} // Component: Anything starting with "Developer Tools"
						]}},
						{"name":"Mobile", "esfilter": {"and":[
							{"term":{"product":"firefox for android"}},
							{"not":{"prefix":{"component":"graphics"}}}  //All except "Graphics, Panning and Zooming"
						]}},
						{"name":"JS", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"or":[
								{"prefix":{"component":"javascript"}},  //starts with "JavaScript" or "js", "MFBT", "Nanojit"
								{"prefix":{"component":"js"}},
								{"prefix":{"component":"mfbt"}},
								{"prefix":{"component":"nanojit"}}
							]}
						]}},
						{"name":"Layout", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"or":[
								{"prefix":{"component":"css parsing"}},  // Component: "CSS Parsing and Computation", starts with "HTML", starts with "Image", starts with "Layout", "Selection"
								{"prefix":{"component":"html"}},
								{"prefix":{"component":"image"}},
								{"prefix":{"component":"layout"}},
								{"prefix":{"component":"selection"}}
							]}
						]}},
						{"name":"Graphics", "esfilter": {"or":[
							{"and":[
								{"term":{"product":"firefox for android"}},
								{"prefix":{"component":"graphics"}}   //Component: "Graphics, Panning and Zooming"
							]},
							{"and":[
								{"term":{"product":"core"}},
								{"or":[
									{"prefix":{"component":"canvas"}},  // Anything starting with "Canvas", "Graphics", "Panning and Zooming", "SVG"
									{"prefix":{"component":"graphics"}},
									{"prefix":{"component":"panning"}},
									{"prefix":{"component":"svg"}}
								]}
							]}
						]}},
						{"name":"Necko", "description":"Network", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"prefix":{"component":"network"}}  // Product: Core, Component: starts with "Networking"
						]}},
						{"name":"Security", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"prefix":{"component":"security"}}  // Product: Core, Component: starts with "Security"
						]}},
						{"name":"DOM", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"or":[
								{"prefix":{"component":"dom"}},  // Anything starting with "DOM", "Document Navigation", "IPC", "XBL", "XForms"
								{"prefix":{"component":"document"}},
								{"prefix":{"component":"ipc"}},
								{"prefix":{"component":"xbl"}},
								{"prefix":{"component":"xform"}}
							]}
						]}},
						{"name":"Media", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"or":[
								{"prefix":{"component":"video"}},  // starts with "Video/Audio", "Web Audio", starts with "WebRTC"
								{"prefix":{"component":"web audio"}},
								{"prefix":{"component":"webrtc"}}
							]}
						]}},
						{"name":"AllY", "description":"Accessibility", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"prefix":{"component":"disability"}}  // "Disability Access APIs"
						]}},
						{"name":"Platform Integration", "esfilter": {"and":[
							{"term":{"product":"core"}},
							{"prefix":{"component":"widget"}}  // Component: starts with "Widget"
						]}},
						{
							"name":"Other",
							"esfilter": {"match_all":{}} // Any tracked bug not in one of the product/component combinations above.
						}
					]
				}
			]
		}
	]);
})();

