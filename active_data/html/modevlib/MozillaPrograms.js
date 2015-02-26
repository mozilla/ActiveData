/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


var MozillaPrograms = {
		"columns": ["projectName", "attributeName", "attributeValue", "esfilter"],
		"rows": [
			["e10s Blockers", "cf_tracking_e10s", "+"],
			["e10s (M2)", "cf_tracking_e10s", "m2+"],
			["e10s (M3)", "cf_tracking_e10s", "m3+"],
			["e10s Noms", "cf_tracking_e10s", "?"],
			["e10s Later", "cf_tracking_e10s", "later"],

			["B2G 2.1", "cf_blocking_b2g", "2.1+"],
			["B2G 2.0", "cf_blocking_b2g", ["2.0+", "1.5+"]],
			["B2G 1.4", "cf_blocking_b2g", "1.4+"],
			["B2G 1.3", "cf_blocking_b2g", "1.3+"],
			["B2G 1.3t", "cf_blocking_b2g", "1.3t+"],
			["B2G 1.4 Noms", "cf_blocking_b2g", "1.4?"],
			["B2G 1.3 Noms", "cf_blocking_b2g", "1.3?"],
			["B2G 1.3t Noms", "cf_blocking_b2g", "1.3t?"],

			["B2G 1.2.0 (Koi)", "cf_blocking_b2g", "koi+"],
			["B2G 1.1.0 (Koi)", "cf_blocking_b2g", "leo+"],
			["B2G 1.1.0 (Leo)", "cf_blocking_b2g", "leo+"],  //WAS CALLED Leo, BUT MERGED WITH Koi
			["B2G 1.0.1 (TEF)", "cf_blocking_b2g", "tef+"],
			["B2G 1.0.1 (TEF -NPOTB -POVB)", null, null, {"and": [
				{"term": {"cf_blocking_b2g": "tef+"}},
				{"not": {"terms": {"status_whiteboard.tokenized": ["npotb", "povb"]}}}
			]}
			],
			["Koi Triage (koi?)", "cf_blocking_b2g", "koi?"],
			["Leo Triage (leo?)", "cf_blocking_b2g", "leo?"],
			["TEF Triage (tef?)", "cf_blocking_b2g", "tef?"],

		["WebRTC", "status_whiteboard.tokenized", "webrtc"],

		["WebRTC Components", null, null, {"or":[ //WebRTC COMPONENTS
			{"and":[
				{"term":{"product":"loop"}},
				{"term":{"component":"general"}}
			]},
			{"and":[
				{"term":{"product":"loop"}},
				{"term":{"component":"client"}}
			]},
			{"and":[
				{"term":{"product":"loop"}},
				{"term":{"component":"server"}}
			]},
			{"and":[
				{"term":{"product":"core"}},
				{"prefix":{"component":"webrtc"}}
			]}
		]}],

		["Build Duty", "status_whiteboard.tokenized", "buildduty"],
			["Boot2Gecko (B2G)", "cf_blocking_basecamp", "+"],
			["Metro MVP", "status_whiteboard.tokenized", "metro-mvp"],
			["Security", "status_whiteboard.tokenized", "sg:dos"],
			["Security", "status_whiteboard.tokenized", "sg:low"],
			["Security", "status_whiteboard.tokenized", "sg:moderate"],
			["Security", "status_whiteboard.tokenized", "sg:high"],
			["Security", "status_whiteboard.tokenized", "sg:critical"],
			["Security", "keywords", "sec-critical"],
			["Security", "keywords", "sec-high"],
			["Security", "keywords", "sec-moderate"],
			["Security", "keywords", "sec-low"],
			["Security (High and Critical Only)", "status_whiteboard.tokenized", "sg:critical"],
			["Security (High and Critical Only)", "status_whiteboard.tokenized", "sg:high"],
			["Security (High and Critical Only)", "keywords", "sec-high"],
			["Security (High and Critical Only)", "keywords", "sec-critical"],
			["in-testsuite", "status_whiteboard.tokenized", "in-testsuite+"],
			["in-testsuite", "status_whiteboard.tokenized", "in-testsuite"],
			["testcase", "keywords", "testcase"],
			["testcase", "keywords", "testcase-wanted"],
			["Crash", "keywords", "topcrash"],            //Robert Kaiser PULLS HIS METRICS USING THIS
			["Crash", "keywords", "crash"],
			["Top Crash", "keywords", "topcrash"],        //THE KEYWORD IS ADDED AND REMOVED TO KEEP ABOUT 100 MARKED
			["QA Wanted", "keywords", "qawanted"],
			["Regression", "status_whiteboard.tokenized", "regression-window-wanted"],
			["Regression", "status_whiteboard.tokenized", "regressionwindow-wanted"],
			["Regression", "keywords", "regressionwindow-wanted"],
			["Regression", "keywords", "regression"],


	        ["Snappy", "status_whiteboard.tokenized", "snappy:p1"],
	        ["Snappy", "status_whiteboard.tokenized", "snappy:p2"],
	        ["Snappy", "status_whiteboard.tokenized", "snappy:p3"],
			["Snappy", "status_whiteboard.tokenized", "snappy"],        //Lawrence Mandel: JUST CATCH ALL SNAPPY
//        ["MemShrink", "status_whiteboard.tokenized", "memshrink:p1"],
//        ["MemShrink", "status_whiteboard.tokenized", "memshrink:p2"],
//        ["MemShrink", "status_whiteboard.tokenized", "memshrink:p3"],
			["MemShrink", "status_whiteboard.tokenized", "memshrink"],        //Nicholas Nethercote: CATCH memshrink (unconfirmed) AND ALL THE pX TOO
			["Fennec", "cf_blocking_fennec10", "+"],
			["Fennec", "cf_blocking_fennec", "+"],
			["Fennec Triage", "cf_blocking_fennec10", "?"],
			["Fennec Triage", "cf_blocking_fennec", "?"],

		["Good First Bug", "status_whiteboard.tokenized", "good first bug"],
		["Reopened", "bug_status", "reopened"]

	]
};
