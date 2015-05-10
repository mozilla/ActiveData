/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("Dimension.js");

if (!Mozilla) var Mozilla={"name":"Mozilla", "edges":[]};


////////////////////////////////////////////////////////////////////////////////
// USE THE separator TO SPLIT THE VALUE INTO A LIST OF PARTITION OBJECTS
//
function string2path(fieldName, separator){
	return function(v){
		var list=v.split(separator);
		list[0]={"name":list[0], "path":list[0]};
		for(var i=1;i<list.length;i++) list[i]={"name":list[i], "path":list[i-1].path+separator+list[i]};
		return list.map(function(v, i){
			return {
				"name":v.name, //ONLY WANT THE LAST ONE FOR
				"esfilter":{"prefix":Map.newInstance(fieldName, v.path)}
			};
		});
	};//method
}//function



Dimension.addEdges(false, Mozilla, [

	{"name":"Telemetry", "index":"raw_telemetry", "edges":[
		{"name":"Date",
			"field":"date",
//			"type":"set",
//			"path":function(v){return [{
//				"name":Date.newInstance(v).format("dd MMM yyyy"),
//				"esfilter":{"term":{"date":v}}
//			}]},
			"type":"time",
			"min":Date.newInstance("10APR2013"), "max":Date.newInstance("16APR2013"), "interval":Duration.DAY
		},

		{"name":"Measures", "edges":[
			{"name":"Start", "field":"simpleMeasurements.start", "type":"numeric", "default":{"aggregate":["median", "average"]}},
			{"name":"Main", "field":"simpleMeasurements.main", "type":"numeric", "default":{"aggregate":["median", "average"]}},
			{"name":"Top Window", "field":"simpleMeasurements.createTopLevelWindow", "type":"numeric", "default":{"aggregate":["median", "average"]}},
			{"name":"Session Restored", "field":"simpleMeasurements.sessionRestored", "type":"numeric", "default":{"aggregate":["median", "average"]}},
			{"name":"First Paint", "field":"simpleMeasurements.firstPaint", "type":"numeric", "min":0, "max":2000, "interval":100, "default":{"aggregate":["median", "average"]}},
			{"name":"First Load URI", "field":"simpleMeasurements.firstLoadURI", "type":"numeric", "default":{"aggregate":["median", "average"]}},

//			{"name":"", "field":"simpleMeasurements.sessionRestoreInitialized", "type":"numeric", "default":{"aggregate":["median", "average"]}},

//			{"name":"", "field":"simpleMeasurements.startupCrashDetectionBegin", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.startupCrashDetectionEnd", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.delayedStartupStarted", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.delayedStartupFinished", "type":"numeric", "default":{"aggregate":["median", "average"]}},


//			{"name":"", "field":"simpleMeasurements.startupSessionRestoreReadBytes", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.startupSessionRestoreWriteBytes", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.startupWindowVisibleReadBytes", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.startupWindowVisibleWriteBytes", "type":"numeric", "default":{"aggregate":["median", "average"]}},

//			{"name":"", "field":"simpleMeasurements.uptime", "type":"numeric", "default":{"aggregate":["median", "average"]}},
//			{"name":"", "field":"simpleMeasurements.shutdownDuration", "type":"numeric", "default":{"aggregate":["median", "average"]}},
			{"name":"Startup Interrupted", "field":"simpleMeasurements.startupInterrupted", "type":"boolean"},
			{"name":"Debugger Attached", "field":"simpleMeasurements.debuggerAttached", "type":"boolean"}
		]},

		{"name":"User Defined", "edges":[
			{"name":"Ordered Startup",
				"description":"Startup events can happen out of the usual order.  This edge partitions all samples into the ordered (main->creatTopLevelWindow->firstPaint->firstLoadURI->sessionRestored) and the unordered (all others).",
				"partitions":[
				{"name":"Orderly", "esfilter":{"and":[
					{"term":{"simpleMeasurements.startupInterrupted":0}},
					{"term":{"simpleMeasurements.debuggerAttached":0}},
					{"script":{"script":"doc[\"simpleMeasurements.start\"].value <= doc[\"simpleMeasurements.main\"].value"}},
					{"script":{"script":"doc[\"simpleMeasurements.main\"].value <= doc[\"simpleMeasurements.createTopLevelWindow\"].value"}},
					{"script":{"script":"doc[\"simpleMeasurements.createTopLevelWindow\"].value <= doc[\"simpleMeasurements.firstPaint\"].value"}},
					{"script":{"script":"doc[\"simpleMeasurements.firstPaint\"].value <= doc[\"simpleMeasurements.sessionRestored\"].value"}},
					{"or":[
						{"not":{"exists":{"field":"doc[\"simpleMeasurements.firstLoadURI\"].value"}}},
						{"script":{"script":"doc[\"simpleMeasurements.firstPaint\"].value <= doc[\"simpleMeasurements.firstLoadURI\"].value"}},
						{"script":{"script":"doc[\"simpleMeasurements.firstLoadURI\"].value <= doc[\"simpleMeasurements.sessionRestored\"].value"}}
					]}
					]}
				},
				{"name":"Unorderly", "esfilter":ESQuery.TrueFilter}
			]},

			{"name":"Warm or Cold", "esfilter":ESQuery.TrueFilter, "partitions":[
				{"name":"Warm", "esfilter":{"script":{"script":"doc[\"simpleMeasurements.main\"].value - doc[\"simpleMeasurements.start\"].value <= 100"}}},
				{"name":"Cold", "esfilter":{"script":{"script":"doc[\"simpleMeasurements.main\"].value - doc[\"simpleMeasurements.start\"].value > 100"}}}
			]},

			{"name":"Start to FirstPaint", "field":"simpleMeasurements.firstPaint-simpleMeasurements.start", "type":"numeric"}
		]},
		{"name":"Instance", "edges":[
			{"name":"reason", "field":"info.reason", "type":"set"},
			{
				"name":"Addons",
				"field":"info.addons.name",
				"type":"set",
				"esfilter":ESQuery.TrueFilter,
				"isFacet":true,
				"end":function(p){return p.name;},
				"path":function(v){
					//MAP TO HUMANE NAMES, IF POSSIBLE
					return [{
						"name":coalesce(Telemetry.addonGUID2Name[v.toLowerCase()], v.toLowerCase()),
						"esfilter":{"prefix":{"info.addons.name":v}}
					}];
				}
			}
		]},

		{"name":"Application", "edges":[
			{"name":"ID", "field":"info.appID", "type":"set"},
			{"name":"Name", "field":"info.appName", "type":"set"},
			{"name":"Update Channel", "field":"info.appUpdateChannel", "type":"set"},
			{
				"name":"Version",
				"field":"info.appVersion",
				"type":"set",
				"path":string2path("info.appVersion", ".")
			},
			{"name":"Build ID",
				"field":"info.appBuildID",
				"type":"set",
				"path":function(v){
					v=""+v;
					return [//DATA IN yyyyMMddHHmmss FORMAT
						{"name":v.left(8), "esfilter":{"range":{"info.appBuildID":{"gte":convert.String2Integer(v.left(8)+"000000"), 'lt':convert.String2Integer(v.left(8)+"000000")+1000000}}}},
						{"name":v.rightBut(8), "esfilter":{"term":{"info.appBuildID": v}}}
					]
				},
				"esfilter":ESQuery.TrueFilter
			},
			{"name":"Platform Build ID", "field":"info.platformBuildID", "type":"set", "esfilter":ESQuery.TrueFilter}
		]},

		{"name":"Environment", "edges":[
			{"name":"OS", "field":"info.OS", "type":"set"},
			{"name":"OS Version", "field":"info.version", "type":"set"},
			{
				"name":"Flash Version",
				"field":"info.flashVersion",
				"type":"set",
				"path":string2path("info.flashVersion", ".")
			},
			{"name":"Arch", "field":"info.arch", "type":"set"},
			{"name":"Locale", "field":"info.locale", "type":"set"},
			{"name":"MemSize (Linear)", "field":"info.memsize", "type":"numeric", "min":"0", "max":15000, "interval":1000, "aggregate":["median", "average"]},
			{"name":"MemSize (Category)", "field":"info.memsize",
				"type":"set",
				"allowNulls":true,
				"partitions":Array.newRange(0, 9).map(function(v){
					return {
						"name":(128*Math.pow(2, v))+"meg",
						"value":(128*Math.pow(2, v))+"meg",
						"esfilter":{"range":{"info.memsize":{"gte":aMath.round(128*Math.pow(2, v-.5),0), "lt":aMath.round(128*Math.pow(2, v+.5), 0)}}}
					};
				}),
				"value":"name"
			},
			{"name":"CPU Count", "field":"info.cpucount", "type":"count"},
			{"name":"DWriteVersion",
				"field":"info.DWriteVersion",
				"type":"set",
				"path":string2path("info.flashVersion", ".")
			},
			{"name":"DWriteEnabled", "field":"info.DWriteEnabled", "type":"boolean"},

			{"name":"Flags", "edges":[
				{"name":"has MMX", "field":"info.hasMMX", "type":"boolean"},
				{"name":"has EDSP", "field":"info.hasEDSP", "type":"boolean"},
				{"name":"has NEON", "field":"info.hasNEON", "type":"boolean"},
				{"name":"has ARMv6", "field":"info.hasARMv6", "type":"boolean"},
				{"name":"has ARMv7", "field":"info.hasARMv7", "type":"boolean"},
				{"name":"has SSE", "field":"info.hasSSE", "type":"boolean"},
				{"name":"has SSE2", "field":"info.hasSSE2", "type":"boolean"},
				{"name":"has SSE3", "field":"info.hasSSE3", "type":"boolean"},
				{"name":"has SSE4A", "field":"info.hasSSE4A", "type":"boolean"},
				{"name":"has SSE4_1", "field":"info.hasSSE4_1", "type":"boolean"},
				{"name":"has SSE4_2", "field":"info.hasSSE4_2", "type":"boolean"}
			]}

		]},

		{"name":"Adapter", "edges":[
			{"name":"Device ID", "field":"info.adapterDeviceID", "type":"set", "esfilter":ESQuery.TrueFilter},
			{"name":"Description", "field":"info.adapterDescription", "type":"set", "esfilter":ESQuery.TrueFilter},
			{"name":"Vendor ID", "field":"info.adapterVendorID", "type":"set"},
			{"name":"RAM", "field":"info.adapterRAM", "type":"set"},
			{"name":"Driver", "field":"info.adapterDriver", "type":"set", "esfilter":ESQuery.TrueFilter},
			{"name":"DriverVersion", "field":"info.adapterDriverVersion", "type":"set", "esfilter":ESQuery.TrueFilter},
			{"name":"DriverDate", "field":"info.adapterDriverDate", "type":"set", "esfilter":ESQuery.TrueFilter}
		]}
	]}
]);

var Telemetry={};

Telemetry.addonGUID2Name = {
	"{972ce4c6-7e08-4474-a285-3208198ce6fd}": "Default theme",
	"{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}": "Adblock Plus",
	"{b9db16a4-6edc-47ec-a1f4-b86292ed211d}": "DownloadHelper",
	"{e4a8a97b-f2ed-450b-b12d-ee082ba24781}": "GreaseMonkey",
	"{73a6fe31-595d-460b-a920-fcc0f8843232}": "NoScript",
	"{19503e42-ca3c-4c27-b1e2-9cdb2170ee34}": "FlashGot",
	"{DDC359D1-844A-42a7-9AA1-88A850A938A8}": "DownThemAll!",
	"{c0c9a2c7-2e5c-4447-bc53-97718bc91e1b}": "Easy YouTube Video Downloader",
	"{D4DD63FA-01E4-46a7-B6B1-EDAB7D6AD389}": "Download Statusbar",
	"{46551EC9-40F0-4e47-8E18-8E5CF550CFB8}": "Stylish",
	"{37964A3C-4EE8-47b1-8321-34DE2C39BA4D}": "Sputnik@Mail.ru",
	"{1018e4d6-728f-4b20-ad56-37578a4de76b}": "Flagfox",
	"{8620c15f-30dc-4dba-a131-7c5d20cf4a29}": "Nightly Tester Tools",
	"{58bd07eb-0ee0-4df0-8121-dc9b693373df}": "Browser Manager MALWARE",
	"{a0d7ccb3-214d-498b-b4aa-0e8fda9a7bf7}": "Web of Trust",
	"{0F827075-B026-42F3-885D-98981EE7B1AE}": "BrowserProtect ADWARE"
};

Map.forall(Telemetry.addonGUID2Name, function(k,v){
	Telemetry.addonGUID2Name[k.toLowerCase()]=v;
});
