
importScript("../modevlib/layouts/dynamic.js");
importScript("../modevlib/layouts/layout.js");
importScript("../modevlib/collections/aMatrix.js");


var FAIL_PREFIX = "failure_";
var currentClicker=null;
var FROM_DATE = Date.today().subtract(Duration.WEEK);
var RECENT_DATE = Date.today().subtract(Duration.DAY);
var TO_DATE = Date.now().floor(Duration.DAY);
var NOW = Date.now();

var tests = {};  //MAP FROM FULL NAME TO UNIQUE TEST
var exclude = [];  //STUFF TO IGNORE



function showFailureTable(testGroups){
	var header = '<tr>' +
		'<th>Score</th>' +
		'<th>Last Fail</th>' +
		'<th>Fail Count</th>' +
		'<th>Suite</th>' +
		'<th>Test</th>' +
		'<th>Platform</th>' +
		'<th>Build Type</th>' +
		'<th style="width:200px;">Subtests</th>' +
		'</tr>';

	var ROW_TEMPLATE = new Template('<tr class="hoverable" id="' + FAIL_PREFIX + '{{rownum}}">' +
		'<td>{{score}}</td>' +
		'<td>{{last_fail|datetime}}</td>' +
		'<td>{{failure_count|html}}</td>' +
		'<td>{{suite|html}}</td>' +
		'<td>{{test|html}}</td>' +
		'<td>{{platform|html}}</td>' +
		'<td>{{build_type|html}}</td>' +
		'<td style="width:200px;">{{subtests|json|html}}</td>' +
		'</tr>'
	);

	body = testGroups.map(function(g, i){
		g.rownum=i;
		g.score = coalesce(g.score, "");
		return ROW_TEMPLATE.expand(g);
	}).join("");

	$("#details").html(
		'<table class="table">' +
		header +
		body +
		'</table>'
	);

	//ADD CLICKERS
	testGroups.forall(function(g){
		var id = g.rownum;
		$("#" + FAIL_PREFIX + id).click(function(e){
			if (currentClicker){
				currentClicker.kill();
			}//endif
			$("#chart").html("");
			var id = convert.String2Integer($(this).attr("id").substring(FAIL_PREFIX.length));
			var g=testGroups[id];

			if (!g.thread){
				g.thread = new Thread(function*(){
					yield (getDetails(0, [g]));
				});
				g.thread.start();
			}//endif

			currentClicker = Thread.run(chart(g));
		});
	});

	//ADD REMOVE BUTTONS
	$("#details").find("td").each(function(){
		add_remove_button(this);
	});
	layoutAll();

}//function


var REMOVE_BUTTON_CLASS = "x";


var add_remove_button = function(element){
	element = $(element);
	var id = "remove_" + Util.UID();

	var BUTTON = new Template(
		'<div id={{id|quote}} layout="mr=..mr" style="height:20px;width=20px;display: none;" dynamicStyle=":hover={background_color:red}">' +
		'<img src="images/x-3x.png">' +
		'</div>'
	);
	element.append(BUTTON.expand({"id":id}));

	// CLICK WILL ADD RESULTS TO EXCLUDE LIST
	$("#"+id).click(function(e){
		var text = $(this).parent().text();
		exclude = Array.union(exclude, [text]);
		//REMOVE ALL ROWS WITH GIVEN text
		//ADD REMOVE TO URL
		//ADD REMOVE TO LIST OF REMOVES
	});

	// SHOW BUTTON DURING HOVER
	element.hover(function(e){
		$("#"+id).show();
	}, function(e){
		$("#"+id).hide();
	});

	$(element).updateDynamicStyle();
};


var chart = function*(testGroup){

	var a = Log.action("Wait for detailed data", true);
	try {
		if (testGroup.thread !== undefined) {
			yield (Thread.join(testGroup.thread));
		}//endif
	}catch (e){
		Log.warning("chart cancelled", e);
	} finally {
		Log.actionDone(a)
	}//try

	a = Log.action("Make chart", true);
	try {
		//SORT REVISIONS BY build_date
		var revisions = (yield(qb.calc2List({
			"from": testGroup.details,
			"select": {"value": "build_date", "aggregate": "min"},
			"edges": ["revision"],
			"sort": "build_date"
		})));
		revisions = revisions.list.select("revision");

		var duration = (yield(Q({
			"from": testGroup.details,
			"select": [
				{"value": "((build_date!=null && duration==null) ? -1 : duration)", "aggregate": "average"},
				{"value": "build_date", "aggregate": "min"}
			],
			"edges": [
				{
					"name":"result",
					"value": //{"when":{"missing":"duration"}, "then":{"literal":"incomplete"}, "else":"ok"}
					"(duration==null ? 'incomplete' : ok)",
					"domain":{"type":"set", "partitions":[
						{"name":"pass", "value":true, "style":{"color": "#1f77b4"}},
						{"name":"fail", "value":false, "style": {"color": "#ff7f0e"}},
						{"name":"incomplete", "value":"incomplete", "style": {"color": "#d62728"}}
					]}
				},
				{"value": "revision", "domain": {"partitions": revisions}}
			]
		})));

		new Matrix({"data":duration.cube}).forall(function(v){
			if (v.build_date != null){
				v.duration = -1;
			}//endif
		});

		aChart.showScatter({
			"id": "chart",
			"cube": duration,
			xAxisSize: 50,
			"legend": true,
			extensionPoints: {
				line_lineWidth: 0
			}
		});
	} finally {
		Log.actionDone(a);
	}//try
};//function

// ALL TESTS RESUTLS FOR TESTS IN LIST testGroups
var getDetails = function*(group, testGroups){
	var a = Log.action("get details #" + group, true);
	try {
		//PULL SUCCESS
		var success = yield (search({
			"from": "unittest",
			"select": [
				"_id",
				{"name": "duration", "value": "result.duration"},
				{"name": "suite", "value": "run.suite"},
				{"name": "chunk", "value": "run.chunk"},
				{"name": "test", "value": "result.test"},
				{"name": "platform", "value": "build.platform"},
				{"name": "build_type", "value": "build.type"},
				{"name": "build_date", "value": "build.date"},
				{"name": "branch", "value": "build.branch"},
				{"name": "revision", "value": "build.revision12"},
				{"name": "ok", "value": "result.ok"}
			],
			"where": {"and": [
				{"gte": {"build.date": FROM_DATE.unix()}},
				{"lt": {"build.date": TO_DATE.unix()}},
				{"eq": {"build.branch": "mozilla-inbound"}},
				{"or": testGroups.map(function(r){
					return {"eq": {
						"run.suite": r.suite,
						"result.test": r.test,
						"build.platform": r.platform,
						"build.type": r.build_type
					}}
				})}
			]},
			"limit": 100000,
			"format": "list"
		}));

		addGroupId(success, ["suite", "test", "platform", "build_type"]);

		var groupedSuccesses = (yield (qb.calc2List({
			"from": success.data,
			"select": [
				{"name": "last_good", "value": "build_date", "aggregate": "max"},
				{"name": "details", "value": ".", "aggregate": "list"},
				{"value": "_group_id", "aggregate": "one"},
				{"name": "success_count", "value": ".", "aggregate": "count"}
			],
			"edges": ["suite", "test", "platform", "build_type"]
		}))).list;


		//CALCULATE THE TOTAL WEIGHT TO NORMALIZE THE TEST SCORES {1 - (age/7)}
		//INSERT THIS NEW INFO TO OUR MAIN DATA STRUCTURE
		groupedSuccesses.forall(function(s){
			var test_group = tests[s._group_id];
			test_group.last_good = s.last_good;
			test_group.details.extend(s.details);
			test_group.success_count = s.success_count;

			var is_done = {};  //THERE ARE SUBTESTS IN THE details, ONLY COUNT THE TEST ONCE
			var failure_score = 0;
			var total_score = 0;
			test_group.details.forall(function(d){
				if (!is_done[d._id]) {
					is_done[d._id] = true;
					var score = 1 - NOW.subtract(Date.newInstance(d.build_date)).divideBy(Duration.DAY) / 7;
					if (score < 0) score = 0;
					if (!d.ok) {
						failure_score += score;
					}//endif
					total_score += score;
				}//endif
			});
			test_group.total_score = total_score;
			test_group.failure_score = failure_score;
			if (total_score != 0) {
				test_group.score = failure_score / total_score;
			}//endif

			$("#" + FAIL_PREFIX + test_group.rownum+" td")
				.first()
				.html("" + aMath.round(test_group.score*100, {digits: 2}));
		});
	} finally {
		Log.actionDone(a);
	}//try
	yield (null);
};//function


var addGroupId = function(g, edges){
	g.data.forall(function(t){
		t._group_id = edges.map(function(e){
			return t[e];
		}).join("::");
	});
};


