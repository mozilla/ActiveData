/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript([
	"../../lib/jquery.js",
	"../../lib/jquery-ui/js/jquery-ui-1.10.2.custom.js",
	"../../lib/jquery-ui/css/start/jquery-ui-1.10.2.custom.css",
	"../../lib/jquery.ba-bbq/jquery.ba-bbq.js",
	"../../lib/jquery-linedtextarea/jquery-linedtextarea.css",
	"../../lib/jquery-linedtextarea/jquery-linedtextarea.js",
	"../../lib/jsonlint/jsl.parser.js",
	"../../lib/jsonlint/jsl.format.js"
]);

importScript("Filter.js");
importScript("ComponentFilter.js");
importScript("ProductFilter.js");
importScript("ProgramFilter.js");
importScript("PartitionFilter.js");
importScript("TeamFilter.js");
importScript("RadioFilter.js");

importScript("../threads/thread.js");
importScript("../qb/aCompiler.js");
importScript("../aFormat.js");


////////////////////////////////////////////////////////////////////////////////
// SYNCHRONIZES THREE VARIABLES SO THEY ARE ALL THE SAME.  CHANGING ONE IMMEDIATELY
// CHANGES THE OTHER TWO, AND A GUI.refresh() IS THEN PERFORMED.
//
// * URL PARAMETER - AS SEEN IN THE ANCHOR (#) PARAMETER LIST
// * FORM VALUE - CURRENT VALUE SHOWN IN THE HTML FORM ELEMENTS
// * GUI.state - VARIABLE VALUES IN THE GUI.state OBJECT

GUI = {};
(function () {
		GUI.state = {};
		GUI.customFilters = [];

		$(".loading").hide();

		Thread.showWorking = function(numThread) {
			var l = $(".loading");
			l.show();
		};//function

		Thread.hideWorking = function() {
			var l = $(".loading");
			l.hide();
		};//function


		////////////////////////////////////////////////////////////////////////////////
		// GIVEN THE THREE, RETURN AN END DATE THAT WILL MAKE THE LAST PARTITION
		// INCLUDE A WHOLE INTERVAL, AND IS *INSIDE* THAT INTERVAL
		////////////////////////////////////////////////////////////////////////////////
		GUI.fixEndDate = function (startDate, endDate, interval) {
			startDate = Date.newInstance(startDate);
			endDate = Date.newInstance(endDate);
			interval = Duration.newInstance(interval);

			var diff = endDate.add(interval).subtract(startDate, interval);

			var newEnd = startDate.add(diff.floor(interval));
			return newEnd.addSecond(-1);
		};


		////////////////////////////////////////////////////////////////////////////////
		// SETUP THE VARIOUS GUI PARAMETER INPUTS
		////////////////////////////////////////////////////////////////////////////////
		GUI.setup = function (
			refreshChart,  //FUNCTION THAT WILL BE CALLED WHEN ANY PARAMETER CHANGES
			parameters,    //LIST OF PARAMETERS (see GUI.AddParameters FOR DETAILS)
			relations,     //SOME RULES TO APPLY TO PARAMETERS, IN CASE THE HUMAN MAKES SMALL MISTAKES
			indexName,     //PERFORM CHECKS ON THIS INDEX
			showDefaultFilters,  //SHOW THE Product/Compoentn/Team FILTERS
			performChecks,       //PERFORM SOME CONSISTENCY CHECKS TOO
			checkLastUpdated     //SEND QUERY TO GET THE LAST DATA?
		) {

			GUI.performChecks=coalesce(performChecks, true);
			GUI.checkLastUpdated=coalesce(checkLastUpdated, true);

			if (typeof(refreshChart) != "function") {
				Log.error("Expecting first parameter to be a refresh (creatChart) function");
			}//endif
			GUI.refreshChart = refreshChart;
			GUI.pleaseRefreshLater = coalesce(GUI.pleaseRefreshLater, false);

			//IF THERE ARE ANY CUSTOM FILTERS, THEN TURN OFF THE DEFAULTS
			var isCustom = false;
			parameters.forall(function (f, i) {
				if (f.type.isFilter) {
					isCustom = true;

					f.type.setSimpleState(f["default"]);
					f.type.name = f.name;
				}
			});

			if (((showDefaultFilters === undefined) && !isCustom) || showDefaultFilters) {
				//USE DEFAULT FILTERS
				GUI.state.programFilter = new ProgramFilter();
				GUI.state.productFilter = new ProductFilter();
				GUI.state.componentFilter = new ComponentFilter();

				GUI.customFilters.push(GUI.state.programFilter);
				GUI.customFilters.push(GUI.state.productFilter);
				GUI.customFilters.push(GUI.state.componentFilter);
			}//endif

			GUI.showLastUpdated(indexName);
			GUI.AddParameters(parameters, relations); //ADD PARAM AND SET DEFAULTS
			GUI.Parameter2State();			//UPDATE STATE OBJECT WITH THOSE DEFAULTS

			GUI.makeSelectionPanel();

			GUI.relations = coalesce(relations, []);
			GUI.FixState();

			GUI.URL2State();				//OVERWRITE WITH URL PARAM
			GUI.State2URL.isEnabled = true;	//DO NOT ALLOW URL TO UPDATE UNTIL WE HAVE GRABBED IT

			GUI.FixState();
			GUI.State2URL();
			GUI.State2Parameter();

			if (!GUI.pleaseRefreshLater) {
				//SOMETIMES SETUP NEEDS TO BE DELAYED A BIT MORE
				GUI.refresh();
			}//endif
		};

		var esHasErrorInIndex;

		//SHOW THE LAST TIME ES WAS UPDATED
		GUI.showLastUpdated = function(indexName) {
			if (!GUI.checkLastUpdated) return;

			Thread.run("show last updated timestamp", function*() {
				var time;

				if (indexName === undefined || indexName == null || indexName == "bugs") {
					var result = yield (ESQuery.run({
						"from": "bugs",
						"select": {"name": "max_date", "value": "modified_ts", "aggregate": "maximum"},
						"esfilter": {"range": {"modified_ts": {"gte": Date.eod().addMonth(-1).getMilli()}}}
					}));

					time = new Date(result.cube.max_date);
					var tm = $("#testMessage");
					tm.html(new Template("<div style={{style|style}}>{{name}}</div>").expand(result.index));
					tm.append("<br>ES Last Updated " + time.addTimezone().format("NNN dd @ HH:mm") + Date.getTimezone());
				} else if (indexName == "reviews") {
                    var result = yield (ESQuery.run({
                        "from": "reviews",
                        "select": [
                            {"name": "last_request", "value": "request_time", "aggregate": "maximum"}
                        ]
                    }));
                    time = Date.newInstance(result.cube.last_request);
                    $("#testMessage").html("Reviews Last Updated " + time.addTimezone().format("NNN dd @ HH:mm") + Date.getTimezone());
				} else if (indexName == "bug_tags") {
					esHasErrorInIndex = false;
					time = yield (BUG_TAGS.getLastUpdated());
					$("#testMessage").html("Bugs Last Updated " + time.addTimezone().format("NNN dd"));
				} else if (indexName == "bug_summary") {
					esHasErrorInIndex = false;
					time = new Date((yield(ESQuery.run({
						"from": "bug_summary",
						"select": {"name": "max_date", "value": "modified_time", "aggregate": "maximum"}
					}))).cube.max_date);
					$("#testMessage").html("Bug Summaries Last Updated " + time.addTimezone().format("NNN dd @ HH:mm") + Date.getTimezone());
				} else if (indexName == "datazilla") {
					esHasErrorInIndex = false;
					time = new Date((yield(ESQuery.run({
						"from": "talos",
						"select": {"name": "max_date", "value": "testrun.date", "aggregate": "maximum"}
					}))).cube.max_date);
					$("#testMessage").html("Datazilla Last Updated " + time.addTimezone().format("NNN dd @ HH:mm") + Date.getTimezone());
				} else if (indexName == "perfy") {
					esHasErrorInIndex = false;
					time = new Date((yield(ESQuery.run({
						"from": "perfy",
						"select": {"name": "max_date", "value": "info.started", "aggregate": "maximum"}
					}))).cube.max_date);
					$("#testMessage").html("Perfy Last Updated " + time.addTimezone().format("NNN dd @ HH:mm") + Date.getTimezone());
				}else if (indexName == "talos"){
					esHasErrorInIndex = false;
					time = new Date((yield(ESQuery.run({
						"from":"talos",
						"select":{"name": "max_date", "value":"testrun.date","aggregate":"maximum"}
					}))).cube.max_date);
					$("#testMessage").html("Latest Push " + time.addTimezone().format("NNN dd @ HH:mm") + Date.getTimezone());
				} else {
					return;
				}//endif

				var age = aMath.round(Date.now().subtract(time).divideBy(Duration.DAY), 1);
				if (age > 1 || esHasErrorInIndex) {
					GUI.bigWarning("#testMessage", aMath.max(3, aMath.floor(age)));
				}//endif

				if (GUI.performChecks && esHasErrorInIndex === undefined) {
					esHasErrorInIndex = yield(GUI.corruptionCheck());
					if (esHasErrorInIndex) {
						$("#testMessage").append("<br>ES IS CORRUPTED!!!");
					}//endif
				}//endif
			});
		};//method


		GUI.corruptionCheck = function*() {
			if (ESQuery.INDEXES.bugs.host.startsWith("https://esfrontline")) return;  //PUBLIC CLUSTER DOES NOT HAVE SCRIPTING

			var t = aTimer.start("Corruption Check");

			var result = yield (ESQuery.run({
				"from": "bugs",
				"select": {"name": "num_null", "value": "expires_on>" + Date.eod().getMilli() + " ? 1 : 0", "aggregate": "add"},
				"edges": ["bug_id"],
				"esfilter": {"range": {"modified_ts": {"gte": Date.now().addMonth(-3).getMilli()}}}
			}));

			var is_error = yield (Q({
				"from": {
					"from": result,
					"select": [
						{"value": "bug_id"}
					],
					"where": "num_null!=1"
				},
				"select": {"name": "is_error", "value": "bug_id", "aggregate": "exists"}
			}));

			t.stop();

			yield (is_error.cube.is_error == 1)
		};//method


		GUI.bigWarning = function (elem, blinkCount) {
			$(elem).addClass("warning").effect("pulsate", { times: blinkCount }, 500 * blinkCount);
		};


		GUI.urlMap = {"%": "%25", "{": "%7B", "}": "%7D", "[": "%5B", "]": "%5D"};

		GUI.State2URL = function () {
			if (!GUI.State2URL.isEnabled) return;

			var simplestate = {};
			Map.forall(GUI.state, function (k, v) {

				var p = GUI.parameters.map(function (v, i) {
					if (v.id == k) return v;
				})[0];

				if (v.isFilter) {
					simplestate[k] = v.getSimpleState();
				} else if (jQuery.isArray(v)) {
					if (v.length > 0) simplestate[k] = v.join(",");
				} else if (p && p.type == "json") {
					v = convert.value2json(v);
					v = v.escape(GUI.urlMap);
					simplestate[k] = v;
				} else if (typeof(v) == "string" || aMath.isNumeric(k)) {
					v = v.escape(GUI.urlMap);
					simplestate[k] = v;
				}//endif
			});

			{//bbq REALY NEEDS TO KNOW WHAT ATTRIBUTES TO REMOVE FROM URL
				var removeList = [];
				var keys = Object.keys(simplestate);
				for (var i = keys.length; i--;) {
					var key = keys[i];
					var val = simplestate[key];
					if (val === undefined) removeList.push(key);
				}//for

				jQuery.bbq.removeState(removeList);
				jQuery.bbq.pushState(Map.copy(simplestate));
			}

		};

		GUI.State2URL.isEnabled = false;


		GUI.URL2State = function () {
			var urlState = jQuery.bbq.getState();
			Map.forall(urlState, function (k, v) {
				if (GUI.state[k] === undefined) return;

				var p = GUI.parameters.map(function (v, i) {
					if (v.id == k) return v;
				})[0];

				if (p && Qb.domain.ALGEBRAIC.contains(p.type)) {
					v = v.escape(Map.inverse(GUI.urlMap));
					GUI.state[k] = v;
				} else if (p && p.type == "json") {
					try {
						v = v.escape(Map.inverse(GUI.urlMap));
						GUI.state[k] = convert.json2value(v);
					} catch (e) {
						Log.error("Malformed JSON: " + v);
					}//try
				} else if (p && p.type == "text") {
					v = v.escape(Map.inverse(GUI.urlMap));
					GUI.state[k] = v;
				} else if (p && p.type == "code") {
					v = v.escape(Map.inverse(GUI.urlMap));
					GUI.state[k] = v;
				} else if (GUI.state[k].isFilter) {
					GUI.state[k].setSimpleState(v);
				} else {
					GUI.state[k] = v.split(",");
				}//endif
			});
		};



		///////////////////////////////////////////////////////////////////////////
		// ADD INTERACTIVE PARAMETERS TO THE PAGE
		// id - id of the html form element (can exist, or not), also used as GUI.state variable
		// name - humane name of the parameter
		// type - some basic data types to drive the type of form element used (time, date, datetime, duration, text, boolean, json, code)
		// default - default value if not specified in URL
		///////////////////////////////////////////////////////////////////////////
		GUI.AddParameters = function (parameters, relations) {
			//KEEP SIMPLE PARAMETERS GUI.parameters AND REST IN customFilters
			GUI.parameters = parameters.map(function (param) {
				if (param.type.isFilter) {
					GUI.state[param.id] = param.type;
					if (param.name) param.type.name = param.name;
					param.type.setSimpleState(param["default"]);
					GUI.customFilters.push(param.type);
				} else {
					return param;
				}//endif
			});


			//INSERT HTML
			var template = new Template('<span class="parameter_name">{{NAME}}</span><input type="{{TYPE}}" id="{{ID}}"><br><br>\n');
			var html = "";
			GUI.parameters.forEach(function (param) {
				if ($("#" + param.id).length > 0) return;

				//SIMPLE VARIABLES
				html += template.replace({
					"ID": param.id,
					"NAME": param.name,
					"TYPE": {
						"time": "text",
						"date": "text",
						"duration": "text",
						"text": "text",
						"boolean": "checkbox",
						"json": "textarea",
						"code": "textarea"
					}[param.type]  //MAP PARAMETER TYPES TO HTML TYPES
				});
			});
			$("#parameters").html(html);


			//MARKUP PARAMETERS
			parameters.forEach(function (param) {
				var defaultValue = param["default"];


				////////////////////////////////////////////////////////////////////////
				// DATETIME
				if (param.type == "datetime") {
					$("#" + param.id).change(function () {
						try {
							$("#" + $(this).id + "_error").html("");
							var value = Date.newInstance($(this).val());
							$(this).val(value.format("yyyy-MM-dd HH:mm:ss"));
						} catch (e) {
							$("#" + $(this).id + "_error").html("&gt;- ERROR, expecting a valid date and time");
							return;
						}//try

						if (GUI.UpdateState()) {
							GUI.refreshChart();
						}
					});
					defaultValue = Date.newInstance(defaultValue).format("yyyy-MM-dd HH:mm:ss");
					$("#" + param.id).val(defaultValue);
					////////////////////////////////////////////////////////////////////////
					// DATE
				} else if (param.type == "date" || param.type == "time") {
					$("#" + param.id).datepicker({ });
					$("#" + param.id).datepicker("option", "dateFormat", "yy-mm-dd");

					$("#" + param.id).change(function () {
						if (GUI.UpdateState()) {
							GUI.refreshChart();
						}
					});
					defaultValue = defaultValue.format("yyyy-MM-dd");
					$("#" + param.id).val(defaultValue);
					////////////////////////////////////////////////////////////////////////
					// DURATION
				} else if (param.type == "duration") {
					$("#" + param.id).change(function () {
						try {
							$("#" + $(this).id + "_error").html("");
							var value = Duration.newInstance($(this).text());
							$(this).text(value.toString());
						} catch (e) {
							$("#" + $(this).id + "_error").html("&gt;- ERROR, expecting a valid duration");
							return;
						}//try

						if (GUI.UpdateState()) {
							GUI.refreshChart();
						}
					});
					defaultValue = defaultValue.toString();
					$("#" + param.id).val(defaultValue);
					////////////////////////////////////////////////////////////////////////
					// BINARY
				} else if (param.type == "boolean") {
					$("#" + param.id).change(function () {
						if (GUI.UpdateState()) {
							GUI.refreshChart();
						}
					});
					$("#" + param.id).prop('checked', defaultValue);
					////////////////////////////////////////////////////////////////////////
					// JSON
				} else if (param.type == "json") {
					var codeDiv = $("#" + param.id);
					codeDiv.linedtextarea();
					codeDiv.change(function () {
						if (this.isChanging) return;
						this.isChanging = true;
						try {
							codeDiv = $("#" + param.id);	//JUST TO BE SURE WE GOT THE RIGHT ONE
							//USE JSONLINT TO FORMAT AND TEST-COMPILE THE code
							var code = jsl.format.formatJson(codeDiv.val());
							codeDiv.val(code);
							jsl.parser.parse(code);
							//TIGHTER PACKING IF JSON
							codeDiv.val(aFormat.json(code));

							if (GUI.UpdateState()) {
								GUI.refreshChart();
							}//endif
						} catch (e) {
							Log.alert(e.message);
						}//try
						this.isChanging = false;
					});
					codeDiv.val(aFormat.json(defaultValue));
				} else if (param.type == "code") {
					var codeDiv = $("#" + param.id);
					codeDiv.linedtextarea();
					codeDiv.change(function () {
						if (GUI.UpdateState()) {
							GUI.refreshChart();
						}
					});
					codeDiv.val(defaultValue);
				} else {
					if (param.type == "string") param.type = "text";
					$("#" + param.id).change(function () {
						if (GUI.UpdateState()) {
							GUI.refreshChart();
						}
					});
					$("#" + param.id).val(defaultValue);
				}//endif

			});//for


		};//method


		//RETURN TRUE IF ANY CHANGES HAVE BEEN MADE
		GUI.State2Parameter = function () {
			GUI.parameters.forEach(function (param) {

				if (param.type == "json") {
					$("#" + param.id).val(convert.value2json(GUI.state[param.id]));
				} else if (param.type == "boolean") {
					$("#" + param.id).prop("checked", GUI.state[param.id]);
				} else if (param.type == "datetime") {
					$("#" + param.id).val(Date.newInstance(GUI.state[param.id]).format("yyyy-MM-dd HH:mm:ss"))
				} else {
				//if (param.type.getSimpleState) return;  //param.type===GUI.state[param.id] NO ACTION REQUIRED
					$("#" + param.id).val(GUI.state[param.id]);
				}//endif
			});
		};


		//RETURN TRUE IF ANY CHANGES HAVE BEEN MADE
		GUI.Parameter2State = function () {
			GUI.parameters.forEach(function (param) {
				if (param.type == "json") {
					GUI.state[param.id] = convert.json2value($("#" + param.id).val());
				} else if (param.type == "boolean") {
					GUI.state[param.id] = $("#" + param.id).prop("checked");
				} else {
					GUI.state[param.id] = $("#" + param.id).val();
				}//endif
			});
		};


		//RETURN TRUE IF ANY CHANGES HAVE BEEN MADE
		GUI.UpdateState = function () {
			var backup = Map.copy(GUI.state);

			GUI.Parameter2State();
			GUI.FixState();
			GUI.State2Parameter();

			//AFTER RELATIONS, IS THERE STILL A CHANGE?
			var changeDetected = false;
			GUI.parameters.forall(function (param) {
				if (backup[param.id] === undefined || GUI.state[param.id] != backup[param.id]) {
					changeDetected = true;
				}//endif
			});

			//PUSH BACK CHANGES IN STATE TO GUI PARAMETERS
			if (changeDetected) GUI.State2URL();
			return changeDetected;
		};

		//APPLY THE RELATIONS TO STATE
		GUI.FixState = function () {
			if (GUI.relations.length > 0) {
				//COMPILE RELATIONS
				var type = typeof(GUI.relations[0]);
				if (type != "function") {
					GUI.relations.forall(function (r, i) {
						GUI.relations[i] = aCompile.method_usingObjects(r, [GUI.state]);
					});
				}//endif

				//UPDATE THE STATE OBJECT
				GUI.relations.forall(function (r) {
					r(GUI.state);
				});
			}//endif
		};


		GUI.makeSelectionPanel = function () {
			if (GUI.customFilters.length == 0) return;

			var html = "";

			html += '<h4><a href="#">Selection Summary</a></h4>';
			html += '<div id="summary"></div>';
			GUI.customFilters.forall(function (f, i) {
				html += '<h4><a href="#">' + f.name + '</a></h4>';
				html += f.makeHTML();
			});

			$("#filters").html(html);

			$("#filters").accordion({
				heightStyle: "content",
				navigation: true,
				collapsible: true
			});
		};


		GUI.UpdateSummary = function () {
			var html = "<br>";

			GUI.customFilters.forall(function (f, i) {
				html += "<b>" + f.getSummary() + "</b>";
				html += "<br><br>";
			});


			html += "<b>Hold CTRL while clicking to multi-select and deselect from the lists below.</b>";

			$("#summary").html(html);
		};

		GUI.refreshRequested = false;	//TRY TO AGGREGATE MULTIPLE refresh() REQUESTS INTO ONE

		GUI.refresh = function (refresh) {
			if (GUI.refreshRequested) return;
			GUI.refreshRequested = true;

			Thread.run("refresh gui", function*() {
				yield (Thread.sleep(200));
				GUI.refreshRequested = false;

				GUI.State2URL();

				var threads = [];
				GUI.customFilters.forall(function (f, i) {
					var t = Thread.run(function*() {
						yield (f.refresh());
					});
					t.name = GUI.customFilters[i].name;
					threads.push(t);
				});

				for (var i = 0; i < threads.length; i++) {
					yield (Thread.join(threads[i]));
				}//for

				GUI.UpdateSummary();

				if (refresh===undefined || refresh==true) GUI.refreshChart();
			});
		};


		GUI.injectFilterss = function (chartRequests) {
			if (!(chartRequests instanceof Array))
				Log.error("Expecting an array of chartRequests");
			for (var i = 0; i < chartRequests.length; i++) {
				(GUI.injectFilters(chartRequests[i]));
			}//for
		};

		GUI.injectFilters = function (chartRequest) {
			if ((chartRequest instanceof Array))
				Log.error("Expecting a chartRequest");

			if (chartRequest.esQuery === undefined)
				Log.error("Expecting chart requests to have a \"esQuery\", not \"query\"");


			var indexName;
			if (chartRequest.query !== undefined && chartRequest.query.from !== undefined) {
				indexName = chartRequest.query.from;
			} else {
				indexName = "reviews";
			}//endif

			GUI.customFilters.forall(function (f, i) {
				ElasticSearch.injectFilter(chartRequest.esQuery, f.makeFilter());
			});
		};


		GUI.getFilters = function (indexName) {
			if (GUI.customFilters.length == 0) return ESQuery.TrueFilter;

			var output = {"and": []};
			GUI.customFilters.forall(function (f, i) {
				if (f.makeFilter){
					output.and.push(f.makeFilter());
				}//endif
			});
			return output;
	};


})();

