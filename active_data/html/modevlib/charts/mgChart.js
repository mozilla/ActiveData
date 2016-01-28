/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("../../lib/metrics-graphics/import.js");
importScript("../../lib/jquery.js");




(function(){
	var DEBUG = true;


	window.aChart = window.aChart || {};

	////////////////////////////////////////////////////////////////////////////
	// STYLES
	////////////////////////////////////////////////////////////////////////////
	var STYLES = [
		{"color": "#1f77b4"},
		{"color": "#ff7f0e"},
		{"color": "#2ca02c"},
		{"color": "#d62728"},
		{"color": "#9467bd"},
		{"color": "#8c564b"},
		{"color": "#e377c2"},
		{"color": "#7f7f7f"},
		{"color": "#bcbd22"},
		{"color": "#17becf"}
	];


	/*
	 * SCATTER USES THE `select` COLUMNS FOR x AND y RESPECTIVELY
	 * THE FIRST EDGE IS USED FOR CATEGORIES
	 */
	aChart.showScatter = function(params){
		Map.expecting(params, ["target", "data"]);

		var data;
		if (params.data instanceof Array) {
			data = params.data;
		} else if (params.data.meta) {
			//ASSUME Qb QUERY RESULT
			var chartCube = params.data;
			if (!(chartCube.select instanceof Array) || chartCube.select.length != 2) {
				Log.error("Expecting `select` clause to have two columns");
			}//endif

			////////////////////////////////////////////////////////////////////////////
			// SERIES (ONLY IF MORE THAN ONE EDGE)
			////////////////////////////////////////////////////////////////////////////
			var xaxis = chartCube.select[0];
			var yaxis = chartCube.select[1];

			var styles = Map.clone(STYLES);

			if (chartCube.edges.length >= 1) {
				chartCube.edges[0].domain.partitions.forall(function(p, i){
					if (p.style !== undefined) styles[i] = Map.setDefault(p.style, styles[i]);
				});
			}//endif

			var edge0 = chartCube.edges[0];
			var parts0 = edge0.domain.partitions;
			parts0.forall(function(p, i){p.dataIndex=i});  //FIX THE LACK OF dataIndex

			if (Map.get(params, "data.meta.format") == "list") {
				data = params.data.data;
				data.forall(function(v){
					v["_colorIndex"]=v[edge0.name].dataIndex;
				});
			} else {
				var cube = Map.zip(Map.map(chartCube.data, function(k, v){
					return [k, new Matrix({"data": v})];
				}));
				var canonical = Map.values(cube)[0];
				//CONVERT cube TO ARRAY OF OBJECTS
				//EDGE PARTS ARE STILL IN OBJECT FORM
				data = new Matrix(canonical).map(function(_, c){
					var output = {};
					Map.forall(cube, function(columnName, m){
						output[columnName] = m.get(c);
						output[edge0.name] = parts0[c[0]];
						output["_colorIndex"] = c[0];
					});
					return output;
				});
			}//endif
		}//endif

		var height = coalesce(Map.get(params, "style.height"), $('#' + params.target).height(), 400);
		var width = coalesce(Map.get(params, "style.width"), $('#' + params.target).width(), 300);

		var x_accessor = coalesce(Map.get(params, "axis.x.value"), xaxis.name);
		var y_accessor = coalesce(Map.get(params, "axis.y.value"), yaxis.name);


		//POPUP FORMATTING
		var format = Map.get(params, "hover.format");
		var mouseover;
		var y_rollover_format;
		var x_rollover_format;
		if (format && isString()) {
			mouseover = function(d, i){
				d3
				.select('#' + params.target + ' svg .mg-active-datapoint')
				.text(new Template(format).expand(d.point));
			};
		}else if (format) {
			if (!format.x) format.x="{{.}}";
			x_rollover_format =(function(t){
				return function(d){
					return t.expand(d);
				};
			})(new Template(format.x));

			if (!format.y) format.y="{{.}}";
			y_rollover_format = (function(t){
				return function(d){
					return t.expand(d);
				};
			})(new Template(format.y));

			mouseover = function(d, i){
				var point = d.point;
				var text = x_rollover_format(Map.get(point, x_accessor))+" "+y_rollover_format(Map.get(point, y_accessor));
				d3.select('#' + params.target + ' svg .mg-active-datapoint').text(text);
			};
		}else{
			//AUTO FORMAT
		}//endif

		//X-AXIS FORMAT
		var xax_format=Map.get(params, "axis.x.format");
		if (xax_format){
			xax_format = (function(t){
				return function(d){
					return t.expand(d);
				};
			})(new Template(xax_format));
		}//endif

		if (DEBUG){
			Log.note(convert.value2json(data));
		}//endif
		var chartParams = {
			title: Map.get(params, "title.label"),
			description: Map.get(params, "title.description"),
			data: data,
			chart_type: 'point',
			width: width,
			height: height,
			right: coalesce(Map.get(params, "area.style.padding.right"), 10),
			target: '#' + params.target,
			xax_format: xax_format,
			x_accessor: x_accessor,
			y_accessor: y_accessor,
			color_accessor: "_colorIndex",
			color_domain: styles.map(function(v, i){
				return i;
			}),
			color_range: styles.map(function(v){
				return v.color;
			}),
			color_type: 'category',
			x_rug: Map.get(params, "axis.x.rug"),
			mouseover: mouseover
		};
		MG.data_graphic(chartParams);

	};


	aChart.show = function(params){




	};

})();
