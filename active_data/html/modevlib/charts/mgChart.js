/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("../../lib/metrics-graphics/import.js");
importScript("../../lib/jquery.js");

window.aChart = window.aChart || {};


(function(){
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
		}else{
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

			var styles = deepcopy(STYLES);

			if (chartCube.edges.length >= 1) {
				chartCube.edges[0].domain.partitions.forall(function(p, i){
					if (p.style !== undefined) styles[i] = Map.setDefault(p.style, styles[i]);
				});
			}//endif


			var cube = Map.zip(Map.map(chartCube.data, function(k, v){return [k, new Matrix({"data": v})];}));
			var canonical = Map.values(cube)[0];
			var edge0=chartCube.edges[0];
			var parts0=edge0.domain.partitions;
			//CONVERT cube TO ARRAY OF OBJECTS
			//EDGE PARTS ARE STILL IN OBJECT FORM
			data = new Matrix(canonical).map(function(_, c){
				var output={};
				Map.forall(cube, function(columnName, m){
					output[columnName] = m.get(c);
					output[edge0.name] = parts0[c[0]];
					output["_colorIndex"]=c[0];
				});
				return output;
			});

		}//endif

		var height = coalesce(Map.get(params, "style.height"), $('#'+params.target).height(), 400);
		var width = coalesce(Map.get(params, "style.width"), $('#'+params.target).width(), 300);

		var chartParams = {
			title: Map.get(params, "title.label"),
			description: Map.get(params, "title.description"),
			data: data,
			chart_type: 'point',
			width: width,
			height: height,
			right: coalesce(Map.get(params, "area.style.padding.right"), 10),
			target: '#'+params.target,
			xax_format: Map.get(params, "axis.x.format"),
			x_accessor: coalesce(Map.get(params, "axis.x.value"), xaxis.name),
			y_accessor: coalesce(Map.get(params, "axis.y.value"), yaxis.name),
			color_accessor: "_colorIndex",
			color_domain: styles.map(function(v, i){return i;}),
			color_range: styles.map(function(v){return v.style;}),
			color_type: 'category',
			x_rug: Map.get(params, "axis.x.rug")
		};
		MG.data_graphic(chartParams);

	};


	function deepcopy(value){
		return convert.json2value(convert.value2json(value));
	}//function
})();
