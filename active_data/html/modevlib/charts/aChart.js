/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("../math/aMath.js");
importScript([
	"../../lib/jquery.js",
	"../../lib/jquery.numberformatter.js",

	"../../lib/ccc/lib/protovis-d3.3.js",
	"../../lib/ccc/lib/protovis-msie.js",

	"../../lib/ccc/lib/jquery.tipsy.js",
	"../../lib/ccc/lib/tipsy.js",
	"../../lib/ccc/lib/tipsy.css",

	"../../lib/ccc/def/def.js",

	"../../lib/ccc/pvc/pvc.js",
	"../../lib/ccc/pvc/pvc.text.js",
	"../../lib/ccc/pvc/pvc.color.js",
	"../../lib/ccc/pvc/pvc.trends.js",
	"../../lib/ccc/pvc/pvc.options.js",
	"../../lib/ccc/pvc/data/_data.js",
	"../../lib/ccc/pvc/data/meta/DimensionType.js",
	"../../lib/ccc/pvc/data/meta/ComplexType.js",
	"../../lib/ccc/pvc/data/meta/ComplexTypeProject.js",
	"../../lib/ccc/pvc/data/translation/TranslationOper.js",
	"../../lib/ccc/pvc/data/translation/MatrixTranslationOper.js",
	"../../lib/ccc/pvc/data/translation/CrosstabTranslationOper.js",
	"../../lib/ccc/pvc/data/translation/RelationalTranslationOper.js",
	"../../lib/ccc/pvc/data/Atom.js",
	"../../lib/ccc/pvc/data/Complex.js",
	"../../lib/ccc/pvc/data/ComplexView.js",
	"../../lib/ccc/pvc/data/Datum.js",
	"../../lib/ccc/pvc/data/Dimension.js",
	"../../lib/ccc/pvc/data/Data.js",
	"../../lib/ccc/pvc/data/Data.selected.js",
	"../../lib/ccc/pvc/data/GroupingSpec.js",
	"../../lib/ccc/pvc/data/DataOper.js",
	"../../lib/ccc/pvc/data/GroupingOper.js",
	"../../lib/ccc/pvc/data/LinearInterpolationOper.js",
	"../../lib/ccc/pvc/data/LinearInterpolationOperSeriesState.js",
	"../../lib/ccc/pvc/data/ZeroInterpolationOper.js",
	"../../lib/ccc/pvc/data/ZeroInterpolationOperSeriesState.js",
	"../../lib/ccc/pvc/data/Data.operations.js",
	"../../lib/ccc/pvc/data/Data.compat.js",
	"../../lib/ccc/pvc/visual/Role.js",
	"../../lib/ccc/pvc/visual/Scene.js",
	"../../lib/ccc/pvc/visual/Var.js",
	"../../lib/ccc/pvc/visual/sign/BasicSign.js",
	"../../lib/ccc/pvc/visual/sign/Sign.js",
	"../../lib/ccc/pvc/visual/sign/Panel.js",
	"../../lib/ccc/pvc/visual/sign/Label.js",
	"../../lib/ccc/pvc/visual/sign/Dot.js",
	"../../lib/ccc/pvc/visual/sign/Line.js",
	"../../lib/ccc/pvc/visual/sign/Area.js",
	"../../lib/ccc/pvc/visual/sign/Bar.js",
	"../../lib/ccc/pvc/visual/sign/PieSlice.js",
	"../../lib/ccc/pvc/visual/sign/Rule.js",
	"../../lib/ccc/pvc/visual/Context.js",
	"../../lib/ccc/pvc/visual/OptionsBase.js",
	"../../lib/ccc/pvc/visual/Axis.js",
	"../../lib/ccc/pvc/visual/CartesianAxis.js",
	"../../lib/ccc/pvc/visual/CartesianAxisRootScene.js",
	"../../lib/ccc/pvc/visual/CartesianAxisTickScene.js",
	"../../lib/ccc/pvc/visual/CartesianFocusWindow.js",
	"../../lib/ccc/pvc/visual/ColorAxis.js",
	"../../lib/ccc/pvc/visual/SizeAxis.js",
	"../../lib/ccc/pvc/visual/Legend.js",
	"../../lib/ccc/pvc/visual/legend/BulletRootScene.js",
	"../../lib/ccc/pvc/visual/legend/BulletGroupScene.js",
	"../../lib/ccc/pvc/visual/legend/BulletItemScene.js",
	"../../lib/ccc/pvc/visual/legend/BulletItemSceneSelection.js",
	"../../lib/ccc/pvc/visual/legend/BulletItemSceneVisibility.js",
	"../../lib/ccc/pvc/visual/legend/BulletItemRenderer.js",
	"../../lib/ccc/pvc/visual/legend/BulletItemDefaultRenderer.js",
	"../../lib/ccc/pvc/visual/plot/Plot.js",
	"../../lib/ccc/pvc/visual/plot/CartesianPlot.js",
	"../../lib/ccc/pvc/visual/plot/CategoricalPlot.js",
	"../../lib/ccc/pvc/visual/plot/BarPlotAbstract.js",
	"../../lib/ccc/pvc/visual/plot/BarPlot.js",
	"../../lib/ccc/pvc/visual/plot/NormalizedBarPlot.js",
	"../../lib/ccc/pvc/visual/plot/WaterfallPlot.js",
	"../../lib/ccc/pvc/visual/plot/PointPlot.js",
	"../../lib/ccc/pvc/visual/plot/MetricXYPlot.js",
	"../../lib/ccc/pvc/visual/plot/MetricPointPlot.js",
	"../../lib/ccc/pvc/visual/plot/PiePlot.js",
	"../../lib/ccc/pvc/visual/plot/HeatGridPlot.js",
	"../../lib/ccc/pvc/visual/plot/BoxPlot.js",
	"../../lib/ccc/pvc/visual/plot/BulletPlot.js",
	"../../lib/ccc/pvc/pvcAbstract.js",
	"../../lib/ccc/pvc/pvcBaseChart.js",
	"../../lib/ccc/pvc/pvcBaseChart.visualRoles.js",
	"../../lib/ccc/pvc/pvcBaseChart.data.js",
	"../../lib/ccc/pvc/pvcBaseChart.plots.js",
	"../../lib/ccc/pvc/pvcBaseChart.axes.js",
	"../../lib/ccc/pvc/pvcBaseChart.panels.js",
	"../../lib/ccc/pvc/pvcBaseChart.selection.js",
	"../../lib/ccc/pvc/pvcBaseChart.extension.js",
	"../../lib/ccc/pvc/pvcBasePanel.js",
	"../../lib/ccc/pvc/pvcPlotPanel.js",
	"../../lib/ccc/pvc/pvcMultiChartPanel.js",
	"../../lib/ccc/pvc/pvcTitlePanelAbstract.js",
	"../../lib/ccc/pvc/pvcTitlePanel.js",
	"../../lib/ccc/pvc/pvcLegendPanel.js",
	"../../lib/ccc/pvc/pvcCartesianAbstract.js",
	"../../lib/ccc/pvc/pvcGridDockingPanel.js",
	"../../lib/ccc/pvc/pvcCartesianGridDockingPanel.js",
	"../../lib/ccc/pvc/pvcCartesianAbstractPanel.js",
	"../../lib/ccc/pvc/pvcPlotBgPanel.js",
	"../../lib/ccc/pvc/pvcCategoricalAbstract.js",
	"../../lib/ccc/pvc/pvcCategoricalAbstractPanel.js",
	"../../lib/ccc/pvc/pvcAxisPanel.js",
	"../../lib/ccc/pvc/pvcAxisTitlePanel.js",
	"../../lib/ccc/pvc/pvcPiePanel.js",
	"../../lib/ccc/pvc/pvcPieChart.js",
	"../../lib/ccc/pvc/pvcBarAbstractPanel.js",
	"../../lib/ccc/pvc/pvcBarAbstract.js",
	"../../lib/ccc/pvc/pvcBarPanel.js",
	"../../lib/ccc/pvc/pvcBarChart.js",
	"../../lib/ccc/pvc/pvcNormalizedBarPanel.js",
	"../../lib/ccc/pvc/pvcNormalizedBarChart.js",
	"../../lib/ccc/pvc/visual/legend/WaterfallBulletGroupScene.js",
	"../../lib/ccc/pvc/pvcWaterfallPanel.js",
	"../../lib/ccc/pvc/pvcWaterfallChart.js",
	"../../lib/ccc/pvc/pvcPointPanel.js",
	"../../lib/ccc/pvc/pvcPoint.js",
	"../../lib/ccc/pvc/pvcHeatGridPanel.js",
	"../../lib/ccc/pvc/pvcHeatGridChart.js",
	"../../lib/ccc/pvc/pvcMetricXYAbstract.js",
	"../../lib/ccc/pvc/data/translation/MetricPointChartTranslationOper.js",
	"../../lib/ccc/pvc/pvcMetricPointPanel.js",
	"../../lib/ccc/pvc/pvcMetricPoint.js",
	"../../lib/ccc/pvc/pvcBulletChart.js",
	"../../lib/ccc/pvc/pvcParallelCoordinates.js",
	"../../lib/ccc/pvc/pvcDataTree.js",
	"../../lib/ccc/pvc/data/translation/BoxplotChartTranslationOper.js",
	"../../lib/ccc/pvc/pvcBoxplotPanel.js",
	"../../lib/ccc/pvc/pvcBoxplotChart.js"
]);


var aChart={};

(function(){

var TIME_FORMAT="yyyy-MM-dd HH:mm:ss";

//STATIC MAP FROM MY CHART TYPES TO CCC CLASS NAMES
var CHART_TYPES={
	"line":"LineChart",
	"stackedarea":"StackedAreaChart",
	"stacked":"StackedAreaChart",
	"area":"StackedAreaChart",
	"stackedbar":"StackedBarChart",
	"bar":"BarChart",
	"bullet":"BulletChart",
	"scatter":"MetricLineChart",
	"xy":"MetricDotChart",
	"dot":"MetricDotChart",
	"heat":"HeatGridChart",
	"box":"BoxplotChart"
};

////////////////////////////////////////////////////////////////////////////
// STYLES
////////////////////////////////////////////////////////////////////////////
var DEFAULT_STYLES = [
	{"color":"#1f77b4"},
	{"color":"#ff7f0e"},
	{"color":"#2ca02c"},
	{"color":"#d62728"},
	{"color":"#9467bd"},
	{"color":"#8c564b"},
	{"color":"#e377c2"},
	{"color":"#7f7f7f"},
	{"color":"#bcbd22"},
	{"color":"#17becf"}
];
aChart.DEFAULT_STYLES=DEFAULT_STYLES;


function copyParam(fromParam, toParam){
	{//COPY EXTENSION POINTS TO PARAMETERS
		var extPoints = {};
		Map.copy(toParam.extensionPoints, extPoints);
		if (fromParam.extensionPoints) Map.copy(fromParam.extensionPoints, extPoints);
		if (fromParam.timeSeriesFormat) toParam.timeSeriesFormat = JavaDateFormat2ProtoVisDateFormat(fromParam.timeSeriesFormat);

		Map.copy(fromParam, toParam);
		toParam.extensionPoints = extPoints;
	}


	fixClickAction(toParam);


	{//ENSURE CONTAINER DIV IS CORRECT SIZE
		var div = $("#" + fromParam.id);
		div.width(toParam.width);
		div.height(toParam.height);
	}
}








aChart.showProgress=function(params){

	var newParam={
		"type":"bar",
		"stacked":true,
		"orientation":"horizontal",
		"titlePaddings":5,
		"height":20,
		"xAxisGrid":false,
		"xAxisSize":0,
		"yAxisSize": 0,
		valuesVisible: true,
		valuesMask: "{series} ({value})",
		"legend":false,
		"legendPaddings":0,
		"orthoAxisDomainRoundMode" : 'none',
		"orthoAxisFixedMax":true,
		extensionPoints: {
			label_textStyle:"white"
		}
	};

	copyParam(params, newParam);

	aChart.show(newParam);
};//method



/*
minPercent = PREVENT SLICES SMALLER THAN GIVEN (minPercent<1.0)
otherStyle = STYLE FOR THE other SLICE
 */
aChart.showPie=function(params){

	var divName=params.id;

	var type=params.type;
	var chartCube=params.cube;

	if (chartCube.cube.length==0){
		Log.warning("Nothing to pie-chart");
		return;
	}//endif
	if (chartCube.select instanceof Array) Log.error("Can not chart when select clause is an array");



	var values = null;

	if (chartCube.edges.length==1){
		var seriesLabels=getAxisLabels(chartCube.edges[0]);

		if (params.minPercent!==undefined){
			var other = 0;

			//COLAPSE INTO 'OTHER' CATEEGORY
			var total = aMath.SUM(chartCube.cube);
			values = [];
			chartCube.cube.forall(function(v, i){
				if (v/total >= params.minPercent){
					values.append({"name":seriesLabels[i], "value":v, "style":chartCube.edges[0].domain.partitions[i].style})
				}else{
					other+=v;
				}//endif
			});
			values = Qb.sort(values, {"value" : "value", "sort" : -1});
			if (other > 0) values.append({"name" : "Other", "value" : other, "style":params.otherStyle});
		} else {
			values = chartCube.cube.map(function(v, i){
				return {"name" : seriesLabels[i], "value" : v, "style":chartCube.edges[0].domain.partitions[i].style}
			});
			values = Qb.sort(values, {"value" : "value", "sort" : -1});
		}//endif
	} else if (chartCube.edges.length==2){
		var aLabels=getAxisLabels(chartCube.edges[0]);
		var bLabels=getAxisLabels(chartCube.edges[1]);

		if (params.minPercent!==undefined){
			var allOther = 0;

			//COLAPSE INTO 'OTHER' CATEEGORY
			var total = aMath.SUM(chartCube.cube.map(aMath.SUM));
			values = [];
			chartCube.cube.forall(function(s, i){
				var other = 0;
				var hasSpecific = false;
				s.forall(function(v, j){
					if (v/total >= params.minPercent){
						values.append({"name":aLabels[i]+"::"+bLabels[j], "value":v});
						hasSpecific=true;
					}else{
						other+=v;
					}//endif
				});
				if (other/total >= params.minPercent){
					if (hasSpecific){
						values.append({"name":aLabels[i]+"::Other", "value":other})
					}else{
						values.append({"name":aLabels[i], "value":other})
					}//endif
				}else{
					allOther+=other;
				}//endif
			});
			values = Qb.sort(values, {"value":"value", "sort":-1});
			if (allOther>0) values.append({"name":"Other", "value":allOther, "style":params.otherStyle});
		}else{
			Log.error("having hierarchical dimension without a minPercent is not implemented");
		}//endif
	}else{
		Log.error("Only one dimension supported");
	}//endif


	var colors = values.map(function(v, i){
		var c="black";
		if (v.style && v.style.color) {
			c = v.style.color;
		} else {
			c = DEFAULT_STYLES[i % DEFAULT_STYLES.length].color;
		}//endif
		if (c.toHTML){
			return c.toHTML();
		}else{
			return c;
		}//endif
	});



	var chartParams={
		canvas: divName,
		width: 400,
		height: 400,
		animate:false,
		title: coalesce(params.name, chartCube.name),
		legend: true,
		legendPosition: "right",
//		legendAlign: "center",
		legendSize:100,
		orientation: 'vertical',
		timeSeries: false,
		valuesVisible:false,
		showValues: false,
		"colors":colors,
		extensionPoints: {
			noDataMessage_text: "No Data To Chart"
//			xAxisLabel_textAngle: aMath.PI/4,
//			xAxisLabel_textAlign: "left",
//			xAxisLabel_textBaseline: "top"
//			xAxisScale_dateTickFormat: "%Y/%m/%d",
//			xAxisScale_dateTickPrecision: xaxis.domain.interval.milli
			//set in miliseconds
		},
		"clickable": true,
		"clickAction":function(series, x, d, elem){
			bugClicker(chartCube, series, x, d);
		}
	};
	Map.copy(params, chartParams);

	fixClickAction(chartParams);

	var chart = new pvc.PieChart(chartParams);

	//FILL THE CROSS TAB DATASTUCTURE TO THE FORMAT EXPECTED (2D array of rows
	//first row is series names, first column of each row is category name
	var data=values.map(function(v, i){return [v.name, v.value]});


	var cccData = {
		"resultset":data,
		"metadata":[{
			"colIndex":0,
			"colType":"String",
			"colName":"Categories"
		},{
			"colIndex":1,
			"colType":"Numeric",
			"colName":"Value"
		}]
	};

	chart.setData(cccData, {crosstabMode: false, seriesInRows: false});

	chart.render();

};//method


aChart.showScatter=function(params){
	Map.expecting(params, ["id", "cube"]);
	var divName=params.id;

	var chartCube=params.cube;
	var type="scatter";
	var stacked=coalesce(params.stacked, false);


	////////////////////////////////////////////////////////////////////////////
	// SERIES (ONLY IF MORE THAN ONE EDGE)
	////////////////////////////////////////////////////////////////////////////
	var xaxis=chartCube.edges[chartCube.edges.length-1];

	////////////////////////////////////////////////////////////////////////////
	// SET MAX WITHOUT "NICE" ROUNDING BUFFER
	////////////////////////////////////////////////////////////////////////////
	if (params.orthoAxisFixedMax==true){
		if (stacked && chartCube.edges.length==1){
			var max=undefined;
			chartCube.edges[0].domain.partitions.forall(function(part,i){
				var total=0;
				Array.newInstance(chartCube.select).forall(function(s){
					total+=coalesce(chartCube.cube[i][s.name], 0);
				});
				max=aMath.max(max, total);
			});
			params.orthoAxisFixedMax=max==0 ? 1 : max;  //DO NOT USE ZERO
		}else{
			Log.error("Not supported yet");
		}
	}//endif

	////////////////////////////////////////////////////////////////////////////
	// STYLES
	////////////////////////////////////////////////////////////////////////////
	var styles = [
		{"color":"#1f77b4"},
		{"color":"#ff7f0e"},
		{"color":"#2ca02c"},
		{"color":"#d62728"},
		{"color":"#9467bd"},
		{"color":"#8c564b"},
		{"color":"#e377c2"},
		{"color":"#7f7f7f"},
		{"color":"#bcbd22"},
		{"color":"#17becf"}
	];

	if (chartCube.edges.length==1){
		if (chartCube.select instanceof Array){
			for(i=0;i<chartCube.select.length;i++){
				if (chartCube.select[i].color!==undefined) Log.error("expecting color in style attribute (style.color)");
				if (chartCube.select[i].style!==undefined) styles[i]=chartCube.select[i].style;
			}//for
		}else{
			if (chartCube.select.color!==undefined) Log.error("expecting color in style attribute (style.color)");
			if (chartCube.select.style!==undefined) styles[0]=chartCube.select.style;
		}//endif
	}else{
		var parts=chartCube.edges[0].domain.partitions;
		for(i=0;i<parts.length;i++){
			if (parts[i].color!==undefined) Log.error("expecting color in style attribute (style.color)");
			if (parts[i].style!==undefined) styles[i]=parts[i].style;
		}//for
	}//endif




	var height=$("#"+divName).height();
	var width=$("#"+divName).width();

	var chartParams={
		canvas: divName,
		width: width,
		height: height,
		animate:false,
		title: coalesce(params.name, chartCube.name),
		legend: (chartCube.edges.length!=1 || Array.newInstance(chartCube.select).length>1),		//DO NOT SHOW LEGEND IF NO CATEGORIES
		legendPosition: "bottom",
		legendAlign: "center",

		orientation: 'vertical',
		timeSeries: (xaxis.domain.type=="time"),
		timeSeriesFormat: JavaDateFormat2ProtoVisDateFormat(xaxis.domain.format),
		showDots:true,
		dotsVisible: true,
		showValues: false,
		originIsZero: this.originZero,
		yAxisPosition: "right",
		yAxisSize: 50,
		xAxisSize: 50,
		"colors":styles.map(function(s){return s.color;}),
		plotFrameVisible: false,
//		"colorNormByCategory": false,        //FOR HEAT CHARTS
		extensionPoints: {
			noDataMessage_text: "No Data To Chart",
			xAxisLabel_textAngle: aMath.PI/4,
			xAxisLabel_textAlign: "left",
			xAxisLabel_textBaseline: "top",
//			label_textStyle:"white",
//			xAxisScale_dateTickFormat: "%Y/%m/%d",
//			xAxisScale_dateTickPrecision: xaxis.domain.interval.milli
			//set in miliseconds

			dot_shapeRadius: 4, //USEd IN LEGEND (VERSION 2)
			dot_shape:"circle",
			line_lineWidth: 4
//			line_strokeStyle:
		}
	};
	copyParam(params, chartParams);


	if (xaxis.domain.type=="time"){
		//LOOK FOR DATES TO MARKUP

		var dateMarks = [];
		dateMarks.appendArray(findDateMarks(xaxis.domain));  //WE CAN PLUG SOME dateMarks RIGHT INTO TIME DOMAIN FOR DISPLAY
		if (dateMarks.length>0){
			chartParams.renderCallback=function(){
				var self=this;
				dateMarks.forall(function(m){
					try{
						self.chart.markEvent(Date.newInstance(m.date).format(Qb.domain.time.DEFAULT_FORMAT), m.name, m.style);
					}catch(e){
						Log.warning("markEvent failed", e);
					}
				});
				if (params.renderCallback) params.renderCallback();  //CHAIN EXISTING, IF ONE
			};
		}//endif
	}//endif



	var chart = new pvc[CHART_TYPES[type]](chartParams);

	//SCATTER REQUIRES ONE RECORD PER DATA POINT
	//first column is category names, second column is series names, third is value
	var data;
	var categoryLabels=[];
	if (chartCube.edges.length==1){
		valueName=Array.newInstance(chartCube.select)[0].name;
		seriesName=xaxis.name;
		seriesFormatter=xaxis.domain.label;
		columns=["placeholder", seriesName, valueName];
		//GIVE EACH SELECT A ROW
		data=chartCube.list.map(function(v, i){
			return [
				"",
				seriesFormatter(v[seriesName]),
				v[valueName]
			];
		});
	}else{
		categoryName=chartCube.edges[0].name;
		categoryLabels=chartCube.edges[0].domain.partitions.map(function(v){
			return chartCube.edges[0].domain.label(v)
		});

		valueName=Array.newInstance(chartCube.select)[0].name;
		seriesName=xaxis.name;
		seriesFormatter=xaxis.domain.label;
		columns=[categoryName, seriesName, valueName];
		//GIVE EACH SELECT A ROW
		data=chartCube.list.map(function(v, i){
			return [
				chartCube.edges[0].domain.label(v[categoryName]),
				seriesFormatter(v[seriesName]),
				v[valueName]
			];
		});
	}//endif

	//CCC2 - metadata MUST BE IN x, y, category ORDER!
	var metadata=columns.map(function(v, i){ return {"colIndex":i, "colName":v, "colType":i==0?"String":"Numeric"};});

	var cccData = {
		"resultset":data,
		"metadata":metadata
	};

	chart.setData(cccData, {crosstabMode: false, seriesInRows: false});
	chart.render();

	//STARTS AS VISIBLE, SO TOGGLE TO HIDE
	styles.forall(function(s, i){
		if (s.visibility && s.visibility=="hidden" && chart.legendPanel!=null){
			var datums=chart.legendPanel.data._datums.map(function(d){
				if (d.key.indexOf(","+categoryLabels[i]+",")>=0) return d;
			});
			pvc.data.Data.setVisible(datums, false);
		}
	});
	chart.render(true, true, false);



//	chart.basePanel.chart.legendPanel

	//ADD BUTTON TO SHOW SHEET
	if (params.sheetDiv){


		var sheetButtonID=divName+"-showSheet";
		var html='<div id='+convert.String2Quote(sheetButtonID)+' class="toolbutton" style="right:3;bottom:3" title="Show Table"><img src="'+Settings.imagePath+'/Spreadsheet.png"></div>';


		$("#"+divName).append(html);
		$("#"+sheetButtonID).click(function(){
			var oldHtml=$("#"+params.sheetDiv).html();
			var newHtml=convert.Cube2HTMLTable(chartCube);

			if (oldHtml!=""){
				$("#"+params.sheetDiv).html("");
			}else{
				$("#"+params.sheetDiv).html(newHtml);
			}//endif
		});
	}//endif


};


aChart.show=function(params){
	Map.expecting(params, ["id", "cube"]);
	var divName=params.id;

	var chartCube=params.cube;
	var type=params.type;
	var stacked=coalesce(params.stacked, false);
	////////////////////////////////////////////////////////////////////////////
	// TYPE OF CHART
	////////////////////////////////////////////////////////////////////////////
	if (type===undefined){
		//NUMBER OF EDGES
		if (chartCube.edges.length==1){
			//TYPE OF EDGES
			if (Qb.domain.ALGEBRAIC.contains(chartCube.edges[0].domain.type)){
				type="line";
			}else{
				type="bar";
			}//endif
		}else if (chartCube.edges.length==2){
			if (Qb.domain.ALGEBRAIC.contains(chartCube.edges[0].domain.type)){
				if (Qb.domain.ALGEBRAIC.contains(chartCube.edges[1].domain.type)){
					type="heat";
				}else{
					type="bar";
//					params.orientation="horizontal"
				}//endif
			}else{
				if (Qb.domain.ALGEBRAIC.contains(chartCube.edges[1].domain.type)){
					type="line";
				}else{
					type="bar";
					stacked=true;
				}//endif
			}//endif
		}else{
			Log.error("Can not handle more than 2 edges");
		}//endif
	}else if (["stacked", "stackedarea", "area"].contains(type)){
		stacked=true;
	}//endif



	////////////////////////////////////////////////////////////////////////////
	// SERIES (ONLY IF MORE THAN ONE EDGE)
	////////////////////////////////////////////////////////////////////////////
	var xaxis=chartCube.edges[chartCube.edges.length-1];
	var seriesLabels=getAxisLabels(xaxis);

	var categoryLabels;
	if (chartCube.edges.length==1 || chartCube.edges[0].domain.partitions.length==0){
		categoryAxis={"domain":{"type":"set", "partitions":Array.newInstance(chartCube.select)}};
		categoryLabels=Array.newInstance(chartCube.select).map(function(v, i){
			return v.name;
		});
	}else if (chartCube.edges.length==2){
		if (chartCube.select instanceof Array){
			Log.error("Can not chart when select clause is an array");
		}//endif
		categoryAxis=chartCube.edges[0];
		categoryLabels=getAxisLabels(chartCube.edges[0]);
	}//endif

	////////////////////////////////////////////////////////////////////////////
	// SET MAX WITHOUT "NICE" ROUNDING BUFFER
	////////////////////////////////////////////////////////////////////////////
//	if (params.orthoAxisFixedMax==true){
//		if (stacked && chartCube.edges.length==1){
//			var max=undefined;
//			chartCube.edges[0].domain.partitions.forall(function(part,i){
//				var total=0;
//				Array.newInstance(chartCube.select).forall(function(s){
//					total+=coalesce(chartCube.cube[i][s.name], 0);
//				});
//				max=aMath.max(max, total);
//			});
//			params.orthoAxisFixedMax=max==0 ? 1 : max;  //DO NOT USE ZERO
//		}else{
//			Log.error("Not supported yet");
//		}
//	}//endif

	////////////////////////////////////////////////////////////////////////////
	// STYLES
	////////////////////////////////////////////////////////////////////////////
	var styles = [
		{"color":"#1f77b4"},
		{"color":"#ff7f0e"},
		{"color":"#2ca02c"},
		{"color":"#d62728"},
		{"color":"#9467bd"},
		{"color":"#8c564b"},
		{"color":"#e377c2"},
		{"color":"#7f7f7f"},
		{"color":"#bcbd22"},
		{"color":"#17becf"}
	];
	if (type=="heat"){
		styles=[
		{"color":"#EEEEEE"},
		{"color":"#BBBBBB"},
		{"color":"#999999"},
		{"color":"#666666"},
		{"color":"#333333"}
		];
	}//endif

	if (chartCube.edges.length==1){
		if (chartCube.select instanceof Array){
			for(i=0;i<chartCube.select.length;i++){
				if (chartCube.select[i].color!==undefined) Log.error("expecting color in style attribute (style.color)");
				if (chartCube.select[i].style!==undefined) styles[i]=chartCube.select[i].style;
			}//for
		}else{
			if (chartCube.select.style!==undefined){
				styles[0]=chartCube.select.style;
			}else{
				var parts=chartCube.edges[0].domain.partitions;
				for(i=0;i<parts.length;i++){
					if (parts[i].color!==undefined) Log.error("expecting color in style attribute (style.color)");
					if (parts[i].style!==undefined) styles[i]=parts[i].style;
				}//for
			}//endif
		}//endif
	}else{
		var parts=chartCube.edges[0].domain.partitions;
		for(i=0;i<parts.length;i++){
			if (parts[i].color!==undefined) Log.error("expecting color in style attribute (style.color)");
			if (parts[i].style!==undefined) styles[i]=parts[i].style;
		}//for
	}//endif




	var height=$("#"+divName).height();
	var width=$("#"+divName).width();

	var defaultParam={
		canvas: divName,
		width: width,
		height: height,
		animate:false,
		ignoreNulls: false,
		title: coalesce(params.name, chartCube.name),
		legend: (chartCube.edges.length!=1 || Array.newInstance(chartCube.select).length>1),		//DO NOT SHOW LEGEND IF NO CATEGORIES
		legendPosition: "bottom",
		legendAlign: "center",

		orientation: 'vertical',
		timeSeries: (xaxis.domain.type=="time"),
		timeSeriesFormat: JavaDateFormat2ProtoVisDateFormat(Qb.domain.time.DEFAULT_FORMAT),
		showDots:true,
		showValues: false,
		"stacked":stacked,
		originIsZero: this.originZero,
		yAxisPosition: "right",
		yAxisSize: 50,
		xAxisSize: 50,
		"otherStyle":{"color":"lightgray"},
		tooltip:{
//			"format":function(){
//				return "hi there";
//			}
		},
		"colors":styles.map(function(s, i){
			var c = coalesce(s.color, DEFAULT_STYLES[i%(DEFAULT_STYLES.length)].color);
			if (c.toHTML){
				return c.toHTML();
			}else{
				return c;
			}//endif
		}),
		plotFrameVisible: false,
		"colorNormByCategory": false,        //FOR HEAT CHARTS
		extensionPoints: {
			noDataMessage_text: "No Data To Chart",
			xAxisLabel_textAngle: aMath.PI/4,
			xAxisLabel_textAlign: "left",
			xAxisLabel_textBaseline: "top",
//			label_textStyle:"white",
//			xAxisScale_dateTickFormat: "%Y/%m/%d",
//			xAxisScale_dateTickPrecision: xaxis.domain.interval.milli
			//set in miliseconds

			dot_shapeRadius: 4, //USEd IN LEGEND (VERSION 2)
			dot_shape:"circle",
			line_lineWidth: 4
//			line_strokeStyle:
		},
		"clickable": true,
		"clickAction":function(series, x, d, elem){
			bugClicker(chartCube, series, x, d, elem);
		}
	};

	if (xaxis.domain.type=="time"){
		//LOOK FOR DATES TO MARKUP

		var dateMarks = [];
		dateMarks.appendArray(findDateMarks(xaxis.domain));  //WE CAN PLUG SOME dateMarks RIGHT INTO TIME DOMAIN FOR DISPLAY
		categoryAxis.domain.partitions.forall(function (part) {  //EACH CATEGORY CAN HAVE IT'S OWN dateMarks to SHOW
			dateMarks.appendArray(findDateMarks(part))
		});
		if (dateMarks.length>0){
			defaultParam.renderCallback=function(){
				var self=this;
				dateMarks.forall(function(m){
					try{
						self.chart.markEvent(m.date.format(Qb.domain.time.DEFAULT_FORMAT), m.name, m.style);
					}catch(e){
						Log.warning("markEvent failed", e);
					}
				});
				if (params.renderCallback) params.renderCallback();  //CHAIN EXISTING, IF ONE
			};
		}//endif
	}//endif


	copyParam(params, defaultParam);


	var chart = new pvc[CHART_TYPES[type]](defaultParam);

	//FILL THE CROSS TAB DATA STRUCTURE TO THE FORMAT EXPECTED (2D array of rows
	//first row is series names, first column of each row is category name
	var data;
	if (chartCube.edges.length==1){
		if (chartCube.select instanceof Array){
			//GIVE EACH SELECT A ROW
			data=[];
			for(var s=0;s<chartCube.select.length;s++){
				var row=chartCube.cube.map(function(v, i){
					return v[chartCube.select[s].name];
				});
				data.push(row);
			}//for
		}else if (Qb.domain.ALGEBRAIC.contains(chartCube.edges[0].domain.type)){
			//ALGEBRAIC DOMAINS ARE PROBABLY NOT MULTICOLORED
			data=[chartCube.cube]
		}else{
			//SWAP DIMENSIONS ON CATEGORICAL DOMAIN SO WE CAN USE COLOR
			var temp=seriesLabels;
			seriesLabels=categoryLabels;
			categoryLabels=temp;
			data=chartCube.cube.map(function(v){return [v];});
		}//endif
	}else{
		data=chartCube.cube.copy();
	}//endif


	//
	//
	data.forall(function(v,i,d){
		v=v.copy();
		for(var j=0;j<v.length;j++){
			if (v[j]==null){
				//the charting library has a bug that makes it simply not draw null values
				//(or even leave a visual placeholder)  This results in a mismatch between
				//the horizontal scale and the values charted.  For example, if the first
				//day has null, then the second day is rendered in the first day position,
				//and so on.
				Log.error("Charting library can not handle null values (maybe set a default?)");
			}//endif
		}//for
		v.splice(0,0, categoryLabels[i]);
		d[i]=v;
	});

	var metadata=seriesLabels.map(function(v, i){ return {"colName":v};});
	metadata.splice(0,0,{"colName":"x"});

	var cccData = {
		"resultset":data,
		"metadata":metadata
	};

	chart.setData(cccData, {crosstabMode: true, seriesInRows: true});
	chart.render();

	//STARTS AS VISIBLE, SO TOGGLE TO HIDE
	styles.forall(function(s, i){
		if (s.visibility && s.visibility=="hidden" && chart.legendPanel!=null){
			var datums=chart.legendPanel.data._datums.map(function(d){
				if (d.key.indexOf(","+categoryLabels[i]+",")>=0) return d;
			});
			pvc.data.Data.setVisible(datums, false);
		}//endif
	});
	chart.render(true, true, false);

//	chart.basePanel.chart.legendPanel

	//ADD BUTTON TO SHOW SHEET
	if (params.sheetDiv){


		var sheetButtonID=divName+"-showSheet";
		var html='<div id='+convert.String2Quote(sheetButtonID)+' class="toolbutton" style="right:3;bottom:3" title="Show Table"><img src="'+Settings.imagePath+'/Spreadsheet.png"></div>';


		$("#"+divName).append(html);
		$("#"+sheetButtonID).click(function(){
			var oldHtml=$("#"+params.sheetDiv).html();
			var newHtml=convert.Cube2HTMLTable(chartCube);

			if (oldHtml!=""){
				$("#"+params.sheetDiv).html("");
			}else{
				$("#"+params.sheetDiv).html(newHtml);
			}//endif
		});
	}//endif


};




//FIX CLICKACTION SO IT WORKS IN BOTH CHART VERSIONS
function fixClickAction(chartParams){
	fixAction(chartParams, "clickAction");
	fixAction(chartParams, "tooltipFormat");
}

function fixAction(chartParams, actionName){
	var action = chartParams[actionName];
	if (action===undefined) return;
	chartParams[actionName] = function(series, x, d, elem){
		if (series.atoms !== undefined){
			//CCC VERSION 2
			var s = coalesce(series.atoms.series, {"value":"data"}).value;
			var c = coalesce(series.atoms.category, series.atoms.x).value;
			var v = coalesce(series.atoms.value, series.atoms.y).value;
			if (c instanceof Date){  //CCC 2 DATES ARE IN LOCAL TZ
				c = c.addTimezone();
			}//endif

			return action(s, c, v, elem, series.dataIndex);
		} else{
			//CCC VERSION 1
			return action(series, x, d, elem);
		}//endif
	};//method
}


//name IS OPTIONAL
function findDateMarks(part, name){
	try{
	var output = [];
	Array.newInstance(part.dateMarks).forall(function (mark) {
		var style = Map.setDefault({}, mark.style, part.style, {"color": "black", "lineWidth": "2.0", verticalAnchor: "top"});
		style.strokeStyle = coalesce(style.strokeStyle, style.color);
		style.textStyle = coalesce(style.textStyle, style.color);

		if (mark.name !== undefined) {
			//EXPECTING {"name":<name>, "date":<date>, "style":<style>} FORMAT
			output.append({
				"name": mark.name,
				"date": Date.newInstance(mark.date),
				"style": style
			})
		} else {
			//EXPECTING <name>:<date> FORMAT
			Map.forall(mark, function(name, date){
				output.append({
					"name": name,
					"date": Date.newInstance(date),
					"style": style
				})
			});
		}
	});

	if (name){
		var matches = output.filter(function(p){return p.name==name;});
		if (matches.length>0){
			return matches.first().date;
		}else{
			return output;
		}//endif
	}else{
		return output;
	}//endif
	}catch(e){
		Log.error("some error found", e);
	}//try
}//method

aChart.findDateMarks = findDateMarks;



var BZ_SHOW_BUG_LIMIT=300;
function bugClicker(query, series, x){

	if (typeof(query.from) != "string"){
		//NOT SUPPORTED
		return;
	}//endif


	try{
		//We can decide to drilldown, or show a bug list.
		//Sometimes drill down is not available, and bug list is too big, so nothing happens
		//When there is a drilldown, the decision to show bugs is made at a lower count (prefering drilldown)
		Thread.run(function*(){
			try{
				var specific;
				if (query.edges.length==2){
					specific=Qb.specificBugs(query, [series, x]);
				}else{
					specific=Qb.specificBugs(query, [x]);
				}//endif



	//			var specific=Qb.specificBugs(query, [series, x]);
				var buglist=(yield (ESQuery.run(specific)));
	//			buglist=buglist.list.map(function(b){return b.bug_id;});
				if (buglist.cube===undefined) buglist.cube=buglist.list;


				if (buglist.cube.length>BZ_SHOW_BUG_LIMIT){
					Log.alert("Too many bugs. Truncating to "+BZ_SHOW_BUG_LIMIT+".", function(){
						Bugzilla.showBugs(buglist.cube.substring(0, BZ_SHOW_BUG_LIMIT));
					});
				}else{
					Bugzilla.showBugs(buglist.cube);
				}//endif
			}catch(e){
				Log.warning("failure to show bugs", e);
			}
		});



	}catch(e){
		//DO NOTHING
	}//try
}


function JavaDateFormat2ProtoVisDateFormat(format){
	if (format===undefined) return undefined;
	return format
		.replaceAll('yyyy', '%y')
		.replaceAll('NNN', '%b')
		.replaceAll('MM', '%m')
		.replaceAll('dd', '%d')
		.replaceAll('HH', '%H')
		.replaceAll('mm', '%M')
		.replaceAll('ss', '%S')
	;
}


function getAxisLabels(axis){
	var labels;
	if (axis.domain.type == "time"){
		if (axis.domain.allowNulls) Log.error("Charting lib can not handle NULL domain value.");
		var format=Qb.domain.time.DEFAULT_FORMAT;
		labels=axis.domain.partitions.map(function(v, i){
			if (v.value!=undefined){
				return v.value.format(format);
			}else{
				return Date.newInstance(v).format(format);
			}//endif
		});
	} else if (axis.domain.type == "duration"){
		labels=axis.domain.partitions.map(function(v, i){
			if (v instanceof String){
				return v;
			} else if (v.milli === undefined){
				return v.value.toString();
			} else{
				return ""+v.divideBy(Duration.DAY);
//				return v.toString();
			}//endif
		});
	} else if (axis.domain.type == "numeric"){
		labels=axis.domain.partitions.select("name");
	} else{
		labels=axis.domain.partitions.map(function(v, i){
			if (v instanceof String){
				return v;
			}else if (aMath.isNumeric(v)){
				return ""+v;
			}else{
				if (axis.domain.label!==undefined){
					//ALL DOMAINS EXPECTED TO HAVE LABELS
					return ""+axis.domain.label(v);
				}else{
					return ""+axis.domain.end(v);
				}//endif
			}//endif
		});
	}//endif
	if (axis.allowNulls) labels.push(axis.domain.NULL.name);
	return labels;
}//method


////////////////////////////////////////////////////////////////////////////////
// CALCULATE THE SPECIFIC VALUES FOR THE LINE
// THIS IS A LITLE SCREWY BECAUSE source.data IS ALREADY INTEGER INDEXED
// param.predict.line EXPECTS ONE PARAMETER FROM THE param.predict.domain
////////////////////////////////////////////////////////////////////////////////
//EXAMPLE
//	aChart.addPredictionLine({
//		"source":{
//			"name":"open",
//			"domain":{"min":sampleMin, "max":MAX_CHART_DATE},
//			"data":flat.list
//		},
//		"predict":{
//			"name":"estimate",
//			"domain":{"min":statsMax, "max":MAX_CHART_DATE},
//			"line":function(date){
//				return aMath.round(openAtStartOfEstimate + (-netCloseRatePerDay * (Date.diffWeekday(date, statsMax))));
//			}
//		}
//	});
//
aChart.addPredictionLine = function(param){
		Map.expecting(param, ["source", "predict"]);
		Map.expecting(param.source, ["name", "domain", "data"]);
		Map.expecting(param.predict, ["name", "domain", "line"]);

		var data=param.source.data;

		var num = param.source.domain.max.subtract(param.source.domain.min).divideBy(Duration.DAY);
		var minDomain = param.predict.domain.min.subtract(param.source.domain.min).divideBy(Duration.DAY)+1;

		//COPY UP TO THE POINT OF PREDICTION (BECAUSE CHARTING LIB SUCKS)
		for(var i = 0; i < Math.min(minDomain, num); i++){
			data[i]["date"] = param.source.domain.min.addDay(i);
			data[i][param.predict.name] = data[i][param.source.name];
		}//for

		//ADD LINEAR LINE
		for(i = minDomain; i < num; i++){
			var date=param.source.domain.min.addDay(i);
			if (data[i] === undefined) data[i] = {"open":null, "closed":null, "total":null};
			data[i]["date"] = date;
			data[i][param.predict.name] = param.predict.line(date);
			if (data[i][param.predict.name] < 0) data[i][param.predict.name] = 0;
		}//for
	};

})();
