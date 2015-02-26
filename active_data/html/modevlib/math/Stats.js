/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("../aLibrary.js");

var Stats={};

(function(){

	var DEBUG=false;

Stats.df={};

//GIVEN A STEPWISE df, RETURN THE STEP WHICH APPROXIMATES THE percentile
Stats.df.percentile=function(df, percentile){
	var df=Stats.df.normalize(df);

	var total=0;
	for(var i=0;i<df.length;i++){
		total+=df[i];
		if (total>=percentile){
			var part=(percentile-total)/df[i];
			return part+i+1;
		}//endif
	}//for
	return df.length;		//HAPPENS WHEN df[i]==0
};//method

//NORMALIZE TO A DISTRIBUTION FUNCTION
Stats.df.normalize=function(df){
	var total=0;
	for(var t=df.length;t--;) total+=aMath.abs(df[t]);
	if (total==0 || (1-epsilon)<total && total<(1+epsilon)) return df;

	var output=[];
	for(var i=df.length;i--;) output[i]=df[i]/total;
	return output;
};//method

epsilon=1/1000000;


//EXPECTING AGE DATA {"id", "birth", "death"}
//EXPECTING REPORTING INTERVAL timeEdge={"min", "max", "interval"}
//EXPECTING SAMPLE DURATION (ASSUMED PRECEEDING) {"duration"}
//IT IS ASSUMED THE data INCLUDES RANGE {"min":min-duration, "max"} FOR ACCURACY
//DATA NEED NOT BE ORDERED
//RETURNS {"time", "age"} FOR PERCENTILE REQUESTED
Stats.percentileAgeOverTime=function(data, timeEdge, sampleSize, percentile){
	var newdata=data.map(function(v, i){
		var d=[];
		d[0]=v.birth.getMilli();
		d[1]=v.death.getMilli();
	});

	var output=[];
	for(var i=timeEdge.min;i.getMilli()<timeEdge.max.getMilli();i=i.add(timeEdge.interval)){
		var min=i.subtract(sampleSize).getMilli();
		var max=i.getMilli();

		var ages=[];
		for(var d=0;d<newdata.length;d++){
			if (newdata[d][0]>=min) ages.push(aMath.min(max, newdata[d][1])-newdata[d][0]);
		}//for

		ages.sort();
		var smaller=aMath.floor(ages.length*percentile);
		var larger=aMath.max(smaller+1, ages.length-1);
		var r=(ages.length*percentile) - smaller;
		var value=ages[smaller]*(1-r)+(ages[larger]*r);

		output.push({"time":i, "age":value});
	}//for

	return output;
};


//EXPECTING AGE DATA {"id", "birth", "death"}
//EXPECTING REPORTING INTERVAL timeEdge={"min", "max", "interval"}
//EXPECTING SAMPLE DURATION (ASSUMED PRECEEDING) {"duration"}
//IT IS ASSUMED THE data INCLUDES RANGE {"min":min-duration, "max"} FOR ACCURACY
//DATA NEED NOT BE ORDERED
//RETURNS {"time", "age"} FOR PERCENTILE REQUESTED
Stats.ageOverTime=function(data, timeEdge, sampleSize, statFunction){
	var newdata=data.map(function(v, i){
		var d=[];
		d[0]=v.birth;
		d[1]=v.death;
		return d;
	});
	newdata.sort(function(a, b){return a[0]-b[0]});


	var start=0;  //SINCE i IS STRICTLY INCREASING, NO NEED TO REVISIT d<start
	var output=[];
	for(var i=timeEdge.min;i.getMilli()<timeEdge.max.getMilli();i=i.add(timeEdge.interval)){
		var min=i.subtract(sampleSize).getMilli();
		var max=i.getMilli();

		var ages=[];
		for(var d=start;d<newdata.length;d++){
			if (min>newdata[d][0]){
				start=d;
				continue;
			}//endif
			if (newdata[d][0]>=max) break;

			var age=aMath.min(max, newdata[d][1]) - newdata[d][0];
			ages.push(age);
		}//for

		//VALUE MUST BE AN OBJECT (WITH MANY STATS)
		var value=statFunction(ages);
		value.time=i;
		output.push(value);
	}//for

	return output;
};

//selectColumns IS AN ARRAY OF SELECT COLUMNS {"name", "percentile"}
//values IS AN ARRAY OF RAW NUMBERS
Stats.percentiles=function(values, selectColumns){
	values.sort(function(a, b){return a-b;});
	var output={};
	for(var i=0;i<selectColumns.length;i++){
		var smaller=aMath.floor(values.length*selectColumns[i].percentile);
		var larger=aMath.min(smaller+1, values.length-1);
		var r=(values.length*selectColumns[i].percentile) - smaller;
		output[selectColumns[i].name]=values[smaller]*(1-r)+(values[larger]*r);

	}//for
	return output;
};



//selectColumns IS AN ARRAY OF SELECT COLUMNS {"name", "percentile"}
//values IS AN ARRAY OF RAW NUMBERS
//LINEAR INTERPOLATION IS USED
Stats.percentile=function(values, percentile){
	if (values.length==0) return null;

	values.sort(function(a, b){return a-b;});
	var smaller=aMath.floor(values.length*percentile);
	var larger=aMath.min(smaller+1, values.length);
	values.prepend(0);
	var r=(values.length*percentile) - smaller;
	return values[smaller]*(1-r)+(values[larger]*r);
};

Stats.middle=function(values, percentile){
	if (values.length==0) return null;
	if (values.length==1) return {"min":values[0], "max":values[0]};

	values.sort(function(a, b){return a-b;});
	var numIgnored=Math.floor(values.length*(1-percentile)/2+0.01);
	return {"min":values[numIgnored], "max":values[values.length-numIgnored-1]};
};



Stats.median=function(values){
	if (values.length==0) return null;
	if (values.length==1) return {"min":values[0], "max":values[0]};

	var i=Math.floor(values.length/2);
	if (values.length %2==0){
		return (values[i-1]+values[i])/2;
	}else{
		return values[i]
	}//endif
};


//USING ONE
//Stats.percentile=function(values, percentile){
//	if (values.length==0) return null;
//
//	values.sort(function(a, b){return a-b;});
//	var smaller=aMath.floor(values.length*percentile+0.5);
//	if (smaller==0) return 0;
//	return values[smaller-1];
//};




Stats.query2regression=function(query){
	var select=Array.newInstance(query.select);
	if (query.edges.length==2 && select.length==1){
		//WE ASSUME THE SELECT IS THE WEIGHT FUNCTION
	}else{
		Log.error("Not supported");
	}//endif

	//CONVERT ALGEBRAIC DOMAINS TO doubles
	var domainY=Qb.domain.algebraic2numeric(query.edges[0].domain);
	var domainX=Qb.domain.algebraic2numeric(query.edges[1].domain);

	//SEND TO REGRESSION CALC
	var line=Stats.regression(Qb.cube.transpose(query, [query.edges[1], query.edges[0]], select).cube, domainX, domainY);

	//CONVERT BACK TO DOMAIN (result REFERS TO
	if (["time", "duration"].contains(domainX.type)) Log.error("Can not convert back to original domain, not implemented yet");
	if (["time", "duration"].contains(domainY.type)) Log.error("Can not convert back to original domain, not implemented yet");


	//BUILD NEW QUERY WITH NEW Qb
	var output=Map.copy(query);
	output.select={"name":query.edges[0].name};
	output.edges=[query.edges[1]];

	output.cube=[];
	domainX.partitions.forall(function(v, i){
		var part=domainX.partitions[i];
		output.cube[i]=line((part.min+part.max)/2)
	});
	output.name=" ";

	return output;
};

//ASSUMES A Qb OF WEIGHT VALUES
//RETURN THE REGRESSION LINE FOR THE GIVEN Qb
Stats.regression=function(weights, domainX, domainY){
	if (DEBUG) Log.note(convert.value2json([weights, {"min":domainX.min, "max":domainX.max, "interval":domainX.interval}, {"min":domainY.min, "max":domainY.max, "interval":domainY.interval}]));

	if (((domainX.max-domainX.min)/domainX.interval)<=1) Log.error("Can not do regression with only one value");
	if (((domainY.max-domainY.min)/domainY.interval)<=1) Log.error("Can not do regression with only one value");

	var t={};
	t.N=0;
	t.X1=0;
	t.X2=0;
	t.XY=0;
	t.Y1=0;
	t.Y2=0;

	var desc="";
	if (DEBUG) desc+="y\tx\n";

	var x=domainX.min+(domainX.interval/2);
	for(var i=0;x<domainX.max;x+=domainX.interval){
		var y=domainY.min+(domainY.interval/2);
		for(var j=0;y<domainY.max;y+=domainY.interval){
			var w=weights[i][j];

			if (DEBUG) {
				for(var k=0;k<w;k++) desc+=y+"\t"+x+"\n";
			}

			t.N+=w;
			t.X1+=w*x;
			t.X2+=w*x*x;
			t.XY+=w*x*y;
			t.Y2+=w*y*y;
			t.Y1+=w*y;
			j++;
		}//for
		i++;
	}//for
	if (DEBUG) Log.note(desc);

	return Stats.regressionLine(t);
};

//EXPECT THE RAW TERMS FROM THE SERIES
Stats.regressionLine=function(terms){
	var o=terms;
	o.Sxx=o.X2 - (o.X1*o.X1)/o.N;
	o.Syy=o.Y2 - (o.Y1*o.Y1)/o.N;
	o.Sxy=o.XY - (o.X1*o.Y1)/o.N;
	o.slope=o.Sxy/o.Sxx;
//	o.slope=-0.0208;
	o.offset=(o.Y1-o.slope*o.X1)/o.N;
	o.r2=(o.Sxy*o.Sxy)/(o.Sxx*o.Syy);


	var output=function(x){
		return o.slope*x+o.offset;
	};
	Map.copy(o, output);

	return output;
};


if (DEBUG){
	try{
		if (Stats.regression([[0, 1], [0, 1], [0, 1]], {"min":0, "max":3, "interval":1}, {"min":0, "max":2, "interval":1}).offset!=1.5) Log.error();
		if (Stats.regression([[1, 1, 1], [1, 1, 1], [1, 1, 1]], {"min":0, "max":3, "interval":1}, {"min":0, "max":3, "interval":1}).slope!=0) Log.error();
		if (Stats.regression([[1, 0, 0], [0, 1, 0], [0, 0, 1]], {"min":0, "max":3, "interval":1}, {"min":0, "max":3, "interval":1}).slope!=1) Log.error();
		if (Stats.regression([[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0]], {"min":0, "max":3, "interval":1}, {"min":0, "max":3, "interval":1}).slope!=1) Log.error();
		if (Stats.regression([[10, 0, 0, 0], [0, 10, 0, 0], [0, 0, 10, 0]], {"min":0, "max":3, "interval":1}, {"min":0, "max":3, "interval":1}).slope!=1) Log.error();

		var result=Stats.regression([
				[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[23,2,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
			],
			{"min":0,"max":20,"interval":5},
			{"min":0,"max":105,"interval":5});
		if (aMath.round(result.offset, 2)!=7.55) Log.error();
		if (aMath.round(result.slope, 4)!=-0.2037) Log.error();



		var result=Stats.regression([
				[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[23,2,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[13,24,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[13,32,14,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[5,51,26,75,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[7,53,39,14,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,31,37,21,9,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,17,19,198,15,8,41,1,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,12,31,24,33,15,11,1,0,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,3,13,14,18,28,16,6,1,0,0,0,0,0,0,0,0,0,0,0,0],
				[0,6,11,121,28,14,121,9,5,12,0,0,0,0,0,0,0,0,0,0,0],
				[0,2,6,9,15,18,13,12,10,2,1,0,0,0,0,0,0,0,0,0,0],
				[0,3,7,6,12,11,16,20,8,10,9,0,1,0,0,0,0,0,0,0,0],
				[0,3,7,66,8,9,72,7,12,62,4,3,8,0,0,0,0,0,0,0,0],
				[0,1,5,5,9,6,14,7,13,17,7,5,3,1,0,0,0,0,0,0,0],
				[0,1,2,2,3,6,4,12,7,11,12,5,3,0,0,0,0,0,0,0,0],
				[1,0,1,45,4,2,59,6,7,51,11,12,41,3,3,6,0,0,0,0,0],
				[1,2,4,2,2,3,6,6,5,3,4,4,5,6,2,0,0,0,0,0,0],
				[0,1,2,5,6,1,1,3,6,7,4,10,13,4,4,3,0,0,0,0,0]
			],
			{"min":0,"max":105,"interval":5},
			{"min":0,"max":105,"interval":5});
		if (aMath.round(result.offset, 2)!=-6.39) Log.error();
		if (aMath.round(result.slope, 4)!=0.5207) Log.error();

//	if (Stats.regression([[1, 1, 1]], {"min":0, "max":3, "interval":1}, {"min":0, "max":1, "interval":1}).offset!=1) Log.error();
	}catch(e){
		Log.error("Test failure", e);
	}//try
	Log.note("All tests pass");
}//endif

})();
