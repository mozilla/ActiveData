
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("aDate.js");
importScript("convert.js");



var Duration = function(){
	this.milli = 0;	//INCLUDES THE MONTH VALUE AS MILLISECONDS
	this.month = 0;
};


Duration.DOMAIN={
	"type":"duration",
	"compare":function(a, b){
		return a.milli-b.milli;
	}

};



Duration.MILLI_VALUES = {
	"year":52 * 7 * 24 * 60 * 60 * 1000,		//52weeks
	"quarter":13 * 7 * 24 * 60 * 60 * 1000,	//13weeks
	"month":28 * 24 * 60 * 60 * 1000,		//4weeks
	"week":7 * 24 * 60 * 60 * 1000,
	"day":24 * 60 * 60 * 1000,
	"hour":60 * 60 * 1000,
	"minute":60 * 1000,
	"second":1000,
	"milli":1
};

Duration.MONTH_VALUES = {
	"year":12,
	"quarter":3,
	"month":1,
	"week":0,
	"day":0,
	"hour":0,
	"minute":0,
	"second":0,
	"milli":0
};

//A REAL MONTH IS LARGER THAN THE CANONICAL MONTH
Duration.MONTH_SKEW = Duration.MILLI_VALUES["year"] / 12 - Duration.MILLI_VALUES.month;

////////////////////////////////////////////////////////////////////////////////
// CONVERT SIMPLE <float><type> TO A DURATION OBJECT
////////////////////////////////////////////////////////////////////////////////
Duration.String2Duration = function(text){
	if (text == "" || text=="zero") return new Duration();

	var s = 0;
	while(s < text.length && (text.charAt(s) <= '9' || text.charAt(s) == ".")) s++;

	var output = new Duration();
	var interval = text.rightBut(s);
	var amount = (s == 0 ? 1 : convert.String2Integer(text.left(s)));

	if (Duration.MILLI_VALUES[interval] === undefined)
		Log.error(interval + " is not a recognized duration type (did you use the pural form by mistake?");
	if (Duration.MONTH_VALUES[interval] == 0){
		output.milli = amount * Duration.MILLI_VALUES[interval];
	} else{
		output.milli = amount * Duration.MONTH_VALUES[interval] * Duration.MILLI_VALUES.month;
		output.month = amount * Duration.MONTH_VALUES[interval];
	}//endif


	return output;
};//method


Duration.parse = function(value){
	var output = new Duration();

	//EXPECTING CONCAT OF <sign><integer><type>
	var plist = value.split("+");
	for(var p = 0; p < plist.length; p++){
		var mlist = plist[p].split("-");
		output = output.add(Duration.String2Duration(mlist[0]));
		for(var m = 1; m < mlist.length; m++){
			output = output.subtract(Duration.String2Duration(mlist[m]));
		}//for
	}//for
	return output;
};//method


Duration.max = function(a, b){
	if (a.month > b.month) {
		return a;
	} else if (b.month > a.month) {
		return b;
	} else if (a.milli > b.milli) {
		return a;
	} else {
		return b;
	}//endif
};//method

Duration.min = function(a, b){
	if (a.month < b.month) {
		return a;
	} else if (b.month < a.month) {
		return b;
	} else if (a.milli < b.milli) {
		return a;
	} else {
		return b;
	}//endif
};//method


Duration.newInstance = function(obj){
	if (obj === undefined) return undefined;
	if (obj == null) return null;
	var output = null;
	if (aMath.isNumeric(obj)){
		output = new Duration();
		output.milli = obj;
	} else if (typeof(obj) == "string"){
		return Duration.parse(obj);
	} else if (!(obj.milli === undefined)){
		output = new Duration();
		output.milli = obj.milli;
		output.month = obj.month;
	} else if (isNaN(obj)){
		//return null;
	} else{
		Log.error("Do not know type of object (" + convert.value2json(obj) + ")of to make a Duration");
	}//endif
	return output;
};//method


Duration.prototype.add = function(duration){
	var output = new Duration();
	output.milli = this.milli + duration.milli;
	output.month = this.month + duration.month;
	return output;
};//method

Duration.prototype.addDay = function(numDay){
	return this.add(Duration.DAY.multiply(numDay));
};//method


Duration.prototype.lt = function(val){
	return this.milli < val.milli;
};//method

Duration.prototype.lte = function(val){
	return this.milli <= val.milli;
};//method

Duration.prototype.multiply=function(amount){
	var output=new Duration();
	output.milli=this.milli*amount;
	output.month=this.month*amount;
	return output;
};//method

Duration.prototype.divideBy=function(amount){
	if (amount.month!==undefined && amount.month!=0){
		var m=this.month;
		var r=this.milli;

		//DO NOT CONSIDER TIME OF DAY
		var tod=r%Duration.MILLI_VALUES.day;
		r=r-tod;

		if (m==0 && r>(Duration.MILLI_VALUES.year/3)){
			m=aMath.floor(12 * this.milli / Duration.MILLI_VALUES.year);
			r-=(m/12)*Duration.MILLI_VALUES.year;
		}else{
			r=(r-(this.month*Duration.MILLI_VALUES.month));
			if (r>=Duration.MILLI_VALUES.day*31)
				Log.error("Do not know how to handle");
		}//endif
		r=aMath.min(29/30, (r+tod)/(Duration.MILLI_VALUES.day*30));

		var output=aMath.floor(m/amount.month)+r;
		return output
	}else if (amount.milli===undefined){
		var output=new Duration();
		output.milli=this.milli/amount;
		output.month=this.month/amount;
		return output;
	}else{
		return this.milli/amount.milli;
	}//endif
};//method


Duration.prototype.subtract = function(duration){
	var output = new Duration();
	output.milli = this.milli - duration.milli;
	output.month = this.month - duration.month;
	return output;
};//method



Duration.prototype.floor = function(interval){
	if (interval===undefined || interval.milli === undefined)
		Log.error("Expecting an interval as a Duration object");
	var output = new Duration();

	if (interval.month != 0){
		if (this.month!=0){
			output.month = aMath.floor(this.month/interval.month)*interval.month;
//			var rest=(this.milli - (Duration.MILLI_VALUES.month * output.month));
//			if (rest>Duration.MILLI_VALUES.day*31){	//WE HOPE THIS BIGGER VALUE WILL STILL CATCH POSSIBLE LOGIC PROBLEMS
//				Log.error("This duration has more than a month's worth of millis, can not handle this rounding");
//			}//endif
//			while (rest<0){
//				output.month-=interval.month;
//				rest=(this.milli - (Duration.MILLI_VALUES.month * output.month));
//			}//while
////			if (rest>Duration.MILLI_VALUES.month){ //WHEN FLOORING xmonth-1day, THE rest CAN BE 4week+1day, OR MORE.
			output.milli = output.month * Duration.MILLI_VALUES.month;
			return output;
		}//endif

		//A MONTH OF DURATION IS BIGGER THAN A CANONICAL MONTH
		output.month = aMath.floor(this.milli * 12 / Duration.MILLI_VALUES["year"] / interval.month)*interval.month;
		output.milli = output.month * Duration.MILLI_VALUES.month;
	} else{
		output.milli = aMath.floor(this.milli / (interval.milli)) * (interval.milli);
	}//endif
	return output;
};//method


Duration.prototype.toString = function(){
	if (this.milli == 0) return "zero";


	var output = "";
	var rest = (this.milli - (Duration.MILLI_VALUES.month * this.month)); //DO NOT INCLUDE THE MONTH'S MILLIS
	var isNegative = (rest < 0);
	rest=aMath.abs(rest);

	//MILLI
	var rem = rest % 1000;
	if (rem != 0) output = "+" + rem + "milli" + output;
	rest = aMath.floor(rest / 1000);

	//SECOND
	rem = rest % 60;
	if (rem != 0) output = "+" + rem + "second" + output;
	rest = aMath.floor(rest / 60);

	//MINUTE
	rem = rest % 60;
	if (rem != 0) output = "+" + rem + "minute" + output;
	rest = aMath.floor(rest / 60);

	//HOUR
	rem = rest % 24;
	if (rem != 0) output = "+" + rem + "hour" + output;
	rest = aMath.floor(rest / 24);

	//DAY
	if (rest<11 && rest!=7){
		rem = rest;
		rest = 0;
	}else{
		rem = rest % 7;
		rest = aMath.floor(rest / 7);
	}//endif
	if (rem != 0) output = "+" + rem + "day" + output;

	//WEEK
	if (rest != 0) output = "+" + rest + "week" + output;

	if (isNegative) output = output.replace("+", "-");


	//MONTH AND YEAR
	if (this.month != 0){
		var sign=(this.month<0 ? "-" : "+");
		var month=aMath.abs(this.month);

		if (month <= 18 && month != 12){
			output = sign + month + "month" + output;
		} else{
			var m = month % 12;
			if (m != 0) output = sign + m + "month" + output;
			var y = aMath.floor(month / 12);
			output = sign + y + "year" + output;
		}//endif
	}//endif


	if (output.charAt(0)=="+") output=output.rightBut(1);
	if (output.charAt(0)=='1' && !aMath.isNumeric(output.charAt(1))) output=output.rightBut(1);
	return output;
};//method


Duration.prototype.format=function(interval, rounding){
	return this.round(Duration.newInstance(interval), rounding)+interval;
};//method

Duration.prototype.round=function(interval, rounding){
	if (rounding===undefined) rounding=0;
	var output=this.divideBy(interval);
	output=aMath.round(output, rounding);
	return output;
};//method



Duration.ZERO=Duration.newInstance(0);
Duration.SECOND=Duration.newInstance("second");
Duration.MINUTE=Duration.newInstance("minute");
Duration.HOUR=Duration.newInstance("hour");
Duration.DAY=Duration.newInstance("day");
Duration.WEEK=Duration.newInstance("week");
Duration.MONTH=Duration.newInstance("month");
Duration.QUARTER=Duration.newInstance("quarter");
Duration.YEAR=Duration.newInstance("year");

Duration.COMMON_INTERVALS=[
	Duration.newInstance("second"),
	Duration.newInstance("15second"),
	Duration.newInstance("30second"),
	Duration.newInstance("minute"),
	Duration.newInstance("5minute"),
	Duration.newInstance("15minute"),
	Duration.newInstance("30minute"),
	Duration.newInstance("hour"),
	Duration.newInstance("2hour"),
	Duration.newInstance("3hour"),
	Duration.newInstance("6hour"),
	Duration.newInstance("12hour"),
	Duration.newInstance("day"),
	Duration.newInstance("2day"),
	Duration.newInstance("week"),
	Duration.newInstance("2week"),
	Duration.newInstance("month"),
	Duration.newInstance("2month"),
	Duration.newInstance("quarter"),
	Duration.newInstance("6month"),
	Duration.newInstance("year")
];
