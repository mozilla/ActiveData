/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("aUtil.js");
importScript("../collections/aArray.js");


if (Date.now) {
	Date.currentTimestamp = Date.now;
}else{
	Date.currentTimestamp = function currentTimestamp(){
		new Date().getTime();
	}//method
}//endif

Date.now = function(){
	return new Date();
};//method

Date.eod=function(){
	return new Date().ceilingDay();
};//method

Date.today=function(){
	return new Date().floorDay();
};//method



Date.newInstance = function(value){
	if (value === undefined || value == null) return null;
	if (typeof(value)=="string"){
		var newval=Date.tryParse(value);
		if (newval!=null) return newval;
	}//endif
	if (aMath.isNumeric(value)){
		value=value-0;
		if (value>9990000000000) return null;
		return new Date(value)
	}//endif

	return new Date(value);
};//method


Date.min=function(){
	var min=null;
	for(var i=0;i<arguments.length;i++){
		var v=arguments[i];
		if (v === undefined || v == null) continue;
		if (min==null || min>v) min=v;
	}//for
	return min;
};//method


Date.max=function(){
	var max=null;
	for(var i=0;i<arguments.length;i++){
		if (arguments[i]==null) continue;
		if (max==null || max<arguments[i]) max=arguments[i];
	}//for
	return max;
};//method




Date.prototype.getMilli = Date.prototype.getTime;


Date.prototype.between=function(min, max){
	if (min==null) return null;	//NULL MEANS UNKNOWN, SO between() IS UNKNOWN
	if (max==null) return null;

	//UNDEFINED MEANS DO-NOT-CARE
	if (min!=undefined){
		if (min.getMilli) min=min.getMilli();
		if (this.getMilli()<min) return false;
	}//endif
	if (max!=undefined){
		if (max.getMilli) max=max.getMilli();
		if (max<=this.getMilli()) return false;
	}//endif
	return true;
};//method

Date.prototype.add = function(interval){
	if (interval===undefined || interval==null){
		Log.error("expecting an interval to add");
	}//endif

	var i = Duration.newInstance(interval);

	var addMilli = i.milli - (Duration.MILLI_VALUES.month * i.month);
	return this.addMonth(i.month).addMilli(addMilli);
};//method



Date.prototype.subtract=function(time, interval){
	if (typeof(time)=="string") Log.error("expecting to subtract a Duration or Date object, not a string");

	if (interval===undefined || interval.month==0){
		if (time.getMilli){
			//SUBTRACT TIME
			return Duration.newInstance(this.getMilli()-time.getMilli());
		}else{
			//SUBTRACT DURATION
			var residue = time.milli - (Duration.MILLI_VALUES.month * time.month);
			return this.addMonth(-time.month).addMilli(-residue);
		}//endif
	}else{
		if (time.getMilli){
			//SUBTRACT TIME
			return Date.diffMonth(this, time);
		}else{
			//SUBTRACT DURATION
			return this.addMilli(-time.milli);
		}//endif
	}//endif
};//method

//RETURN THE NUMBER OF WEEKDAYS BETWWEN GIVEN TIMES
Date.diffWeekday=function(endTime, startTime){
	var out=0;

	{//TEST
		if (startTime<=endTime){
			for(d=startTime;d.getMilli()<endTime.getMilli();d=d.addDay(1)){
				if (![6,0].contains(d.dow())) out++;
			}//for
		}else{
			for(d=endTime;d.getMilli()<startTime.getMilli();d=d.addDay(1)){
				if (![6,0].contains(d.dow())) out--;
			}//for
		}//endif
	}


	//SHIFT SO SATURDAY IS START OF WEEK
	endTime=endTime.addDay(1);
	startTime=startTime.addDay(1);


	if ([0,1].contains(startTime.dow())){
		startTime=startTime.floorWeek().addDay(2);
	}//endif

	if ([0,1].contains(endTime.dow())){
		endTime=endTime.floorWeek();
	}//endif

	var startWeek=startTime.addWeek(1).floorWeek();
	var endWeek=endTime.addMilli(-1).floorWeek();


	var output=((startWeek.getMilli()-startTime.getMilli())+((endWeek.getMilli()-startWeek.getMilli())/7)*5+(endTime.getMilli()-endWeek.addDay(2).getMilli()))/Duration.DAY.milli;


	if (out!=aMath.sign(output)*aMath.ceil(aMath.abs(output)))
		Log.error("Weekday calculation failed internal test");


	return output;
};//method


Date.diffMonth=function(endTime, startTime){
	//MAKE SURE WE HAVE numMonths THAT IS TOO BIG;
	var numMonths=aMath.floor((endTime.getMilli()-startTime.getMilli()+(Duration.MILLI_VALUES.day*31))/Duration.MILLI_VALUES.year*12);

	var test = startTime.addMonth(numMonths);
	while (test.getMilli()>endTime.getMilli()){
		numMonths--;
		test= startTime.addMonth(numMonths);
	}//while


	////////////////////////////////////////////////////////////////////////////
	// TEST
	var testMonth=0;
	while(startTime.addMonth(testMonth).getMilli()<=endTime.getMilli()){
		testMonth++;
	}//while
	testMonth--;
	if (testMonth!=numMonths)
		Log.error("Error calculating number of months between ("+startTime.format("yy-MM-dd HH:mm:ss")+") and ("+endTime.format("yy-MM-dd HH:mm:ss")+")");
	// DONE TEST
	////////////////////////////////////////////////////////////////////////////

	var output=new Duration();
	output.month=numMonths;
	output.milli=endTime.getMilli()-startTime.addMonth(numMonths).getMilli()+(numMonths*Duration.MILLI_VALUES.month);
//	if (output.milli>=Duration.MILLI_VALUES.day*31)
//		Log.error("problem");
	return output;
};//method



Date.prototype.dow=Date.prototype.getUTCDay;


//CONVERT THIS GMT DATE TO LOCAL DATE
Date.prototype.addTimezone = function(){
	return this.addMinute(-this.getTimezoneOffset());
};

//CONVERT THIS LOCAL DATE TO GMT DATE
Date.prototype.subtractTimezone = function(){
	return this.addMinute(this.getTimezoneOffset());
};


Date.prototype.addMilli = function(value){
	return new Date(this.getMilli() + value);
};//method


Date.prototype.addSecond = function(value){
	var output = new Date(this);
	output.setUTCSeconds(this.getUTCSeconds() + value);
	return output;
};//method

Date.prototype.addMinute = function(value){
	var output = new Date(this);
	output.setUTCMinutes(this.getUTCMinutes() + value);
	return output;
};//method


Date.prototype.addHour = function(value){
	var output = new Date(this);
	output.setUTCHours(this.getUTCHours() + value);
	return output;
};//method


Date.prototype.addDay = function(value){
	if (value===undefined) value=1;
	var output = new Date(this);
	output.setUTCDate(this.getUTCDate() + value);
	return output;
};//method


Date.prototype.addWeekday = function(value){
	var output=this.addDay(1);
	if ([0,1].contains(output.dow())) output=output.floorWeek().addDay(2);

	var weeks=aMath.floor(value/5);
	value=value-(weeks*5);
	output=output.addDay(value);
	if ([0,1].contains(output.dow())) output=output.floorWeek().addDay(2);

	output=output.addWeek(weeks).addDay(-1);
	return output;
};//method



Date.prototype.addWeek = function(value){
	if (value===undefined) value=1;
	var output = new Date(this);
	output.setUTCDate(this.getUTCDate() + (value * 7));
	return output;
};//method

Date.prototype.addMonth = function(value){
	if (value==0) return this;	//WHOA! SETTING MONTH IS CRAZY EXPENSIVE!!
	var output = new Date(this);
	output.setUTCMonth(this.getUTCMonth() + value);
	return output;
};//method

Date.prototype.addYear = function(value){
	var output = new Date(this);
	output.setUTCFullYear(this.getUTCFullYear() + value);
	return output;
};//method


//RETURN A DATE ROUNDED DOWN TO THE CLOSEST FULL INTERVAL
Date.prototype.floor = function(interval, minDate){
	if (minDate===undefined){
		if (interval.month!==undefined && interval.month>0){
			if (interval.month % 12 ==0){
				return this.addMonth(-interval.month+12).floorYear()
			}else if ([1, 2, 3, 4, 6].contains(interval.month)){
				var temp=this.floorYear();
				return temp.add(this.subtract(temp).floor(interval))
			}else{
				Log.error("Can not floor interval '"+interval.toString()+"'")
			}//endif
		}//endif

		var interval_str;
		if (interval.milli!=undefined){
			interval_str=interval.toString();
		}else{
			interval_str=interval
			interval=Duration.newInstance(interval_str)
		}//endif

		if (interval_str.indexOf("year")>=0) return this.floorYear();
		if (interval_str.indexOf("month")>=0) return this.floorMonth();
		if (interval_str.indexOf("week")>=0) return this.floorWeek();
		if (interval_str.indexOf("day")>=0) return this.floorDay();
		if (interval_str.indexOf("hour")>=0) return this.floorHour();
		Log.error("Can not floor interval '" + interval_str + "'");
	}//endif

	return minDate.add(this.subtract(minDate).floor(interval));
};//method


Date.prototype.floorYear = function(){
	var output = new Date(this);
	output.setUTCMonth(0, 1);
	output.setUTCHours(0, 0, 0, 0);
	return output;
};//method


Date.prototype.floorMonth = function(){
	var output = new Date(this);
	output.setUTCDate(1);
	output.setUTCHours(0, 0, 0, 0);
	return output;
};//method


Date.prototype.floorWeek = function(){
	var output = new Date(this);
	output.setUTCDate(this.getUTCDate() - this.getUTCDay());
	output.setUTCHours(0, 0, 0, 0);
	return output;
};//method


Date.prototype.floorDay = function(){
	var output = new Date(this);
	output.setUTCHours(0, 0, 0, 0);
	return output;
};//method


Date.prototype.floorHour = function(){
	var output = new Date(this);
	output.setUTCMinutes(0);
	return output;
};//method




Date.prototype.ceilingDay = function(){
	return this.floorDay().addDay(1);
};//method


Date.prototype.ceilingWeek = function(){
	return this.floorWeek().addWeek(1);
};//method


Date.prototype.ceilingMonth = function(){
	return this.floorMonth().addMonth(1);
};//method

Date.prototype.ceiling = function(interval){
	return this.floor(interval).add(interval);
};//method




// ------------------------------------------------------------------
// These functions use the same 'format' strings as the
// java.text.SimpleDateFormat class, with minor exceptions.
// The format string consists of the following abbreviations:
//
// Field        | Full Form          | Short Form
// -------------+--------------------+-----------------------
// Year         | yyyy (4 digits)    | yy (2 digits), y (2 or 4 digits)
// Month        | MMM (name or abbr.)| MM (2 digits), M (1 or 2 digits)
//              | NNN (abbr.)        |
// Day of Month | dd (2 digits)      | d (1 or 2 digits)
// Day of Week  | EE (name)          | E (abbr)
// Hour (1-12)  | hh (2 digits)      | h (1 or 2 digits)
// Hour (0-23)  | HH (2 digits)      | H (1 or 2 digits)
// Hour (0-11)  | KK (2 digits)      | K (1 or 2 digits)
// Hour (1-24)  | kk (2 digits)      | k (1 or 2 digits)
// Minute       | mm (2 digits)      | m (1 or 2 digits)
// Second       | ss (2 digits)      | s (1 or 2 digits)
// MilliSecond  | fff (3 digits)     |
// MicroSecond  | ffffff (6 digits)  |
// AM/PM        | a                  |
//
// NOTE THE DIFFERENCE BETWEEN MM and mm! Month=MM, not mm!
// Examples:
//  "MMM d, y" matches: January 01, 2000
//                      Dec 1, 1900
//                      Nov 20, 00
//  "M/d/yy"   matches: 01/20/00
//                      9/2/00
//  "MMM dd, yyyy hh:mm:ssa" matches: "January 01, 2000 12:30:45AM"
// ------------------------------------------------------------------

Date.MONTH_NAMES = new Array('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec');
Date.DAY_NAMES = new Array('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat');
Date.LZ = function(x){
	return(x < 0 || x > 9 ? "" : "0") + x
};


// ------------------------------------------------------------------
// formatDate (date_object, format)
// Returns a date in the output format specified.
// The format string uses the same abbreviations as in getDateFromFormat()
// ------------------------------------------------------------------
Date.prototype.format = function(format){
	var y = this.getUTCFullYear() + "";
	var M = this.getUTCMonth() + 1;
	var d = this.getUTCDate();
	var E = this.getUTCDay();
	var H = this.getUTCHours();
	var m = this.getUTCMinutes();
	var s = this.getUTCSeconds();
	var f = this.getUTCMilliseconds();

	var v = {};
	v["y"] = y;
	v["yyyy"] = y;
	v["yy"] = y.substring(2, 4);
	v["M"] = M;
	v["MM"] = Date.LZ(M);
	v["MMM"] = Date.MONTH_NAMES[M - 1];
	v["NNN"] = Date.MONTH_NAMES[M + 11];
	v["d"] = d;
	v["dd"] = Date.LZ(d);
	v["E"] = Date.DAY_NAMES[E + 7];
	v["EE"] = Date.DAY_NAMES[E];
	v["H"] = H;
	v["HH"] = Date.LZ(H);
	v["h"] = ((H + 11) % 12) + 1;
	v["hh"] = Date.LZ(v["h"]);
	v["K"] = H % 12;
	v["KK"] = Date.LZ(v["K"]);
	v["k"] = H + 1;
	v["kk"] = Date.LZ(v["k"]);
	v["a"] = ["AM","PM"][aMath.floor(H / 12)];
	v["m"] = m;
	v["mm"] = Date.LZ(m);
	v["s"] = s;
	v["ss"] = Date.LZ(s);
	v["fff"] = f;
	v["ffffff"] = f*1000;


	var output = "";
	var formatIndex = 0;
	while(formatIndex < format.length){
		var c = format.charAt(formatIndex);
		var token = "";
		while((format.charAt(formatIndex) == c) && (formatIndex < format.length)){
			token += format.charAt(formatIndex++);
		}//while

		if (v[token] != null){
			output = output + v[token];
		} else{
			output = output + token;
		}//endif
	}
	return output;
};


Date.Timezones = {
	"GMT":0,
	"EST":-5,
	"CST":-6,
	"MST":-7,
	"PST":-8
};



Date.getTimezone = function(){
	Log.warning("Date.getTimezone is incomplete!");
	Date.getTimezone=function(){
		var offset = new Date().getTimezoneOffset();

		//CHEAT AND SIMPLY GUESS
		if (offset == 240 || offset == 300) return "EDT";
		if (offset == 420 || offset == 480) return "PDT";
		return "(" + aMath.round(offset / 60) + "GMT)"
	};
	return Date.getTimezone();
};

////////////////////////////////////////////////////////////////////////////////
// WHAT IS THE MOST COMPACT DATE FORMAT TO DISTINGUISH THE RANGE
////////////////////////////////////////////////////////////////////////////////
Date.niceFormat=function(domain){
	if (!["date", "time"].contains(domain.type)) Log.error("Expecting a time domain");

	var minDate=domain.min;
	var maxDate=domain.max;
	var interval=domain.interval;

	var minFormat=0; //SECONDS
	if (interval.milli>=Duration.MILLI_VALUES.minute) minFormat=1;
	if (interval.milli>=Duration.MILLI_VALUES.hour) minFormat=2;
	if (interval.milli>=Duration.MILLI_VALUES.day) minFormat=3;
	if (interval.milli>=Duration.MILLI_VALUES.month) minFormat=4;
	if (interval.month>=Duration.MONTH_VALUES.month) minFormat=4;
	if (interval.month>=Duration.MONTH_VALUES.year) minFormat=5;

	var maxFormat=5;//year
	var span=maxDate.subtract(minDate, interval);
	if (span.month<Duration.MONTH_VALUES.year && span.milli<Duration.MILLI_VALUES.day*365) maxFormat=4; //month
	if (span.month<Duration.MONTH_VALUES.month && span.milli<Duration.MILLI_VALUES.day*31) maxFormat=3; //day
	if (span.milli<Duration.MILLI_VALUES.day) maxFormat=2;
	if (span.milli<Duration.MILLI_VALUES.hour) maxFormat=1;
	if (span.milli<Duration.MILLI_VALUES.minute) maxFormat=0;

	if (maxFormat<=minFormat) maxFormat=minFormat;

	//INDEX BY [minFormat][maxFormat]
	return [
	["ss.000", "mm:ss", "HH:mm:ss", "NNN dd, HH:mm:ss", "NNN dd, HH:mm:ss", "dd-NNN-yyyy HH:mm:ss"],
	[      "", "HH:mm", "HH:mm"   ,   "E dd, HH:mm"   , "NNN dd, HH:mm"   , "dd-NNN-yyyy HH:mm"   ],
	[      "",      "", "HH:mm"   ,   "E dd, HH:mm"   , "NNN dd, HH:mm"   , "dd-NNN-yyyy HH:mm"   ],
	[      "",      "",         "",   "E dd"          , "NNN dd"          , "dd-NNN-yyyy"         ],
	[      "",      "",         "", ""                , "NNN"             ,    "NNN yyyy"         ],
	[      "",      "",         "", ""                , ""                ,        "yyyy"         ]
	][minFormat][maxFormat];
};//method


Date.getBestInterval=function(minDate, maxDate, requestedInterval, numIntervals){
	Map.expecting(numIntervals, ["min", "max"]);

	var dur=maxDate.subtract(minDate);
	if (dur.milli>Duration.MONTH.milli*numIntervals.min){
		dur=maxDate.subtract(minDate, Duration.MONTH);

		var biggest=dur.divideBy(numIntervals.min).month;
		var best=Duration.COMMON_INTERVALS[0];
		for(i=0;i<Duration.COMMON_INTERVALS.length;i++){
			if (biggest>Duration.COMMON_INTERVALS[i].month) best=Duration.COMMON_INTERVALS[i];
		}//for
		return best;
	}else{
		var requested=requestedInterval.milli;
		var smallest=dur.divideBy(numIntervals.max).milli;
		var biggest=dur.divideBy(numIntervals.min).milli;

		if (smallest<=requested && requested<biggest) return requestedInterval;
		if (requested>biggest){
			for(i=Duration.COMMON_INTERVALS.length;i--;){
				if (biggest>Duration.COMMON_INTERVALS[i].milli) return Duration.COMMON_INTERVALS[i];
			}//for
			return Duration.COMMON_INTERVALS[0];
		}else if (requested<smallest){
			for(i=0;i<Duration.COMMON_INTERVALS.length;i++){
				if (smallest<=Duration.COMMON_INTERVALS[i].milli) return Duration.COMMON_INTERVALS[i];
			}//for
			return Duration.COMMON_INTERVALS.last();
		}//endif

	}//endif
};


// ------------------------------------------------------------------
// Utility functions for parsing in getDateFromFormat()
// ------------------------------------------------------------------
function _isInteger(val){
	var digits = "1234567890";
	for(var i = 0; i < val.length; i++){
		if (digits.indexOf(val.charAt(i)) == -1){
			return false;
		}
	}
	return true;
}
function _getInt(str, i, minlength, maxlength){
	for(var x = maxlength; x >= minlength; x--){
		var token = str.substring(i, i + x);
		if (token.length < minlength){
			return null;
		}
		if (_isInteger(token)){
			return token;
		}
	}
	return null;
}


function internalChecks(year, month, date, hh, mm, ss, fff, ampm){
	// Is date valid for month?
	if (month == 2){
		//LEAP YEAR
		if (((year % 4 == 0) && (year % 100 != 0) ) || (year % 400 == 0)){
			if (date > 29) return 0;
		} else{
			if (date > 28) return 0;
		}//endif
	}//endif
	if ((month == 4) || (month == 6) || (month == 9) || (month == 11)){
		if (date > 30) return 0;
	}//endif

	// Correct hours value
	if (hh < 12 && ampm == "PM"){
		hh = hh - 0 + 12;
	} else if (hh > 11 && ampm == "AM"){
		hh -= 12;
	}//endif

	var newDate = new Date(Date.UTC(year, month - 1, date, hh, mm, ss, fff));
	//newDate=newDate.addMinutes(new Date().getTimezoneOffset());
	return newDate;
}//method






// ------------------------------------------------------------------
// getDateFromFormat( date_string , format_string, isPastDate )
//
// This function takes a date string and a format string. It matches
// If the date string matches the format string, it returns the
// getTime() of the date. If it does not match, it returns 0.
// isPastDate ALLOWS DATES WITHOUT YEAR TO GUESS THE RIGHT YEAR
// THE DATE IS EITHER EXPECTED TO BE IN THE PAST (true) WHEN FILLING
// OUT A TIMESHEET (FOR EXAMPLE) OR IN THE FUTURE (false) WHEN
// SETTING AN APPOINTMENT DATE
// ------------------------------------------------------------------
Date.getDateFromFormat=function(val, format, isPastDate){
	val = val + "";
	format = format + "";
	var valueIndex = 0;
	var formatIndex = 0;
	var token = "";
	var token2 = "";
	var x, y;
	var now = new Date();

	//DATE BUILDING VARIABLES
	var year = null;
	var month = now.getMonth() + 1;
	var dayOfMonth = 1;
	var hh = 0;
	var mm = 0;
	var ss = 0;
	var fff = 0;
	var ampm = "";

	while(formatIndex < format.length){
		// Get next token from format string
		token = "";
		var c = format.charAt(formatIndex);
		while((format.charAt(formatIndex) == c) && (formatIndex < format.length)){
			token += format.charAt(formatIndex++);
		}//while

		// Extract contents of value based on format token
		if (token == "yyyy" || token == "yy" || token == "y"){
			if (token == "yyyy"){
				x = 4;
				y = 4;
			}
			if (token == "yy"){
				x = 2;
				y = 2;
			}
			if (token == "y"){
				x = 2;
				y = 4;
			}
			year = _getInt(val, valueIndex, x, y);
			if (year == null) return 0;
			valueIndex += year.length;
			if (year.length == 2){
				if (year > 70){
					year = 1900 + (year - 0);
				} else{
					year = 2000 + (year - 0);
				}
			}

		} else if (token == "MMM" || token == "NNN"){
			month = 0;
			for(var i = 0; i < Date.MONTH_NAMES.length; i++){
				var month_name = Date.MONTH_NAMES[i];
				var prefixLength = 0;
				while(val.charAt(valueIndex + prefixLength).toLowerCase() == month_name.charAt(prefixLength).toLowerCase()){
					prefixLength++;
				}//while
				if (prefixLength >= 3){
					if (token == "MMM" || (token == "NNN" && i > 11)){
						month = i + 1;
						if (month > 12){
							month -= 12;
						}
						valueIndex += prefixLength;
						break;
					}
				}
			}
			if ((month < 1) || (month > 12)){
				return 0;
			}

		} else if (token == "EE" || token == "E"){
			for(var i = 0; i < Date.DAY_NAMES.length; i++){
				var day_name = Date.DAY_NAMES[i];
				if (val.substring(valueIndex, valueIndex + day_name.length).toLowerCase() == day_name.toLowerCase()){
					valueIndex += day_name.length;
					break;
				}
			}

		} else if (token == "MM" || token == "M"){
			month = _getInt(val, valueIndex, token.length, 2);
			if (month == null || (month < 1) || (month > 12)){
				return 0;
			}
			valueIndex += month.length;
		} else if (token == "dd" || token == "d"){
			dayOfMonth = _getInt(val, valueIndex, token.length, 2);
			if (dayOfMonth == null || (dayOfMonth < 1) || (dayOfMonth > 31)){
				return 0;
			}
			valueIndex += dayOfMonth.length;
		} else if (token == "hh" || token == "h"){
			hh = _getInt(val, valueIndex, token.length, 2);
			if (hh == null || (hh < 1) || (hh > 12)){
				return 0;
			}
			valueIndex += hh.length;
		} else if (token == "HH" || token == "H"){
			hh = _getInt(val, valueIndex, token.length, 2);
			if (hh == null || (hh < 0) || (hh > 23)){
				return 0;
			}
			valueIndex += hh.length;
		} else if (token == "KK" || token == "K"){
			hh = _getInt(val, valueIndex, token.length, 2);
			if (hh == null || (hh < 0) || (hh > 11)){
				return 0;
			}
			valueIndex += hh.length;
		} else if (token == "kk" || token == "k"){
			hh = _getInt(val, valueIndex, token.length, 2);
			if (hh == null || (hh < 1) || (hh > 24)){
				return 0;
			}
			valueIndex += hh.length;
			hh--;
		} else if (token == "mm" || token == "m"){
			mm = _getInt(val, valueIndex, token.length, 2);
			if (mm == null || (mm < 0) || (mm > 59)){
				return 0;
			}
			valueIndex += mm.length;
		} else if (token == "ss" || token == "s"){
			ss = _getInt(val, valueIndex, token.length, 2);
			if (ss == null || (ss < 0) || (ss > 59)){
				return 0;
			}
			valueIndex += ss.length;
		} else if (token == "fff"){
			fff = _getInt(val, valueIndex, token.length, 3);
			if (fff == null || (fff < 0) || (fff > 999)){
				return 0;
			}
			valueIndex += fff.length;
		} else if (token == "ffffff"){
			fff = _getInt(val, valueIndex, token.length, 6);
			if (fff == null || (fff < 0) || (fff > 999999)){
				return 0;
			}
			fff/=1000
			valueIndex += fff.length;
		} else if (token == "a"){
			if (val.substring(valueIndex, valueIndex + 2).toLowerCase() == "am"){
				ampm = "AM";
			} else if (val.substring(valueIndex, valueIndex + 2).toLowerCase() == "pm"){
				ampm = "PM";
			} else{
				return 0;
			}
			valueIndex += 2;
		} else if (token.trim() == ""){
			while(val.charCodeAt(valueIndex) <= 32) valueIndex++;
		} else{
			if (val.substring(valueIndex, valueIndex + token.length) != token){
				return 0;
			} else{
				valueIndex += token.length;
			}
		}//endif
	}
	// If there are any trailing characters left in the value, it doesn't match
	if (valueIndex != val.length) return 0;

	if (year == null){
		//WE HAVE TO GUESS THE YEAR
		year = now.getFullYear();
		var oldDate = (now.getTime()) - 86400000;
		var newDate = internalChecks(year, month, dayOfMonth, hh, mm, ss, fff, ampm);
		if (isPastDate){
			if (newDate != 0 && (newDate + "") < (oldDate + "")) return newDate;
			return internalChecks(year - 1, month, dayOfMonth, hh, mm, ss, fff, ampm);
		} else{
			if (newDate != 0 && (newDate + "") > (oldDate + "")) return newDate;
			return internalChecks(year + 1, month, dayOfMonth, hh, mm, ss, fff, ampm);
		}//endif
	}//endif

	return internalChecks(year, month, dayOfMonth, hh, mm, ss, fff, ampm);
};//method

// ------------------------------------------------------------------
// parseDate( date_string [,isPastDate])
//
// This function takes a date string and tries to match it to a
// number of possible date formats to get the value. It will try to
// match against the following international formats, in this order:
// y-M-d   MMM d, y   MMM d,y   y-MMM-d   d-MMM-y  MMM d
// M/d/y   M-d-y      M.d.y     MMM-d     M/d      M-d
// d/M/y   d-M-y      d.M.y     d-MMM     d/M      d-M
// ------------------------------------------------------------------
{
	var generalFormats = ['EE MMM d, yyyy', 'EE MMM d, yyyy @ hh:mm a', 'y M d', 'y - M - d',  'yyyy - MM - dd HH : mm : ss', 'MMM d, y', 'MMM d y', 'MMM d', 'y - MMM - d', 'yyyyMMMd', 'd - MMM - y', 'd MMM y'];
	var monthFirst = ['M / d / y', 'M - d - y', 'M . d . y', 'MMM - d', 'M / d', 'M - d'];
	var dateFirst = ['d / M / y', 'd - M - y', 'd . M . y', 'd - MMM', 'd / M', 'd - M'];

	Date.CheckList = [].appendArray(generalFormats).appendArray(dateFirst).appendArray(monthFirst);
}



Date.tryParse=function(val, isFutureDate){
	val = val.trim();

	var d = null;
	for(var i = 0; i < Date.CheckList.length; i++){
		d = Date.getDateFromFormat(val, Date.CheckList[i], !coalesce(isFutureDate, false));
		if (d != 0){
			var temp=Date.CheckList[i];
			Date.CheckList.splice(i, 1);
			Date.CheckList.prepend(temp);
			return d;
		}//endif
	}//for
	return null;
};//method


Date.EPOCH=Date.newInstance("1/1/1970");



