/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

////////////////////////////////////////////////////////////////////////////////
// AGGREGATION
////////////////////////////////////////////////////////////////////////////////

if (Qb===undefined) var Qb = {};


Qb.aggregate = {};
Qb.aggregate.compile = function(select){
	if (select.aggregate === undefined) select.aggregate = "none";

	if (Qb.aggregate[select.aggregate] === undefined){
		Log.error("Do not know aggregate aggregate '" + select.aggregate + "'");
	}//endif

	Qb.aggregate[select.aggregate](select);

	//DEFAULT AGGREGATION USES A STRUCTURE (OR VALUE) THAT CHANGES
	//SOME AGGREGATES DEFER calc() UNTIL LATER
	if (select.aggFunction===undefined){
		select.aggFunction=function(row, result, agg){
			try{
				var v=this.calc(row, result);
				return this.add(agg, v);
			}catch(e){
				this.calc(row, result);
				Log.error("can not calc", e);
			}//try
		};//method
	}//endif


	return select;
};//method




Qb.aggregate.join = function(select){
	if (select.separator === undefined) select.separator = '';

	select.defaultValue = function(){
		return [];
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		total.push(v);
		return total;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);

	select.domain.end=function(total){
		return total.join(select.separator);
	};//method


};

Qb.aggregate.average = function(select){
	select.defaultValue = function(){
		return {total:0.0, count:0.0};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;

		total.total += v;
		total.count++;

		return total;
	};//method

	select.domain = {

		compare:function(a, b){
			a = select.end(a);
			b = select.end(b);

			if (a == null){
				if (b == null) return 0;
				return -1;
			} else if (b == null){
				return 1;
			}//endif

			return ((a < b) ? -1 : ((a > b) ? +1 : 0));
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			if (total.count == 0){
				if (select["default"]!==undefined) return select["default"];
				return null;
			}//endif
			return total.total / total.count;
		}
	};
};
Qb.aggregate.avg=Qb.aggregate.average;
Qb.aggregate.mean=Qb.aggregate.average;


Qb.aggregate.geomean = function(select){
	select.defaultValue = function(){
		return {total:0.0, count:0.0};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;

		total.total += Math.log(v);
		total.count++;

		return total;
	};//method

	select.domain = {

		compare:function(a, b){
			a = select.end(a);
			b = select.end(b);

			if (a == null){
				if (b == null) return 0;
				return -1;
			} else if (b == null){
				return 1;
			}//endif

			return ((a < b) ? -1 : ((a > b) ? +1 : 0));
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			if (total.count == 0){
				if (select["default"]!==undefined) return select["default"];
				return null;
			}//endif
			return Math.exp(total.total / total.count);
		}
	};
};


////////////////////////////////////////////////////////////////////////////////
// THIS VALUE WILL BE SET ONCE AND ONLY ONCE
Qb.aggregate.none = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		if (total == null) return v;
		Log.error("Not expecting to aggregate, only one non-null value allowed per set");
		return null;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);


};


////////////////////////////////////////////////////////////////////////////////
// THE AGGREGATE MAY BE ACCUMULATED MANY TIMES BUT ONLY ONE VALUE IS SET
Qb.aggregate.one = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		if (total == null) return v;
		if (total==v) return total;
		Log.error("Expecting only one value to aggregate");
		return null;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);

	select.domain.end=function(value){
		if (value == null && select["default"]!==undefined) return eval(select["default"]);
		return value;
	};//method
};

////////////////////////////////////////////////////////////////////////////////
// PICK ANY VALUE AS THE AGGREGATE
Qb.aggregate.any = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		if (total == null) return v;
		return null;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);

	select.domain.end=function(value){
		if (value == null && select["default"]!==undefined) return eval(select["default"]);
		return value;
	};//method
};

Qb.aggregate.sum = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		return total + v;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);

	select.domain.end=function(value){
		if (value == null && select["default"]!==undefined) return eval(select["default"]);
		return value;
	};//method



};
Qb.aggregate.add=Qb.aggregate.sum;
Qb.aggregate.X1=Qb.aggregate.sum;


//SUM OF SQUARES
Qb.aggregate.X2 = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		return total + (v*v);
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);

	select.domain.end=function(value){
		if (value == null && select["default"]!==undefined) return eval(select["default"]);
		return value;
	};//method
};


//SUM OF SQUARES
Qb.aggregate.stddev = function(select){
	select.defaultValue = function(){
		return {z0:0.0, z1:0.0, z2:0.0};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;

		total.z0++;
		total.z1+=v;
		total.z2=v*v;

		return total;
	};//method

	select.domain = {

		compare:function(a, b){
			a = select.end(a);
			b = select.end(b);

			if (a == null){
				if (b == null) return 0;
				return -1;
			} else if (b == null){
				return 1;
			}//endif

			return ((a < b) ? -1 : ((a > b) ? +1 : 0));
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			if (total.count == 0){
				if (select["default"]!==undefined) return select["default"];
				return null;
			}//endif
			return (total.z2-(total.z1*total.z1/total.z0))/total.z0;
		}
	};
};





//RETURN ZERO (FOR NO DATA) OR ONE (FOR DATA)
Qb.aggregate.binary = function(select){
	select.defaultValue = function(){
		return 0;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		return 1;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);
};
Qb.aggregate.exists=Qb.aggregate.binary;




Qb.aggregate.count = function(select){
	select.defaultValue = function(){
		return 0;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		return total + 1;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);
};

Qb.aggregate.maximum = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		if (total == null || v > total) return v;
		return total;
	};//method

	select.domain = {};
	Map.copy(Qb.domain.value, select.domain);
	select.domain.end=function(value){
		if (value == null && select["default"]!==undefined) return select["default"];
		return value;
	};//method
};

Qb.aggregate.minimum = function(select){
	select.defaultValue = function(){
		return null;
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		if (total == null || v < total) return v;
		return total;
	};//method

	select.domain=Map.copy(Qb.domain.value, {});

	select.domain.end=function(value){
		if (value == null && select["default"]!==undefined) return select["default"];
		return value;
	};//method

};


Qb.aggregate.percentile = function(select){
	select.defaultValue = function(){
		return {list:[]};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;

		total.list.push(v);

		return total;
	};//method

	select.domain = {
		//HOPEFULLY WE WILL NEVER NEED TO SORT PERCENTILES!!!
		compare:function(a, b){
			Log.error("Please, NO!");

			a = select.end(a);
			b = select.end(b);

			if (a == null){
				if (b == null) return 0;
				return -1;
			} else if (b == null){
				return 1;
			}//endif

			return ((a < b) ? -1 : ((a > b) ? +1 : 0));
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			var l=total.list;
			if (l.length == 0){
				if (select["default"]!==undefined) return select["default"];
				return null;
			}//endif

			//THE Stats CAN ONLY HANDLE NUMBERS, SO WE CONVERT TYPES TO NUMBERS AND BACK AGAIN WHEN DONE
			if (l[0].milli){
				for(i=l.length;i--;) l[i]=l[i].milli;
				var output=Stats.percentile(l, select.percentile);
				return Duration.newInstance(output);
			}else if (total.list[0].getMilli){
				for(i=l.length;i--;) l[i]=l[i].getMilli();
				var output=Stats.percentile(l, select.percentile);
				return Date.newInstance(output);
			}else{
				return Stats.percentile(l, select.percentile);
			}//endif
		}
	};
};

Qb.aggregate.median = function(select){
	select.defaultValue = function(){
		return {list:[]};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;

		total.list.push(v);

		return total;
	};//method

	select.domain = {
		//HOPEFULLY WE WILL NEVER NEED TO SORT PERCENTILES!!!
		compare:function(a, b){
			Log.error("Please, NO!");

			a = select.end(a);
			b = select.end(b);

			if (a == null){
				if (b == null) return 0;
				return -1;
			} else if (b == null){
				return 1;
			}//endif

			return ((a < b) ? -1 : ((a > b) ? +1 : 0));
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			var l=total.list;
			if (l.length == 0){
				if (select["default"]!==undefined) return select["default"];
				return null;
			}//endif

			//THE Stats CAN ONLY HANDLE NUMBERS, SO WE CONVERT TYPES TO NUMBERS AND BACK AGAIN WHEN DONE
			if (l[0].milli){
				for(i=l.length;i--;) l[i]=l[i].milli;
				var output=Stats.median(l);
				return Duration.newInstance(output);
			}else if (total.list[0].getMilli){
				for(i=l.length;i--;) l[i]=l[i].getMilli();
				var output=Stats.median(l);
				return Date.newInstance(output);
			}else{
				return Stats.median(l);
			}//endif
		}
	};
};



Qb.aggregate.middle = function(select){
	select.defaultValue = function(){
		return {list:[]};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;

		total.list.push(v);

		return total;
	};//method

	select.domain = {
		//HOPEFULLY WE WILL NEVER NEED TO SORT PERCENTILES!!!
		compare:function(a, b){
			Log.error("Please, NO!");

			a = select.end(a);
			b = select.end(b);

			if (a == null){
				if (b == null) return 0;
				return -1;
			} else if (b == null){
				return 1;
			}//endif

			return ((a < b) ? -1 : ((a > b) ? +1 : 0));
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			var l=total.list;
			if (l.length == 0){
				if (select["default"]!==undefined) return select["default"];
				return null;
			}//endif

			//THE Stats CAN ONLY HANDLE NUMBERS, SO WE CONVERT TYPES TO NUMBERS AND BACK AGAIN WHEN DONE
			if (l[0].milli){
				for(i=l.length;i--;) l[i]=l[i].milli;
				var output=Stats.middle(l, select.percentile);
				return {"min":Duration.newInstance(output.min), "max":Duration.newInstance(output.max)};
			}else if (total.list[0].getMilli){
				for(i=l.length;i--;) l[i]=l[i].getMilli();
				var output=Stats.middle(l, select.percentile);
				return {"min":Date.newInstance(output.min), "max":Date.newInstance(output.max)};
			}else{
				return Stats.middle(l, select.percentile);
			}//endif
		}
	};
};


Qb.aggregate.array = function(select){
	select.defaultValue = function(){
		return {list:[]};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		total.list.push(v);
		return total;
	};//method

	select.domain = {
		//HOPEFULLY WE WILL NEVER NEED TO SORT ARRAY OBJECTS
		compare:function(a, b){
			Log.error("Please, NO!");
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			if (select.sort==1){
				total.list.sort(function(a, b){return a-b;});
			}else if (select.sort==-1){
				total.list.sort(function(a, b){return b-a;});
			}//endif
			return total.list;
		}
	};
};

Qb.aggregate.union = function(select){
	select.defaultValue = function(){
		return {map:{}};
	};//method

	select.add = function(total, v){
		if (v === undefined || v == null) return total;
		total.map[v]=v;
		return total;
	};//method

	select.domain = {
		//HOPEFULLY WE WILL NEVER NEED TO SORT ARRAY OBJECTS
		compare:function(a, b){
			Log.error("Please, NO!");
		},

		NULL:null,

		getCanonicalPart:function(value){
			return value;
		},

		getKey:function(partition){
			return partition;
		},

		end :function(total){
			return Map.getValues(total.map);
		}
	};
};


