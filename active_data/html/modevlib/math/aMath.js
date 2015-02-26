/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


aMath = {};

(function(){
	aMath.PI = Math.PI;


	aMath.isNumeric = function(n){
		if (n == null) return null;
		return !isNaN(parseFloat(n)) && isFinite(n);
	};

	aMath.isNaN = function(n){
		return typeof(n) == "number" && n != +n;
	};


	if (!aMath.isNaN(NaN)) Log.error();
	if (aMath.isNaN("test")) Log.error();
	if (aMath.isNaN(0)) Log.error();
	if (aMath.isNaN(42)) Log.error();
	if (aMath.isNaN({"hi" : 42})) Log.error();


	//THIS WILL RETURN ZERO IF value IS NOT A NUMBER
	aMath.alpha2zero = function(value){
		return aMath.isNumeric(value) ? value - 0 : 0;
	};

	aMath.sign = function(n){
		if (n == null) return null;
		return n > 0.0 ? 1.0 : (n < 0.0 ? -1.0 : 0.0);
	};

	aMath.abs = function(n){
		if (n == null) return null;
		return Math.abs(n);
	};

	aMath.round = function(value, rounding){
		if (rounding === undefined) return Math.round(value);
		var d;
		if (rounding.digits !== undefined) {
			d = Math.pow(10, rounding.digits - aMath.ceiling(aMath.log10(value)));
		} else {
			d = Math.pow(10, rounding);
		}//endif
		return Math.round(value * d) / d;
	};//method

	function SUM(values){
		var sum = null;
		for (var i = 0; i < values.length; i++) {
			var v = values[i];
			if (v == null) continue;
			if (sum == null)
				sum = v;
			else
				sum += v;
		}//for
		return sum;
	}

	aMath.SUM = SUM;
	aMath.sum = function(){
		return SUM(arguments);
	};//add


	aMath.add = aMath.sum;


	aMath.mean = function(){
		var add = null;
		var count = 0;
		for (var i = 0; i < arguments.length; i++) {
			if (arguments[i] == null) continue;
			if (add == null)
				add = arguments[i] - 0;
			else
				add += arguments[i] - 0;
			count++
		}//for

		if (add == null) return null;
		return add / count;
	};//add

	function MAX(values){
		var max = null;
		for (var i = 0; i < values.length; i++) {
			if (values[i] == null) continue;
			if (max == null || max < values[i]) max = values[i];
		}//for
		return max;
	}

	aMath.MAX = MAX;
	aMath.max = function(){
		return MAX(arguments);
	};//method


	function MIN(values){
		var min = null;
		for (var i = 0; i < values.length; i++) {
			if (values[i] == null) continue;
			if (min == null || min > values[i]) min = values[i];
		}//for
		return min;
	}//method

	aMath.MIN = MIN;
	aMath.min = function(){
		return MIN(arguments);
	};//method

	aMath.log10 = function(v){
		return Math.log(v)/Math.log(10);
	};//method

	//
	aMath.average = function(array){
		var total = 0.0;
		var count = 0;
		for (var i = 0; i < array.length; i++) {
			if (array[i] == null) continue;
			total += array[i];
			count++;
		}//for
		if (count == 0) return null;
		return total / count;
	};//method


	aMath.floor = Math.floor;
	aMath.ceil = Math.ceil;
	aMath.ceiling = Math.ceil;
	aMath.log = Math.log;
	aMath.random = Math.random;

	var niceNumbers = [11, 12, 15, 20, 22, 24, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100];

	aMath.niceCeiling = function(value){
		if (value == 0) return 0;
		if (value < 0) Log.error("negative numbers not supported yet");
		var sig = Math.floor(Math.log10(value)) - 1;
		var d = Math.pow(10, sig);
		value /= d;
		for (var i = 0; i < niceNumbers.length; i++)
			if (niceNumbers[i] >= value)
				return niceNumbers[i] * d;
		throw Log.error("bug");
	};

})();


(function(){
	function Cart(x, y){
		this.x = x;
		this.y = y;
	}

	aMath.Cart = Cart;

	aMath.Cart.prototype.toPolar = function(){
		var r = Math.sqrt(this.x * this.x + this.y * this.y);
		var t = Math.atan2(this.y, this.x);
		return new Polar(r, t);
	};


	function Polar(r, t){
		this.r = r;
		this.t = t;
	}

	aMath.Polar = Polar;

	var D2R = Math.PI / 180;
	var R2D = 1 / D2R;

	aMath.Polar.prototype.toCart = function(){
		var x = this.r * Math.sin(this.t);
		var y = this.r * Math.cos(this.t);

		return new Cart(x, y);
	};

	aMath.Polar.prototype.addRadians = function(rads){
		var t = this.t + rads;
		return new Polar(this.r, t);
	};
	aMath.Polar.prototype.rotate = aMath.Polar.prototype.addRadians;

	aMath.Polar.prototype.addDegrees = function(degs){
		this.t += degs * D2R;
	};


})();
