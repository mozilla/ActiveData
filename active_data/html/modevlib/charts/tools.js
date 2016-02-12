/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("../../lib/d3/d3.js");
importScript("../collections/aArray.js");

(function(){ //BETTER D3

	//SOME ATTRIBUTES THAT CAN BE FUNCTIONS BY SAME NAME
	var proto = d3.selection.prototype;
	["cx", "cy", "x", "y", "width", "height"].forall(function(name){
		proto[name]=function(){
			return proto.attr.apply(this, [name].appendArray(arguments));
		}
	});
	["fill", "visibility"].forall(function(name){
		proto[name]=function(){
			return proto.style.apply(this, [name].appendArray(arguments));
		}
	});

	//TRANSFORMS COULD BE DONE EASIER TOO
	function transform(){
		return (function(self, name, params){
			return self.each(function(d, i){
				var acc = this.getAttribute("transform");
				var value = name+"("+params.map(function(f){
						return isFunction(f) ?  f(d, i) : f;
					}).join(",")+")";
				this.setAttribute("transform", value+" "+coalesce(acc, ""));
			});
		})(this, arguments[0], Array.prototype.slice.call(arguments, 1));
	}

	["translate", "rotate"].forall(function(name){
		proto[name]=function(){
			return transform.apply(this, [name].appendArray(arguments));
		}
	});



})();


