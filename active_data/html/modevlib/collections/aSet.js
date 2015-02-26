/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("../util/aUtil.js");
importScript("aArray.js");



var aSet=function(data){
	if (this.VERIFY_INSTANCE!=aSet.VERIFY_INSTANCE){
		Log.error("Expecting to be called using 'new'");
	}//endif
	this.map={};
	if (data!==undefined)
		this.addArray(data);
};


(function(){
	aSet.VERIFY_INSTANCE={};
	aSet.prototype.VERIFY_INSTANCE=aSet.VERIFY_INSTANCE;

	aSet.prototype.add=function(v){
		this.map[v]=v;
		return this;
	};

	aSet.prototype.addArray=function(a){
		for(var i=a.length;i--;){
			var v=a[i];
			this.map[v]=v;
		}//for
		return this;
	};

	aSet.prototype.remove=function(v){
		this.map[v]=undefined;
		return this;
	};

	aSet.prototype.getArray=function(){
		return Map.getValues(this.map);
	};

	aSet.prototype.contains=function(v){
		return this.map[v]!==undefined;
	};

	aSet.prototype.map=function(func){
		return this.getArray().map(func);
	};


})();




