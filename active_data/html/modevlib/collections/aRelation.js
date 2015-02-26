/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */



//GOOD FOR MANY-MANY RELATIONS
var Relation=function(){
	this.map={};
};

Relation.prototype.add=function(from, to){
	if (!this.map[from]) this.map[from]={};
	this.map[from][to]=1;
	return this;
};


//RETURN TRUE IF THIS RELATION IS NET-NEW
Relation.prototype.testAndAdd=function(from, to){
	var isNew=true;
	var f=this.map[from];
	if (!f){
		this.map[from]=Map.newInstance(to, 1);
	}else if (f[to]){
		isNew=false;
	}else{
		f[to]=1;
	}//endif
	return isNew;
};

Relation.prototype.addArray=function(from, toArray){
	var f=this.map[from];
	if (!f){
		f={};
		this.map[from]=f;
	}//endif

	for(var i=toArray.length;i--;){
		var c=toArray[i];
		if (c===undefined || c==null || c=="") continue;
		f[c]=1;
	}//for
	return this;
};

//RETURN AN ARRAY OF OBJECTS THAT from MAPS TO
Relation.prototype.get=function(from){
	var o=this.map[from];
	if (!o) return [];
	return Object.keys(o);
};

//RETURN AN ARRAY OF OBJECTS THAT from MAPS TO
Relation.prototype.getMap=function(from){
	return this.map[from];
};

Relation.prototype.forall=function(func){
	var keys=Object.keys(this.map);
	for(var i=0;i<keys.length;i++){
		func(keys[i], Object.keys(this.map[keys[i]]), i);
	}//for
};


