/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


if (window['import'+'Script'] === undefined) var importScript=function(){};



var Map={};


////////////////////////////////////////////////////////////////////////////////
// undefined EFFECTIVELY REMOVES THE KEY FROM THE MAP
// null IS A VALID VALUE INDICATING THE VALUE IS UNKNOWN
////////////////////////////////////////////////////////////////////////////////

Map.newInstance=function(key, value){
	var output={};
	output[key]=value;
	return output;
};//method

//LIST OF [k, v] TUPLES EXPECTED
Map.zip=function(keys, values){
	var output={};

	if (values===undefined){
		keys.forall(function(kv){
			//LIST OF [k, v] TUPLES EXPECTED
			output[kv[0]]=kv[1];
		});
	}else{
		keys.forall(function(k, i){
			output[k]=values[i];
		});
	}//endif

	return output;
};//method




Map.copy = function(from, to){
	if (to===undefined) to={};
	var keys = Object.keys(from);
	for(var k = 0; k < keys.length; k++){
		var v=from[keys[k]];
		if (v===undefined) continue;	//DO NOT ADD KEYS WITH NO VALUE
		to[keys[k]] = v;
	}//for
	return to;
};


//IF dest[k]==undefined THEN ASSIGN source[k]
Map.setDefault = function(dest){
	for(var s=1;s<arguments.length;s++){
		var source=arguments[s];
		if (source===undefined) continue;
		var keys = Object.keys(source);
		for(var k = 0; k < keys.length; k++){
			var key = keys[k];
			if (dest[key]===undefined){
				dest[key]=source[key];
			}//endif
		}//for
	}//for
	return dest;
};

Map.jsonCopy=function(value){
	if (value===undefined) return undefined;
	return JSON.parse(JSON.stringify(value));
};

Map.clone = Map.jsonCopy;


//IF map IS NOT 1-1 THAT'S YOUR PROBLEM
Map.inverse=function(map){
	var output={};
	Map.forall(map, function(k, v){output[v]=k;});
	return output;
};//method


//THROW AN ERROR IF WE DO NOT SEE THE GIVEN ATTRIBUTE IN THE LIST
Map.expecting=function(obj, keyList){
	for(i=0;i<keyList.length;i++){
		if (obj[keyList[i]]===undefined) Log.error("expecting object to have '"+keyList[i]+"' attribute");
	}//for
};

// ASSUME THE DOTS (.) IN fieldName ARE SEPARATORS
// AND THE RESULTING LIST IS A PATH INTO THE STRUCTURE
// (ESCAPE "." WITH "\\.", IF REQUIRED)
Map.get=function(obj, fieldName){
	if (obj===undefined || obj==null) return obj;
	var path = splitField(fieldName);
	for (var i=0;i<path.length-1;i++){
		var step = path[i];
		if (step=="length"){
			obj = eval("obj.length");
		}else{
			obj = obj[step];
		}//endif
		if (obj===undefined || obj==null) return undefined;
	}//endif
	return obj[path.last()];
};//method


Map.codomain=function(map){
	var output=[];
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var val=map[keys[i]];
		if (val!==undefined) output.push(val);
	}//for
	return output;
};//method

//RETURN KEYS
Map.domain=function(map){
	var output=[];
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var key=keys[i];
		var val=map[key];
		if (val!==undefined) output.push(key);
	}//for
	return output;
};//method


//RETURN TRUE IF MAPS LOOK IDENTICAL
Map.equals=function(a, b){
	var keys=Object.keys(a);
	for(var i=keys.length;i--;){
		var key=keys[i];
		if (b[key]!=a[key]) return false;
	}//for

	keys=Object.keys(b);
	for(i=keys.length;i--;){
		key=keys[i];
		if (b[key]!=a[key]) return false;
	}//for

	return true;
};//method


var forAllKey=function(map, func){
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var key=keys[i];
		var val=map[key];
		if (val!==undefined) func(key, val);
	}//for
};

Map.forall = forAllKey;
Map.items = forAllKey;

var countAllKey=function(map){
	var count=0;
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var key=keys[i];
		var val=map[key];
		if (val!==undefined) count++;
	}//for
	return count;
};

var mapAllKey=function(map, func){
	var output=[];
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var key=keys[i];
		var val=map[key];
		if (val!==undefined){
			var result=func(key, val);
			if (result!==undefined) output.push(result);
		}//endif
	}//for
	return output;
};


//RETURN LIST OF {"key":key, "value":val} PAIRS
function getItems(map){
	var output=[];
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var key=keys[i];
		var val=map[key];
		if (val!==undefined){
			output.push({"key":key, "value":val});
		}//endif
	}//for
	return output;
}//function
Map.getItems=getItems;


Map.getValues=function getValues(map){
	var output=[];
	var keys=Object.keys(map);
	for(var i=keys.length;i--;){
		var key=keys[i];
		var val=map[key];
		if (val!==undefined){
			output.push(val);
		}//endif
	}//for
	return output;
};

Map.getKeys = Object.keys;


//USE THE MAP FOR REVERSE LOOKUP ON codomain VALUES PROVIDED
//SINCE THE MAP CODOMAIN IS A VALUE, === IS USED FOR COMPARISION
var reverseMap=function(map, codomain){
	var output=[];
	codomain.forall(function(c, i){
		Map.forall(map, function(k, v){
			if (v===c) output.push(k);
		});
	});
	return output;
};



//RETURN FIRST NOT NULL, AND DEFINED VALUE
function coalesce(){
	var args=arguments;
	if (args instanceof Array && args.length == 1) {
		if (arguments[0] == undefined) {
			return null;
		}else{
			args=arguments[0]; //ASSUME IT IS AN ARRAY
		}//endif
	}//endif

	var a;
	for(var i=0;i<args.length;i++){
		a=args[i];
		if (a!==undefined && a!=null) return a;
	}//for
	return null;
}//method

var coalesce=coalesce;


var Util = {};

Util.coalesce = coalesce;

Util.returnNull = function(__row){
	return null;
};//method

(function(){
	var next=0;

	Util.UID=function(){
		next++;
		return next;
	};//method
})();


//POOR IMPLEMENTATION
Util.GUID=function(){
	return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
		var r = aMath.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
		return v.toString(16);
	});
};//method



