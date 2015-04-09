/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

String.prototype.trim=function(values){
	if (values===undefined) values=" \t\r\n";
	if (this.length==0) return "";

	for(var s=0;s<this.length;s++) if (values.indexOf(this.charAt(s))<0) break;
	for(var e=this.length;e--;) if (values.indexOf(this.charAt(e))<0) break;
	return this.substring(s, Math.max(s, e+1));
};

String.join = function(list, seperator){
	var output = "";
	for(var i = 0; i < list.length; i++){
		if (output != "") output += seperator;
		output += list[i];
	}//for
	return output;
};

String.trim=function(v){
	return v.trim();
};



//RETURN THE STRING BETWEEN THE start AND end
//IF end IS UNDEFINED, THEN GRABS TO END OF STRING
String.prototype.between=function(start, end){
	var s=this.indexOf(start);
	if (s==-1) return null;
	s+=start.length;
	if (end===undefined) return this.substring(s);

	var e=this.indexOf(end, s);
	if (e==-1) return null;
	return this.substring(s, e);
};


String.prototype.indent=function(numTabs){
	if (numTabs===undefined) numTabs=1;
	var indent="\t\t\t\t\t\t".left(numTabs);
	var str=this.toString();
	var white = str.rightBut(str.rtrim().length); //REMAINING WHITE IS KEPT (CASE OF CR/LF ESPECIALLY)
	return indent+str.rtrim().replaceAll("\n", "\n"+indent) + white;
};


String.prototype.replacePrefix=function(oldPrefix, newPrefix){
	if (this.startsWith(oldPrefix)){
		return newPrefix+this.rightBut(oldPrefix.length);
	}//endif
	return this;
};


//FIND LAST INSTANCE OF find AND REPLACE WITH replace
String.prototype.replaceLast=function(find, replace){
	find = Array.newInstance(find);
	for(var f=0;f<find.length;f++){
		var i=this.lastIndexOf(find[f]);
		if (i==-1) continue;
		return this.left(i)+replace+this.rightBut(i+find[f].length);
	}//for
	return this;
};




String.prototype.rtrim=function(values){
	if (values===undefined) values=" \t\r\n";
	for(var i=this.length;i--;) if (values.indexOf(this.charAt(i))<0) break;
	return this.substring(0, i+1);
};

String.prototype.startsWith=function(value){
	return this.substring(0, value.length)==value;
};


/// REPLACE ALL INSTANCES OF find WITH REPLACE, ONLY ONCE
String.prototype.replaceAll = function(find, replace){
	return this.split(find).join(replace);
};//method



String.prototype.deformat = function(){
	var output=[];
	for(var i=0;i<this.length;i++){
		var c=this.charAt(i);
		if ((c>='a' && c<='z') || (c>='0' && c<='9')){
			output.push(c);
		}else if (c>='A' && c<='Z'){
			output.push(String.fromCharCode(c.charCodeAt(0)+32));
		}//endif
	}//for
	return output.join("");
};//method

String.prototype.left = function(amount){
	return this.substring(0, aMath.min(this.length, amount));
};//method

String.prototype.right = function(amount){
	return this.substring(this.length - amount);
};//method

String.prototype.leftBut = function(amount){
	return this.substring(0, this.length - amount);
};//method

String.prototype.rightBut = function(amount){
	return this.substring(amount, this.length);
};//method

String.prototype.endsWith=function(value){
	return this.substring(this.length - value.length)==value;
};//method

//MAP STRINGS TO OTHER STRINGS, IF NOT DEFINED, DO NOT TOUCH CHARACTERS
String.prototype.escape=function(map){
	var output="";
	var keys=Object.keys(map);
	var s=0;
	while(true){
		var min=this.length;
		var kk;
		for(var k=0;k<keys.length;k++){
			var m=this.indexOf(keys[k], s);
			if (m>=0 && m<min){
				min=m;
				kk=keys[k];
			}//endif
		}//for
		output+=this.substring(s, min);

		if (min==this.length) return output;
		output+=map[kk];
		s=min+kk.length;
	}//while
};//method


String.prototype.ltrim=function(c){
	var e=this.length;
	while(e>0 && this.charAt(e-1)==c) e--;
	return this.substring(0, e);
};//method

//RETURN STRING WITH GIVEN CHARACTERS MAPPED
String.prototype.translate=function translate(mapper){
	var output=[];
	for(var i=0;i<this.length;i++){
		var c= this[i];
		var d = mapper[c];
		if (d===undefined){
			output.append(c);
		}else{
			output.append(d)
		}//endif
	}//for
	return output.join("");
};

function isString(value){
	return (value instanceof String || typeof(value) == "string");
}//method


function joinPath(){
	var output = [];
	output.append(arguments[0].rtrim("/"));
	for (var i = 1; i < arguments.length; i++) {
		var a = arguments[i];
		if (a !== undefined && a != null) {
			output.append(arguments[i].trim("/"))
		}//endif
	}//for
	return output.join("/");
}//method
