/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


aCompile={};

////////////////////////////////////////////////////////////////////////////////
// EXECUTE SIMPLE CODE IN THE CONTEXT OF AN OBJECT'S VARIABLES
////////////////////////////////////////////////////////////////////////////////
aCompile.method_usingObjects=function(code, contextObjects){
	if (!(contextObjects instanceof Array)) contextObjects=[contextObjects];

	var contextTypes=[];
	contextObjects.forall(function(v, i){
		contextTypes[i]={"columns":Qb.getColumnsFromList([v])};
	});

	return aCompile.method(code, contextTypes);

};

////////////////////////////////////////////////////////////////////////////////
// EXECUTE SIMPLE CODE IN THE CONTEXT OF AN OBJECT'S VARIABLES
// contextTypes ARE TABLES 
////////////////////////////////////////////////////////////////////////////////
aCompile.method=function(code, contextTypes){
	if (!(contextTypes instanceof Array)) contextTypes=[contextTypes];
	var output;

	var contexTypeNames=[];
	contextTypes.forall(function(c, i){
		contexTypeNames.push("__source"+i);
	});

	var f =
		"output=function("+contexTypeNames.join(",")+"){\n";

		for(var s = contextTypes.length; s--;){
			contextTypes[s].columns.forall(function(p){
				//ONLY DEFINE VARS THAT ARE USED
				if (code.indexOf(p.name) != -1){
					f += "var " + p.name + "="+contexTypeNames[s]+"." + p.name + ";\n";
				}//endif
			});
		}//for

		f+=code.trim().rtrim(";")+";\n";

		//FIRST CONTEXT OBJECT IS SPECIAL, AND WILL HAVE IT'S VARS ASSIGNED BACK
		contextTypes[0].columns.forall(function(p){
			if (code.indexOf(p.name) != -1){
				f += contexTypeNames[0] + "." + p.name + "=" + p.name + ";	\n";
			}//endif
		});

		f += "}";
		eval(f);

	return output;

};


////////////////////////////////////////////////////////////////////////////////
// COMPILE AN EXPRESSION, WHICH WILL RETURN A VALUE
////////////////////////////////////////////////////////////////////////////////
aCompile.expression=function(code, contextTypes){
	if (!(contextTypes instanceof Array)) contextTypes=[contextTypes];
	var output;

	var contexTypeNames=[];
	contextTypes.forall(function(c, i){
		contexTypeNames.push("__source"+i);
	});

	var f =
		"output=function("+contexTypeNames.join(",")+"){\n";

		for(var s = contextTypes.length; s--;){
			contextTypes[s].columns.forall(function(p){
				//ONLY DEFINE VARS THAT ARE USED
				if (code.indexOf(p.name) != -1){
					f += "var " + p.name + "="+contexTypeNames[s]+"." + p.name + ";\n";
				}//endif
			});
		}//for

		f+="return "+code.trim().rtrim(";")+";\n}";
		eval(f);

	return output;

};