/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("../collections/aArray.js");
importScript("../util/convert.js");


(function(){
	var Exception=function(description, cause, offset){
		Error.call(this);
		this.message=description;
		this.cause=wrap(cause);
		if (offset!==undefined){
			try{
				throw new Error();
			}catch(e){
				this.stack=parseStack(e.stack).slice(offset + 1);
			}//try
		}//endif
	};
	Exception.prototype = Object.create(Error);

	function pythonExcept2Exception(except){
		var output = new Exception(
			new Template(except.template).expand(except.params),
			Array.newInstance(except.cause).map(pythonExcept2Exception).unwrap()
		);
		output.stack = pythonTrace2Stack(except.trace);
		return output;
	}//function

	function pythonTrace2Stack(stack){
		if (stack===undefined || stack==null) return [];
		var output = stack.map(function(s){
			return {
				"function":s.method,
				"fileName":s.file,
				"lineNumber":s.line,
				"depth":s.depth
			};
		});
		return output;
	}//function


	function wrap(e){
		if (e===undefined || e instanceof Exception) return e;
		if (e instanceof Error){
			var output=new Exception(e.message, e.cause);
			output.fileName= e.fileName;
			output.lineNumber = e.lineNumber;
			output.columnNumber = e.columnNumber;
			output.stack = parseStack(e.stack);
			return output;
		} else if (e.type=="ERROR"){
			return pythonExcept2Exception(e);
		}//endif

		return new Exception(""+e);
	}//function
	Exception.wrap = wrap;






	//window.Exception@file:///C:/Users/klahnakoski/git/MoDevMetrics/html/modevlib/debug/aException.js:14:4
	//build@file:///C:/Users/klahnakoski/git/MoDevMetrics/html/modevlib/threads/thread.js:76:2
	//@file:///C:/Users/klahnakoski/git/MoDevMetrics/html/modevlib/threads/thread.js:442:1
	//window.Exception@file:///C:/Users/klahnakoski/git/MoDevMetrics/html/modevlib/debug/aException.js:14:4
	//@file:///C:/Users/klahnakoski/git/MoDevMetrics/html/modevlib/debug/aException.js:77:2
	//@file:///C:/Users/klahnakoski/git/MoDevMetrics/html/modevlib/debug/aException.js:9:2
	var stackPattern=/(.*)@(.*):(\d+):(\d+)/;

	//IS THIS GOOGLE CHROME?
	if (navigator.userAgent.toLowerCase().indexOf('chrome') > -1){
		//TypeError: Converting circular structure to JSON
		//    at Object.stringify (native)
		//    at Object.Map.jsonCopy (http://localhost:63342/charts/platform/modevlib/util/aUtil.js:83:26)
		//    at http://localhost:63342/charts/platform/modevlib/Dimension.js:58:22
		//    at Array.map (http://localhost:63342/charts/platform/modevlib/collections/aArray.js:85:10)
		//    at Object.Dimension.getDomain (http://localhost:63342/charts/platform/modevlib/Dimension.js:53:33)
		//    at __createChart (http://localhost:63342/charts/platform/release-history.html:167:75)
		//    at next (native)
		//    at Thread_prototype_resume [as resume] (http://localhost:63342/charts/platform/modevlib/threads/thread.js:248:24)
		//    at Object.Thread.resume.retval [as success] (http://localhost:63342/charts/platform/modevlib/threads/thread.js:226:11)
		//    at XMLHttpRequest.request.onreadystatechange (http://localhost:63342/charts/platform/modevlib/rest/Rest.js:93:15)"

		stackPattern=/\s*at (.*) \((.*):(\d+):(\d+)\)/;
	}//endif

	function parseStack(stackString){
		var output = [];
		if (stackString===undefined || stackString==null) return output;
		stackString.split("\n").forEach(function(l){
			var parts=stackPattern.exec(l);
			if (parts==null) return;
			output.append({
				"function":parts[1],
				"fileName":parts[2],
				"lineNumber":parts[3],
				"columnNumber":parts[4]
			});
		});
		return output;
	}//function

	//MAKE A GENERIC ERROR OBJECT DESCRIBING THE ARGUMENTS PASSED
	Exception.error=function(){
		var args = Array.prototype.slice.call(arguments).map(function(v,i){
			if (typeof(v)=="string") return v;
			return convert.value2json(v);
		});
		return new Exception("error called with arguments("+args.join(",\n"+")"), null);
	};

	Exception.prototype.contains=function(type){
		if (this==type) return true;
		if (this.message.indexOf(type)>=0) return true;

		if (this.cause===undefined){
			return false;
		}else if (this.cause instanceof Array){
			for(var i=this.cause.length;i--;){
				if (this.cause[i].contains(type)) return true;
			}//for
			return false;
		}else if (typeof(this.cause)=="function"){
			return this.cause.contains(type);
		}else{
			return this.cause.message.indexOf(type)>=0;
		}//endif
	};


	Exception.prototype.toString=function(){
		var output = this.message + "\n";

		if (this.stack){
			output = output + this.stack.map(function(s){
				try{
					return "File " + s.fileName.split("/").last() + ", line " + s.lineNumber + ", in " + s.function + "\n";
				}catch(e){
					throw "Not expected"
				}//try
			}).join("").indent(1);
		}//endif

		if (this.cause === undefined){
			return output;
		}else if (this.cause instanceof Exception){
			return output +
				"caused by " + this.cause.toString();
		}else if (this.cause.toString === undefined){
			return output +
				"caused by\n"+
					this.cause.indent() + "\n";
		}else{
			return output + "\n" +
				"caused by\n"+
					this.cause.message.toString().indent() + "\n";
		}//endif
	};

	Exception.TIMEOUT=new Exception("TIMEOUT");

	window.Exception=Exception;

})();
