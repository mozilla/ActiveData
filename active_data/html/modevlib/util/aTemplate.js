importScript("../collections/aArray.js");
importScript("aUtil.js");
importScript("aString.js");
importScript("convert.js");


var Template = function Template(template){
	this.template = template;
};

(function(){
	Template.prototype.expand = function expand(values){
		if (values === undefined){
			return this.template;
		}//endif
		var map = {};
		if (!(values instanceof Array)) {
			var keys = Object.keys(values);
			keys.forall(function(k){
				map[k.toLowerCase()] = values[k];
			});
		}//endif
		//ADD RELATIVE REFERENCES
		map["."] = values;

		return _expand(this.template, [map]);
	};
	Template.prototype.replace = Template.prototype.expand;

	function toString(value){
		if (isString(value)) return value;
		return convert.value2json(value)
	}//function
	///////////////////////////////////////////////////////////////////////////
	// DEFINE TEMPLATE FUNCTIONS HERE
	///////////////////////////////////////////////////////////////////////////
	var FUNC = {};
	FUNC.html = convert.String2HTML;
	FUNC.style = convert.Object2style;
	FUNC.css = convert.Object2CSS;
	FUNC.attribute = convert.value2HTMLAttribute;
	FUNC.datetime = function(d, f){
		f = coalesce(f, "yyyy-MM-dd HH:mm:ss");
		return d.format(f);
	};
	FUNC.indent = function(value, amount){
		return toString(value).indent(amount);
	};
	FUNC.left = function(value, amount){
		return toString(value).left(amount);
	};


	function _expand(template, namespaces){
		if (template instanceof Array) {
			return _expand_array(template, namespaces);
		} else if (isString(template)) {
			return _expand_text(template, namespaces);
		} else {
			return _expand_loop(template, namespaces);
		}//endif
	}


	function _expand_array(arr, namespaces){
		//AN ARRAY OF TEMPLATES IS SIMPLY CONCATENATED
		return arr.map(function(t){
			return _expand(t, namespaces);
		}).join("");
	}

	function _expand_loop(loop, namespaces){
		Map.expecting(loop, ["from", "template"]);
		if (typeof(loop.from) != "string") {
			Log.error("expecting from clause to be string");
		}//endif

		return namespaces[0][loop.from].map(function(m, i, all){
			var map = Map.copy(namespaces[0]);
			if (m instanceof Object && !(m instanceof Array)) {
				var keys = Object.keys(m);
				keys.forall(function(k){
					map[k.toLowerCase()] = m[k];
				});
			}//endif
			//ADD RELATIVE REFERENCES
			map["."] = m;
			namespaces.forall(function(n, i){
				map[Array(i + 3).join(".")] = n;
			});

			return _expand(loop.template, namespaces.copy().prepend(map));
		}).join(loop.separator === undefined ? "" : loop.separator);
	}

	function _expand_text(template, namespaces){
		//namespaces IS AN ARRAY OBJECTS FOR VARIABLE NAME LOOKUP
		//CASE INSENSITIVE VARIABLE REPLACEMENT

		//COPY VALUES, BUT WITH lowerCase KEYS
		var map = namespaces[0];

		var output = template;
		var s = 0;
		while (true) {
			s = output.indexOf('{{', s);
			if (s < 0) return output;
			var e = output.indexOf('}}', s);
			if (e < 0) return output;
			var path = output.substring(s + 2, e).toLowerCase().split("|");
			var key = path[0];
			var val = Map.get(map, key);
			for (var p = 1; p < path.length; p++) {
				var func = path[p].split("(")[0];
				if (FUNC[func] === undefined) {
					Log.error(func + " is an unknown string function for template expansion")
				}//endif
				if (path[p].split("(").length==1){
					val = FUNC[func](val)
				}else{
					try {
						val = eval("FUNC[func](val, " + path[p].split("(")[1]);
					}catch (f){
						Log.warning("Can not evaluate "+convert.String2Quote(output.substring(s + 2, e)), f)
					}//try
				}//endif
			}//for

			if (val === undefined) {
				val = "undefined"
			} else if (val == null) {
				val = "";  //NULL IS NOTHING
			} else if (typeof(val)=="string"){
				//do nothing
			}else if (val.toString){
				val=val.toString()
			}else{
				val = "" + val;
			}//endif

			if (val !== undefined && (val instanceof String || typeof(val) == "string" || (typeof map[key]) != "object")) {
				output = output.substring(0, s) + val + output.substring(e + 2);
				e = s + val.length;
			} else {
				//Log.debug()
			}//endif
			s = e;
		}//while
	}

})();
