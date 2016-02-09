importScript("../collections/aArray.js");
importScript("aUtil.js");
importScript("aString.js");
importScript("aDate.js");
importScript("convert.js");


var Template = function Template(template){
	if (template instanceof Template){
		this.template = template.template;
	}else{
		this.template = template;
	}//endif
};

(function(){

	Template.prototype.expand = function expand(values){
		if (values === undefined){
			return this.template;
		}//endif

		var map = values;
		if (typeof(values)=="object" && !(values instanceof Array) && !(values instanceof Date)) {
			var newMap = {};
			Map.forall(values, function(k, v){
				newMap[k.toLowerCase()]=v;
			});
			map = newMap;
		}//endif

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
		return Date.newInstance(d).format(f);
	};
	FUNC.indent = function(value, amount){
		return toString(value).indent(amount);
	};
	FUNC.left = function(value, amount){
		return toString(value).left(amount);
	};
	FUNC.deformat = function(value){
		return toString(value).deformat();
	};
	FUNC.json = function(value){
		return convert.value2json(value);
	};
	FUNC.comma = function(value){
		//SNAGGED FROM http://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
		var parts = value.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        return parts.join(".");
	};
	FUNC.quote = function(value){
		return convert.value2quote(value);
	};
	FUNC.format = function(value, format){
		if (value instanceof Duration){
			return value.format(format);
		}
		return Date.newInstance(value).format(format);
	};
	FUNC.round = function(value, digits){
		return aMath.round(value, {"digits":digits});
	};
	FUNC.metric = aMath.roundMetric;
	FUNC.upper = function(value){
		if (isString(value)){
			return value.toUpperCase();
		}else{
			return convert.value2json();
		}
	};

	function _expand(template, namespaces){
		if (template instanceof Array) {
			return _expand_array(template, namespaces);
		} else if (isString(template)) {
			return _expand_text(template, namespaces);
		} else if (template.from_items) {
			return _expand_items(template, namespaces);
		} else if (template.from) {
			return _expand_loop(template, namespaces);
		}else{
			Log.error("Not recognized {{template}}", {"template": template})
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

		return Map.get(namespaces[0], loop.from).map(function(m){
			var map = Map.copy(namespaces[0]);
			map["."] = m;
			if (m instanceof Object && !(m instanceof Array)) {
				Map.forall(m, function(k, v){
					map[k.toLowerCase()] = v;
				});
			}//endif
			namespaces.forall(function(n, i){
				map[Array(i + 3).join(".")] = n;
			});

			return _expand(loop.template, namespaces.copy().prepend(map));
		}).join(loop.separator === undefined ? "" : loop.separator);
	}

	/*
	LOOP THROUGH THEN key:value PAIRS OF THE OBJECT
	 */
	function _expand_items(loop, namespaces){
		Map.expecting(loop, ["from_items", "template"]);
		if (typeof(loop.from_items) != "string") {
			Log.error("expecting `from_items` clause to be string");
		}//endif

		return Map.map(Map.get(namespaces[0], loop.from_items), function(name, value){
			var map = Map.copy(namespaces[0]);
			map["name"] = name;
			map["value"] = value;
			if (value instanceof Object && !(value instanceof Array)) {
				Map.forall(value, function(k, v){
					map[k.toLowerCase()] = v;
				});
			}//endif
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
			var path = output.substring(s + 2, e).split("|");
			var key = path[0].toLowerCase();
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
