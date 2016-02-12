/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("aHTML.js");
importScript("aUtil.js");
importScript("aDuration.js");


var convert = function(){
};

(function(){
	"use strict";
//
//PULL THE BUGS OUT OF THE ELASTIC SEARCH RESULT OBJECT
//
	convert.ESResult2List = function(esResult){
		var output = [];
		var h = esResult.hits.hits;
		for (var x = 0; x < h.length; x++) {
			if (h[x].fields !== undefined) {
				output.push(h[x].fields);
			} else {
				output.push(h[x]._source);
			}//endif
		}//for
		return output;
	};//method


	convert.ESResult2HTMLSummaries = function(esResult){
		var output = "";

		if (esResult["facets"] === undefined) return output;

		var keys = Object.keys(esResult.facets);
		for (var x = 0; x < keys.length; x++) {
			output += convert.ESResult2HTMLSummary(esResult, keys[x]);
		}//for
		return output;
	};//method


	convert.ESResult2HTMLSummary = function(esResult, name){
		var output = "";
		output += "<h3>" + name + "</h3>";
		var facet = esResult.facets[name];
		if (facet._type == "statistical") {
			output += convert.value2json(facet);
		} else {
			output += convert.List2HTMLTable(facet.terms);
		}//endif


		return output;
	};//method


	convert.json2value = function(json){
		try {
			return JSON.parse(json);
		} catch (e) {
			Log.error("Can not parse json:\n" + json.indent(1), e);
		}//try
	};//method

	/**
	 * @return {string} CSS style
	 */
	convert.Map2Style = function(map){
		return Map.map(map, function(k, v){
					return k + ":" + v;
				}).join(";") + ";";
	};//method

	function value2json(json){
		try {
			if (json instanceof Array) {
				try {
					var singleLine = JSON.stringify(json);
					if (singleLine.length < 60) return singleLine;
				} catch (e) {
					Log.warning("Problem turning array to json:", e);
				}//try

				if (json.length == 0) return "[]";
				if (json.length == 1) return "[" + convert.value2json(json[0]) + "]";

				return "[\n" + json.map(function(v){
							if (v === undefined) return "undefined";
							return convert.value2json(v).indent(1);
						}).join(",\n") + "\n]";
			} else if (typeof(json) == "function") {
				return "undefined";
			} else if (json instanceof Duration) {
				return convert.String2Quote(json.toString());
			} else if (json instanceof Date) {
				return convert.String2Quote(json.format("dd-NNN-yyyy HH:mm:ss"));
			} else if (json instanceof Object) {
				try {
					var singleLine = JSON.stringify(json);
					if (singleLine.length < 60) return singleLine;
				} catch (e) {
					Log.warning("Problem turning object to json:", e);
				}//try

				var keys = Object.keys(json);
				if (keys.length == 0) return "{}";
				if (keys.length == 1) return "{\"" + keys[0] + "\":" + convert.value2json(json[keys[0]]).trim() + "}";

				var output = "{\n\t";
				for (var k in json) {  //NATURAL ORDER
					if (keys.contains(k)) {
						var v = json[k];
						if (v !== undefined) {
							if (output.length > 3) output += ",\n\t";
							output += "\"" + k + "\":" + convert.value2json(v).indent(1).trim();
						}//endif
					}//endif

				}//for
				return output + "\n}";
			} else {
				return JSON.stringify(json);
			}//endif
		} catch(e){
			Log.error("Problem with jsonification", e);
		}//try
	}//function
	convert.value2json = value2json;


	convert.Object2CSS = function(value){
		//FIND DEPTH
		var depth = 1;
		Map.forall(value, function(name, value){
			if (value != null && typeof(value) == "object") {
				depth = 2;
			}//endif
		});

		if (depth == 2) {
			return Map.map(value, function(selector, css){
				return selector + " {" +
					Map.map(css, function(name, value){
						return name + ":" + value;
					}).join(";") + "}";
			}).join("\n\n");
		} else {
			return Map.map(value, function(name, value){
				return name + ":" + value;
			}).join(";")
		}//endif
	};//method

	convert.style2Object = function(value){
		return Map.zip(value.split(";").map(function(attr){
			if (attr.trim() == "") return undefined;
			return attr.split(":").map(function(v){
				return v.trim();
			});
		}));
	};//method

	convert.Object2style = function(style){
		return Map.map(style, function(name, value){
			return name + ":" + value;
		}).join(";");
	};//method


	convert.Object2URL = function(value){
		return $.param(value).replaceAll("%5B%5D=", "=");
	};//method


	convert.URLParam2Object = function(param){
		// CONVERT URL QUERY PARAMETERS INTO DICT
		if (param === undefined || param == null) {
			return {};
		}//endif

		function _decode(v){
			var output = "";
			var i = 0;
			while (i < v.length) {
				var c = v[i];
				if (c == "%") {
					var d = convert.hex2bytes(v.substring(i + 1, i + 3));
					output += d;
					i += 3;
				} else {
					output += c;
					i += 1;
				}//endif
			}//while

			try {
				return convert.json2value(output)
			} catch (e) {
				//DO NOTHING
			}//try
			return output;
		}//function


		var query = {};
		param.split("&").forall(function(p){
			if (!p) return;

			var k, v;
			if (p.indexOf("=") == -1) {
				k = p;
				v = true;
			} else {
				var kv = p.split("=");
				k = kv[0];
				v = kv[1];
				v = _decode(v)
			}//endif

			var u = query[k];
			if (u === undefined || u == null) {
				query[k] = v
			} else if (u instanceof Array) {
				u.append(v)
			} else {
				query[k] = [u, v]
			}//endif
		});

		return query;
	};//function

	convert.int2char = function int2char(i){
		return String.fromCharCode(i);
	};

	function int2hex(value, numDigits){
		return ("0000000" + value.toString(16)).right(numDigits);
	}//function
	convert.int2hex = int2hex;
	convert.number2hex = convert.int2hex;



	(function(){
		var urlMap = {"%": "%25", "{": "%7B", "}": "%7D", "[": "%5B", "]": "%5D"};
		for(var i=0;i<33;i++) urlMap[String.fromCharCode(i)]="%"+int2hex(i, 2);
		for(i=123;i<256;i++) urlMap[String.fromCharCode(i)]="%"+int2hex(i, 2);

		convert.Object2URLParam = function(value){
			return Map.map(value, function(k,v){
				if (v instanceof Array){
					return v.map(function(vv){
						if (isString(vv)){
							return k.escape(urlMap)+"="+vv.escape(urlMap);
						}else{
							return k.escape(urlMap)+"="+value2json(vv).escape(urlMap);
						}//endif
					}).join("&");
				}else if (Map.isObject(v)){
					return k.escape(urlMap)+"="+value2json(v).escape(urlMap);
				}else{
					return k.escape(urlMap)+"="+(""+v).escape(urlMap);
				}//endif
			}).join("&");
		};//function
	})();


	(function(){
		var entityMap = {
			//" ": "&nbsp;",
			"&": "&amp;",
			"<": "&lt;",
			">": "&gt;",
			'"': '&quot;',
			"'": '&#39;',
			"/": '&#x2F;',
			"\n": "<br>",
			"\t": "&nbsp;&nbsp;&nbsp;&nbsp;"
		};


		/**
		 * @return {string} HTML safe string
		 */
		convert.String2HTML = function String2HTML(value){
			if (value == null) return "";
			return ("" + value).translate(entityMap);
		};//method

		var attrMap = {
			"&": "&amp;",
			'"': '&quot;',
			"\n": "",
			"\t": ""
		};

		convert.value2HTMLAttribute = function(value){
			if (value == null) return "";
			if (typeof(value) == "string") return value.translate(attrMap);
			return convert.value2json(value).translate(attrMap);
		};

	})();


	convert.String2HTMLTable = function(value){
		value = "<table><tr>" + value.replaceAll("\n", "</tr><tr>").replaceAll("\t", "</td><td>") + "</tr></table>";
		return value;
	};//method

	/**
	 * @return {string} Javascript quoted for same value
	 */
	convert.String2Quote = function(str){
		return "\"" + (str + '').replaceAll("\n", "\\n").replace(/([\n\t\\"'])/g, "\\$1").replace(/\0/g, "\\0") + "\"";
	};//method

	convert.string2quote = convert.String2Quote;
	convert.value2quote = function(value){
		if (isString(value)) {
			return convert.String2Quote(value);
		} else if (value === undefined || value == null) {
			return "null";
		} else {
			return "" + value;
		}//endif
	};//method


	/**
	 * @return {string} Javascript code for tha same
	 */
	convert.Date2Code = function(date){
		return "Date.newInstance(" + date.getMilli() + ")";
	};//method

	convert.Date2milli = function(date){
		return date.getMilli();
	};//method

	convert.milli2Date = function(milli){
		return new Date(milli);
	};//method


	/**
	 * CONVERT TO SOME MOSTLY HUMAN READABLE FORM (MEANT TO BE DIGESTED BY OTHER TEXT TOOLS)
	 * @return {String}
	 */
	convert.Value2Text = function(value){
		if (value === undefined) {
			return "";
		} else if (value == NaN) {
			return NaN;
		} else if (value == null) {
			return "null";
		} else if (typeof(value) == "string") {
			return convert.String2Quote(value);
		} else if (aMath.isNumeric(value)) {
			if (("" + value).length == 13) {
				//PROBABLY A TIMESTAMP
				value = new Date(value);
				if (value.floorDay().getMilli() == value.getMilli()) {
					return value.format("yyyy/MM/dd");
				} else {
					return value.format("yyyy/MM/dd HH:mm:ss");
				}//endif
			} else {
				return "" + value;
			}
		} else if (value.milli) {
			//DURATION
			return "\"" + value.toString() + "\"";
		} else if (value.getTime) {
			if (value.floorDay().getMilli() == value.getMilli()) {
				return value.format("yyyy/MM/dd");
			} else {
				return value.format("yyyy/MM/dd HH:mm:ss");
			}//endif
		}//endif

		var json = JSON.stringify(value);
		return json;
	};//method


	/**
	 * @return {String}
	 */
	convert.Value2Quote = function(value){
		if (value === undefined) {
			return "undefined";
		} else if (value == NaN) {
			return "NaN";
		} else if (value == null) {
			return "null";
		} else if (typeof(value) == "string") {
			return convert.String2Quote(value);
		} else if (aMath.isNumeric(value)) {
			return "" + value;
		} else if (value.milli) {
			//DURATION
			return "Duration.newInstance(\"" + value.toString() + "\")";
		} else if (value.getTime) {
			return "Date.newInstance(" + value.getMilli() + ")";
		}//endif

		var json = convert.value2json(value);
		return convert.String2Quote(json);
	};//method


	/**
	 * @return {string} return integer value found in string
	 */
	convert.String2Integer = function(value){
		if (value === undefined) return undefined;
		if (value == null || value == "") return null;
		return value - 0;
	};//method


	function unPipe(value){
		var s = value.indexOf("\\", 1);
		if (s < 0) return value.substring(1);

		var result = "";
		var e = 1;
		while (true) {
			var c = value.charAt(s + 1);
			if (c == 'p') {
				result = result + value.substring(e, s) + '|';
				s += 2;
				e = s;
			} else if (c == '\\') {
				result = result + value.substring(e, s) + '\\';
				s += 2;
				e = s;
			} else {
				s++;
			}//endif
			s = value.indexOf("\\", s);
			if (s < 0) break;
		}//while
		return result + value.substring(e);
	}//method


	/**
	 * @return {string}
	 */
	convert.Pipe2Value = function(value){
		var type = value.charAt(0);
		if (type == '0') return null;
		if (type == 'n') return convert.String2Integer(value.substring(1));

		if (type != 's' && type != 'a')
			Log.error("unknown pipe type");

		//EXPECTING MOST STRINGS TO NOT HAVE ESCAPED CHARS
		var output = unPipe(value);
		if (type == 's') return output;

		return output.split("|").map(function(v, i){
			return convert.Pipe2Value(v);
		});
	};//method

	convert.Pipe2Value.pipe = new RegExp("\\\\p", "g");
	convert.Pipe2Value.bs = new RegExp("\\\\\\\\", "g");


	/**
	 * @return {String}
	 */
	convert.Cube2HTMLTable = function(query){

		//WRITE HEADER

		var header = "";
		if (query.name) header += wrapWithHtmlTag("h2", query.name);
		var content = "";

		var e = query.edges[0];

		if (query.edges.length == 0 || (query.edges.length == 1 && query.edges[0].domain.interval == "none")) {
			//NO EDGES, OR ONLY SMOOTH EDGES MEANS ALL POINTS MUST BE LISTED
			return convert.List2HTMLTable(query);
		} else if (query.edges.length == 1) {
			header += "<td>" + convert.String2HTML(e.name) + "</td>";

			if (query.select instanceof Array) {
				header += query.select.map(function(s){
					return wrapWithHtmlTag("td", s.name);
				}).join("");

				content = e.domain.partitions.map(function(v, i){
					return "<tr>" +
						wrapWithHtmlTag("th", e.domain.end(v)) +
						query.select.map(function(s){
							return wrapWithHtmlTag("td", query.cube[i][s.name])
						}).join("") +
						"</tr>";
				}).join("\n");
			} else {
				header += wrapWithHtmlTag("th", query.select.name);

				content = e.domain.partitions.map(function(p, i){
					return "<tr>" + wrapWithHtmlTag("th", e.domain.end(p)) + wrapWithHtmlTag("td", query.cube[i]) + "</tr>";
				}).join("\n");
			}//endif
		} else if (query.edges.length == 2) {
			if (query.select instanceof Array) {
				Log.error("Can not display cube: select clause can not be array, or there can be only one edge");
			} else {

				header += wrapWithHtmlTag("td", query.edges[1].name);	//COLUMN FOR SECOND EDGE
				e.domain.partitions.forall(function(p){
					var name = e.domain.end(p);
					if (name == p && typeof(name) != "string") name = p.name;
					if (p.name !== undefined && p.name != name)
						Log.error("make sure part.name matches the end(part)==" + name + " codomain");
					header += "<td>" + convert.String2HTML(name) + "</td>";
				});

				content = "";
				query.edges[1].domain.partitions.forall(function(p, r){
					var name = query.edges[1].domain.label(p);
					if (name == p && typeof(name) != "string") name = p.name;
//				if (p.name!==undefined && p.name!=name)
//					Log.error("make sure part.name matches the end(part)=="+name+" codomain");
					content += "<tr>" + wrapWithHtmlTag("th", name);
					for (var c = 0; c < query.cube.length; c++) {
						content += wrapWithHtmlTag("td", query.cube[c][r]);
					}//for
					content += "</tr>";
				});

				if (query.edges[1].allowNulls) {
					content += "<tr>" + wrapWithHtmlTag("th", query.edges[1].domain.NULL.name);
					for (var c = 0; c < query.cube.length; c++) {
						content += wrapWithHtmlTag("td", query.cube[c][r]);
					}//for
					content += "</tr>";
				}//endif


				//SHOW FIRST EDGE AS ROWS, SECOND AS COLUMNS
//			header+=wrapWithHtmlTag(e.name);	//COLUMN FOR FIRST EDGE
//			query.edges[1].domain.partitions.forall(function(v, i){
//				header += "<td>" + convert.String2HTML(v.name) + "</td>";
//			});
//
//			content=query.cube.map(function(r, i){
//				return "<tr>"+
//					wrapWithHtmlTag(e.domain.partitions[i].name, "th")+
//					r.map(function(c, j){
//						return wrapWithHtmlTag(c);
//					}).join("")+
//					"</tr>";
//			}).join("");
			}//endif
		} else {
			Log.error("Actual cubes not supported !!")
		}//endif

		return "<table class='table'>" +
			"<thead>" + header + "</thead>\n" +
			"<tbody>" + content + "</tbody>" +
			"</table>"
			;


	};//method


	/**
	 * @return {String}
	 */
	convert.List2HTMLTable = function(data, options){
		if (data.list && data.columns) {
			//CONVERT FROM QUERY TO EXPECTED FORM
			options = data;
			data = data.list;
		} else if (data.meta && data.meta.format == "list" && data.data) {
			//CONVERT FROM ActiveData LIST
			options = {"columns": data.header};
			data = data.data;
		}//endif

		if (data.length == 0) {
			return "<table class='table'><tbody><tr><td>no records to show</td></tr></tbody></table>";
		}//endif

		//WRITE HEADER
		var header = "";
		var columns;
		if (options && options.columns) {
			columns = options.columns;
		} else {
			columns = qb.getColumnsFromList(data);
		}//endif
		columns.forall(function(v, i){
			header += wrapWithHtmlTag("td", v.name);
		});
		header = "<thead><tr><div>" + header + "</div></tr></thead>";

		var output = "";
		var numRows = data.length;
		if (options !== undefined) {
			if (options.limit !== undefined && numRows > options.limit) {
				numRows = options.limit;
			}//endif
		}//endif


		//WRITE DATA
		for (var i = 0; i < data.length; i++) {
			var row = "";
			for (var c = 0; c < columns.length; c++) {
				var value = data[i][columns[c].name];
				row += wrapWithHtmlTag(["td", "div"], value);
			}//for
			output += "<tr>" + row + "</tr>\n";
		}//for

		return "<table class='table'>" +
			"<thead>" + header + "</thead>\n" +
			"<tbody>" + output + "</tbody>" +
			"</table>"
			;
	};//method


	function wrapWithHtmlTag(tagName, value){
		if (tagName === undefined) tagName = "td";
		tagName = Array.newInstance(tagName);
		var prefix = tagName.map(function(t){
			return "<" + t + ">"
		}).join("");
		var suffix = qb.reverse(tagName).map(function(t){
			return "</" + t + ">"
		}).join("");


		if (value === undefined) {
//		return "<"+tagName+">&lt;undefined&gt;</"+tagName+">";
			return prefix + suffix;
		} else if (value == null) {
//		return "<"+tagName+">&lt;null&gt;</"+tagName+">";
			return prefix + suffix;
		} else if (value instanceof HTML) {
			return prefix + value + suffix;
		} else if (typeof(value) == "string") {
			return prefix + convert.String2HTML(value) + suffix;
		} else if (aMath.isNumeric(value) && ("" + value).split(".")[0].length == 10 && ("" + value).startsWith("1") && value > 1200000000) {
			//PROBABLY A UNIX TIMESTAMP
			value = new Date(value * 1000);
			if (value.floorDay().getMilli() == value.getMilli()) {
				return prefix + new Date(value).format("dd-NNN-yyyy") + suffix;
			} else {
				return prefix + new Date(value).format("dd-NNN-yyyy HH:mm:ss") + suffix;
			}//endif
		} else if (aMath.isNumeric(value)) {
			if (("" + value).split(".")[0].length == 13) {
				//PROBABLY A TIMESTAMP
				value = new Date(value);
				if (value.floorDay().getMilli() == value.getMilli()) {
					return prefix + new Date(value).format("dd-NNN-yyyy") + suffix;
				} else {
					return prefix + new Date(value).format("dd-NNN-yyyy HH:mm:ss") + suffix;
				}//endif
			} else {
				return prefix.leftBut(1) + " style='text-align:right;'>" + value + suffix;
			}
		} else if (value.getTime) {
			if (value.floorDay().getMilli() == value.getMilli()) {
				return prefix + new Date(value).format("dd-NNN-yyyy") + suffix;
			} else {
				return prefix + new Date(value).format("dd-NNN-yyyy HH:mm:ss") + suffix;
			}//endif
		} else if (value.milli) {
			//DURATION
			return prefix + value.toString() + suffix;
		}//endif

		var json = convert.value2json(value);
		return prefix + convert.String2HTML(json) + suffix;
	}//function


	convert.List2Tab = function(data){
		/**
		 * CONVERT TO TAB DELIMITED TABLE
		 * @return {string} simple text table
		 */
		var output = "";

		//WRITE HEADER
		var columns = qb.getColumnsFromList(data);
		for (var cc = 0; cc < columns.length; cc++) output += convert.String2Quote(columns[cc].name) + "\t";
		output = output.substring(0, output.length - 1) + "\n";

		//WRITE DATA
		for (var i = 0; i < data.length; i++) {
			for (var c = 0; c < columns.length; c++) {
				output += convert.Value2Text(data[i][columns[c].name]) + "\t";
			}//for
			output = output.substring(0, output.length - 1) + "\n";
		}//for
		output = output.substring(0, output.length - 1);

		return output;
	};//method


///////////////////////////////////////////////////////////////////////////////
// CONVERT TO TAB DELIMITED TABLE
///////////////////////////////////////////////////////////////////////////////
	convert.Table2List = function(table){
		/**
		 * CONVERT TO LIST OF OBJECTS
		 * @return {array}
		 */
		var output = [];

		//MAP LIST OF NAMES TO LIST OF COLUMN OBJCETS
		if (table.columns[0].substr !== undefined) {
			for (var i = 0; i < table.columns.length; i++) {
				table.columns[i] = {"name": table.columns[i]};
			}//for
		}//endif


		for (var d = 0; d < table.rows.length; d++) {
			var item = {};
			var row = table.rows[d];
			for (var c = 0; c < table.columns.length; c++) {
				item[table.columns[c].name] = row[c];
			}//for
			output.push(item);
		}//for
		return output;
	};//method


	convert.List2Table = function(list, columnOrder){
		var columns = qb.getColumnsFromList(list);
		if (columnOrder !== undefined) {
			var newOrder = [];
			OO: for (var o = 0; o < columnOrder.length; o++) {
				for (var c = 0; c < columns.length; c++) {
					if (columns[c].name == columnOrder[o]) {
						newOrder.push(columns[c]);
						continue OO;
					}//endif
				}//for
				Log.error("Can not find column by name of '" + columnOrder[o] + "'");
			}//for

			CC: for (c = 0; c < columns.length; c++) {
				for (o = 0; o < columnOrder.length; o++) {
					if (columns[c].name == columnOrder[o]) continue CC;
				}//for
				newOrder.push(columns[c]);
			}//for

			columns = newOrder;
		}//endif

		var data = [];
		for (var i = 0; i < list.length; i++) {
			var item = list[i];
			var row = [];
			for (c = 0; c < columns.length; c++) {
				row[c] = coalesce(item[columns[c].name], null);
			}//for
			data.push(row);
		}//for
		return {"columns": columns, "data": data};
	};//method



	convert.char2ASCII = function(char){
		return char.charCodeAt(0);
	};


	convert.hex2int = function(value){
		return parseInt(value, 16);
	};//method

	convert.hex2bytes = function(value){
		var output = "";
		for (var i = 0; i < value.length; i += 2) {
			var c = parseInt(value.substring(i, i + 2), 16);
			output += String.fromCharCode(c);
		}//for
		return output;
	};//method


//CONVERT FROM STRING TO SOMETHING THAT CAN BE USED BY $()
	function String2Selector(str){
		return str.replace(/([ ;&,\.\+\*\~':"\!\^#$%@\[\]\(\)\/=>\|])/g, '\\$1');
	}//method

	convert.String2JQuery = String2Selector;
	convert.String2Selector = String2Selector;

	convert.String2Regexp = function(s){
		return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
	};

	/**
	 * @return {Boolean}
	 */
	var TRUE_FILTER = function(row, i, rows){
		return true;
	};

	/**
	 * @return {Boolean}
	 */
	var FALSE_FILTER = function(row, i, rows){
		return false;
	};

	convert.esFilter2function = function(esFilter){
		if (esFilter === undefined || esFilter == null || esFilter == true) return TRUE_FILTER;
		if (esFilter == false) return FALSE_FILTER;

		function select(path, rows){
			//RETURN THE CHILD ROWS FOUND ALONG path
			if (path.length == 0) return rows;

			var output = [];
			rows.forall(function(r){
				var crows = Array.newInstance(Map.get(r, path[0]));
				var cresult = select(path.rightBut(1), crows).map(function(cr){
					return Map.newInstance(path[0], cr);
				});
				output.extend(cresult);
			});
			return output;

		}//function

		var keys = Object.keys(esFilter);
		if (keys.length != 1)
			Log.error("Expecting only one filter aggregate");
		var op = keys[0];
		if (op == "and") {
			var list = esFilter[op];
			if (list.length == 0) return TRUE_FILTER;
			if (list.length == 1) return convert.esFilter2function(list[0]);

			var tests = list.map(convert.esFilter2function);
			return function(row, i, rows){
				for (var t = 0; t < tests.length; t++) {
					if (!tests[t](row, i, rows)) return false;
				}//for
				return true;
			};
		} else if (op == "or") {
			var list = esFilter[op];
			if (list.length == 0) return FALSE_FILTER;
			if (list.length == 1) return convert.esFilter2function(list[0]);

			var tests = list.map(convert.esFilter2function);
			return function(row, i, rows){
				for (var t = 0; t < tests.length; t++) {
					if (tests[t](row, i, rows)) return true;
				}//for
				return false;
			};
		} else if (op == "not") {
			var test = convert.esFilter2function(esFilter[op]);
			return function(row, i, rows){
				return !test(row, i, rows);
			};
		} else if (op == "term") {
			var terms = esFilter[op];
			return function(row, i, rows){
				var variables = Object.keys(terms);
				for (var k = 0; k < variables.length; k++) {
					var variable = variables[k];
					var val = terms[variable];
					var row_val = Map.get(row, variable);
					if (val instanceof Date) {
						if (row_val.getTime() != terms[variable].getTime()) return false;
					} else if (row_val instanceof Array) {
						if (!row_val.contains(val)) return false;
					} else {
						if (row_val != val) return false;
					}//endif
				}//for
				return true;
			};
		} else if (op == "terms") {
			var terms = esFilter[op];
			var variables = Object.keys(terms);
			if (variables.length > 1) {
				Log.error("not allowed");
			}//endif
			var variable = variables[0];
			return function(row){
				var value = row[variable];
				if (value === undefined) {
					return false;
				} else if (value instanceof Array) {
					if (terms[variable].intersect(value).length == 0) return false;
				} else {
					if (!terms[variable].contains(value)) return false;
				}//endif

				return true;
			};
		} else if (op == "exists") {
			//"exists":{"field":"myField"}
			var field = coalesce(esFilter[op].field, esFilter[op]);
			return function(row, i, rows){
				var val = row[field];
				return (val !== undefined && val != null);
			};
		} else if (op == "missing") {
			var field = coalesce(esFilter[op].field, esFilter[op]);
			return function(row, i, rows){
				var val = row[field];
				return (val === undefined || val == null);
			};
		} else if (['gt', 'gte', 'lt', 'lte'].contains(op)) {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var value = pair[variableName];

			return {
				"gt": function(row, i, rows){
					var v = Map.get(row, variableName);
					if (v === undefined) return false;
					return v > value;
				},
				"gte": function(row, i, rows){
					var v = Map.get(row, variableName);
					if (v === undefined) return false;
					return v >= value;
				},
				"lt": function(row, i, rows){
					var v = Map.get(row, variableName);
					if (v === undefined) return false;
					return v < value;
				},
				"lte": function(row, i, rows){
					var v = Map.get(row, variableName);
					if (v === undefined) return false;
					return v <= value;
				}
			}[op];
		} else if (op == "range") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var range = pair[variableName];

			return function(row, i, rows){
				if (range.gte !== undefined) {
					if (range.gte > Map.get(row, variableName)) return false;
				} else if (range.gt !== undefined) {
					if (range.gt >= Map.get(row, variableName)) return false;
				}//endif

				if (range.lte !== undefined) {
					if (range.lte < Map.get(row, variableName)) return false;
				} else if (range.lt !== undefined) {
					if (range.lt <= Map.get(row, variableName)) return false;
				}//endif

				return true;
			};
		} else if (op == "script") {
			Log.error("scripts not supported");
		} else if (op == "prefix") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var prefix = pair[variableName];
			return function(row, i, rows){
				var v = Map.get(row, variableName);
				if (v === undefined) {
					return false;
				} else if (v instanceof Array) {
					for (var vi = 0; vi < v.length; vi++) {
						if (typeof(v[vi]) == "string" && v[vi].startsWith(prefix)) return true;
					}//endif
					return false;
				} else if (typeof(v) == "string") {
					return v.startsWith(prefix);
				} else {
					Log.error("Do not know how to handle")
				}//endif
			};
		} else if (op == "match_all") {
			return TRUE_FILTER;
		} else if (op == "regexp") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var regexp = new RegExp(pair[variableName]);
			return function(row, i, rows){
				if (regexp.test(Map.get(row, variableName))) {
					return true;
				} else {
					return false;
				}//endif
			}
		} else if (op == "contains") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var substr = pair[variableName];
			return function(row, i, rows){
				var v = Map.get(row, variableName);
				if (v === undefined) {
					return false;
				} else if (v instanceof Array) {
					return v.contains(substr);
				} else if (typeof(v) == "string") {
					return v.indexOf(substr) >= 0;
				} else {
					Log.error("Do not know how to handle")
				}//endif
			}
		} else if (op == "nested") {
			//REACH INTO THE NESTED TEMPLATE FOR THE filter
			var path = splitField(esFilter[op].path);
			var deepFilter = convert.esFilter2function(esFilter[op].query.filtered.filter);


			return function(row, i, rows){
				//MAKE A LIST OF CHILD DOCUMENTS TO RUN FILTER ON
				var childRows = select(path, [row]);
				return (childRows.filter(deepFilter).length > 0);
			};
		} else {
			Log.error("'" + op + "' is an unknown operation");
		}//endif

		return "";
	};//method

//CONVERT ES FILTER TO JAVASCRIPT EXPRESSION
	convert.esFilter2Expression = function(esFilter){
		if (esFilter === undefined) return "true";

		var output = "";

		var keys = Object.keys(esFilter);
		if (keys.length != 1)
			Log.error("Expecting only one filter aggregate");
		var op = keys[0];
		if (op == "and") {
			var list = esFilter[op];
			if (list.length == 0) Log.error("Expecting something in 'and' array");
			if (list.length == 1) return convert.esFilter2Expression(list[0]);
			for (var i = 0; i < list.length; i++) {
				if (output != "") output += " &&\n";
				output += "(" + convert.esFilter2Expression(list[i]) + ")";
			}//for
			return output;
		} else if (op == "or") {
			var list = esFilter[op];
			if (list.length == 0) Log.error("Expecting something in 'or' array");
			if (list.length == 1) return convert.esFilter2Expression(list[0]);
			for (var i = 0; i < list.length; i++) {
				if (output != "") output += " ||\n";
				output += "(" + convert.esFilter2Expression(list[i]) + ")";
			}//for
			return output;
		} else if (op == "not") {
			return "!(" + convert.esFilter2Expression(esFilter[op]) + ")";
		} else if (op == "term") {
			return Map.map(esFilter[op], function(variableName, value){
				if (value instanceof Array) {
					Log.error("Do not use term filter with array of values (" + convert.value2json(esFilter) + ")");
				}//endif
				return "Array.newInstance(" + variableName + ").contains(" + convert.Value2Quote(value) + ")";  //ARRAY BASED FOR MULTIVALUED VARIABLES
			}).join(" &&\n");
		} else if (op == "terms") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var valueList = pair[variableName];
			if (valueList.length == 0)
				Log.error("Expecting something in 'terms' array");
			if (valueList.length == 1) return (variableName) + "==" + convert.Value2Quote(valueList[0]);
			output += "[" + valueList.map(convert.String2Quote).join(", ") + "].intersect(Array.newInstance(" + variableName + ")).length > 0";  //ARRAY BASED FOR MULTIVALUED VARIABLES
			return output;
		} else if (op == "exists") {
			var variableName = esFilter[op].field;
			return "(" + variableName + "!==undefined && " + variableName + "!=null)";
		} else if (op == "missing") {
			var variableName = esFilter[op].field;
			return "(" + variableName + "===undefined || " + variableName + "==null)";
		} else if (op == "range") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var range = pair[variableName];
			var lower = "";
			var upper = "";

			if (!(range.gte === undefined)) {
				lower = convert.Value2Quote(range.gte) + "<=" + (variableName);
			} else if (!(range.gt === undefined)) {
				lower = convert.Value2Quote(range.gt) + "<" + (variableName);
			} else if (!(range.from == undefined)) {
				if (range.include_lower == undefined || range.include_lower) {
					lower = convert.Value2Quote(range.from) + "<=" + (variableName);
				} else {
					lower = convert.Value2Quote(range.from) + "<" + (variableName);
				}//endif
			}//endif

			if (!(range.lte === undefined)) {
				upper = convert.Value2Quote(range.lte) + ">=" + (variableName);
			} else if (!(range.lt === undefined)) {
				upper = convert.Value2Quote(range.lt) + ">" + (variableName);
			} else if (!(range.from == undefined)) {
				if (range.include_lower == undefined || range.include_lower) {
					upper = convert.Value2Quote(range.from) + ">=" + (variableName);
				} else {
					upper = convert.Value2Quote(range.from) + ">" + (variableName);
				}//endif
			}//endif

			if (upper == "" || lower == "") {
				return "(" + upper + lower + ")";
			} else {
				return "(" + upper + ") &&\n(" + lower + ")";
			}//endif
		} else if (op == "script") {
			var script = esFilter[op].script;
			return (script);
		} else if (op == "prefix") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			var value = pair[variableName];
			return 'Array.OR(Array.newInstance('+variableName+').map(function(v){\n'+
			'	return typeof(v)=="string" && v.startsWith(' + convert.Value2Quote(value) + ')\n'+
			'}))\n';
		} else if (op == "match_all") {
			return "true"
		} else if (op == "regexp") {
			var pair = esFilter[op];
			var variableName = Object.keys(pair)[0];
			return "(function(){ return new RegExp(" + convert.Value2Quote(pair[variableName]) + ").test(" + variableName + ");})()";
		} else {
			Log.error("'" + op + "' is an unknown operation");
		}//endif

		return "";


	};//method
})();
