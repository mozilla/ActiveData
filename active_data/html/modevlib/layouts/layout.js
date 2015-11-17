
var layoutAll;


(function(){
	var DELAY_JAVASCRIPT = 200;
	var DEBUG = true;

	var allFormula = [];
	var mapSelf2DynamicFormula = {"v": {}, "h": {}};

	layoutAll = function layout_all(){
		//GRASP ALL LAYOUT FORMULA
		$("[layout]").each(function(){
			var self = $(this);
			var layout = self.attr("layout");
			parseLayout(self, layout);
		});

		//FIND THE STATIC RELATIONS (USING CSS, WHEN POSSIBLE)
		var mapSelf2Parents = {"v": {}, "h": {}};
		var mapSelf2TrueParents = {};
		var mapSelf2StaticFormula = {};
		forall(allFormula, function(e){
			if (!mapSelf2Parents[e.d][e.l.id]) mapSelf2Parents[e.d][e.l.id]=[];
			if (mapSelf2Parents[e.d][e.l.id].indexOf(e.r.id)==-1)	mapSelf2Parents[e.d][e.l.id].push(e.r.id);
			if (!mapSelf2TrueParents[e.l.id]) mapSelf2TrueParents[e.l.id]=[];
			if (mapSelf2TrueParents[e.l.id].indexOf(e.r.id)==-1) mapSelf2TrueParents[e.l.id].push(e.r.id);
		});

		//PICK A SINGLE PARENT FOR EACH ELEMENT
		forAllKey(mapSelf2TrueParents, function(selfID, parents){
			var self = $("#" + selfID);
			var parent = $("#" + parents[0]);
			prep(self, parent);
		});

		forAllKey(mapSelf2Parents, function(dim, rel){
			forAllKey(rel, function(selfID, parents){
				if (parents.length == 1) {
					forall(allFormula, function(e){
						if (e.d != dim || e.l.id != selfID) return;
						if (!mapSelf2StaticFormula[selfID]) mapSelf2StaticFormula[selfID] = {"v": [], "h": []};
						mapSelf2StaticFormula[selfID][dim].push(e);
					});
				} else {
					mapSelf2DynamicFormula[dim][selfID] = map(allFormula, function(e){
						if (e.d != dim || e.l.id != selfID) return;

						if (e.r.id == parents[0]) {
							//WE SHOULD PREFER THE TOP-LEFT TO BE THE STATIC PARENT?
							if (!mapSelf2StaticFormula[selfID]) mapSelf2StaticFormula[selfID] = {"v": [], "h": []};
							mapSelf2StaticFormula[selfID][dim].push(e);
						}//endif

						return e;
					});
				}//endif
			});
		});

		//FIND PURE CSS SOLUTIONS
		//FIND CSS WIDTH/HEIGHT DESIGNATIONS
		forAllKey(mapSelf2StaticFormula, function(selfID, rel){
			var self = $("#" + selfID);
			forAllKey(dimMap, function(dim, mapper){
				var formula = rel[dim];
				if (formula.length == 0) {
					//DEFAULT IS TO CENTER EVERYTHING
					self.css(mapper.left, "50%");
					self.css("transform", translate(-0.5, dim));
					return;
				}//endif

				formula = removeRedundancy(formula);
				if (formula.length == 1) {
					//position ONLY
					formula = formula[0];
				} else if (formula.length == 2) {
					//HEIGHT
					if (formula[0].l.coord == formula[1].l.coord && formula[0].r.coord != formula[1].r.coord) {
						Log.error("Constraint is wrong");
					}//endif

					if (formula[0].l.coord > formula[0].r.coord){
						var temp = formula[0];
						formula[0] = formula[1];
						formula[1] = temp;
					}//endif

					self.css("box-sizing", "border-box");
					self[mapper.width]((100 * (formula[0].r.coord - formula[1].r.coord) / (formula[0].l.coord - formula[1].l.coord)) + "%");

					if (formula[0].l.coord == formula[0].r.coord) {
						formula = formula[0];
					} else if (formula[1].l.coord == formula[1].l.coord) {
						formula = formula[1];
					} else {
						Log.error("Do not know how to handle");
					}//endif
				} else {
					Log.error("too many constraints")
				}//endif

				//POSITION
				if (formula.l.coord == 0.0) {
					self.css(mapper.left, (100 * formula.r.coord) + "%");
				} else if (formula.l.coord != 1.0) {
					self.css(mapper.left, (100 * formula.r.coord) + "%");
					self.css("transform", translate(-formula.l.coord, dim));
				} else if (formula.l.coord == 1.0) {
					self.css(mapper.right, (100 * (1 - formula.r.coord)) + "%");
				} else {
					Log.error("can not handle lhs of " + formula.l.coord);
				}//endif
			});
		});

		//POSSIBLE TOPOLOGICAL ORDERING
		//APPLY ALL FORMULA
		positionDynamic();
	};//function

	function translate(amount, dim){
		if (dim == "h") {
			return "translate(" + (amount * 100) + "%, 0)";
		} else {
			return "translate(0, " + (amount * 100) + "%)";
		}//endif
	}//function


	var dimMap = {
		"v": {
			"left": "top",
			"right": "bottom",
			"width": "height",
			"outerWidth": "outerHeight"
		},
		"h": {
			"left": "left",
			"right": "right",
			"width": "width",
			"outerWidth": "outerWidth"
		}
	};

	function positionDynamic(){
		//COMPILE FUNCTION TO HANDLE RESIZE
		var layoutFunction = "function(){\n";
		forAllKey(dimMap, function(dim, mapper){
			forAllKey(mapSelf2DynamicFormula[dim], function(selfID, formula){
				if (formula.length == 2) {
					var points = map(formula, function(e){
						var parent = $("#" + e.r.id);
						var p = parent.offset();
						var code;
						if (e.r.coord == 0) {
							code = '$("#' + e.r.id + '").offset().' + mapper.left
						} else if (e.r.coord == 1) {
							code = '($("#' + e.r.id + '").offset().' + mapper.left + ' + $("#' + e.r.id + '").' + mapper.outerWidth + '())';
						} else {
							code = '($("#' + e.r.id + '").offset().' + mapper.left + ' + $("#' + e.r.id + '").' + mapper.outerWidth + '()*' + e.r.coord + ')';
						}//endif

						return {
							"l": e.l.coord,
							"rcode": code
						};
					});

					layoutFunction += '$("#' + selfID + '").' + mapper.outerWidth + '(((' + points[0].rcode + '-' + points[1].rcode + ')/' + (points[0].l - points[1].l) + ')+"px");\n';

				}//endif
			});
		});
		layoutFunction += "};";

		if (DEBUG){
			Log.note("dynamicLayout="+layoutFunction)
		}//endif

		var dynamicLayout;
		eval("dynamicLayout=" + layoutFunction);
		dynamicLayout();
		window.onresize = debounce(dynamicLayout, DELAY_JAVASCRIPT);
	}//function


	function prep(self, parent){
		var p = parent.css("position");
		if (p === undefined || p=="static") {
			parent.css({"position": "relative"});
		}//endif

		if (self.parent().attr("id") != parent.attr("id")) {
			parent.append(self);
		}//endif
		self.css({"position": "absolute"})
	}//method


	var canonical = {
		"top": {"v": 0.0},
		"middle": {"v": 0.5},
		"bottom": {"v": 1.0},
		"left": {"h": 0.0},
		"center": {"h": 0.5},
		"right": {"h": 1.0},
		"tr": {"v": 0.0, "h": 1.0},
		"rt": {"v": 0.0, "h": 1.0},
		"tc": {"v": 0.0, "h": 0.5},
		"ct": {"v": 0.0, "h": 0.5},
		"tl": {"v": 0.0, "h": 0.0},
		"lt": {"v": 0.0, "h": 0.0},
		"mr": {"v": 0.5, "h": 1.0},
		"rm": {"v": 0.5, "h": 1.0},
		"mc": {"v": 0.5, "h": 0.5},
		"cm": {"v": 0.5, "h": 0.5},
		"ml": {"v": 0.5, "h": 0.0},
		"lm": {"v": 0.5, "h": 0.0},
		"br": {"v": 1.0, "h": 1.0},
		"rb": {"v": 1.0, "h": 1.0},
		"bc": {"v": 1.0, "h": 0.5},
		"cb": {"v": 1.0, "h": 0.5},
		"bl": {"v": 1.0, "h": 0.0},
		"lb": {"v": 1.0, "h": 0.0}
	};

	var canonical_vertical = {
		"top": 0.0,
		"middle": 0.5,
		"bottom": 1.0,
		"t": 0.0,
		"m": 0.5,
		"b": 1.0
	};

	var canonical_horizontal = {
		"left": 0.0,
		"center": 0.5,
		"right": 1.0,
		"l": 0.0,
		"c": 0.5,
		"r": 1.0
	};

	function parseLayout(self, layout){
		forall(layout.split(";"), function(f){
			parseFormula(self, f);
		});
	}//function

	function parseFormula(self, expr){
		if (expr.trim().length == 0) return;

		var temp = expr.split("=");
		var lhs = temp[0].trim();
		var rhs = temp[1].trim();

		if (lhs.startsWith("..")) {
			Log.error("'..' on left of assignment is not allowed")
		} else if (lhs.startsWith(".")) {
			lhs = lhs.substring(1);
		}//endif

		var selfID = getID(self);
		var coord = canonical[lhs];
		rhs = parseRHS(self, rhs);

		forall(["v", "h"], function(d){
			if (coord[d] !== undefined) {
				allFormula.push({
					"l": {"id": selfID, "coord": coord[d]},
					"r": {"id": rhs.id, "coord": rhs.coord[d]},
					"d": d
				});
			}//endif
		});

	}//function

	function getID(element){
		var output = element.attr('id');
		if (output === undefined) {
			output = "uid_" + UID();
			element.attr('id', output);
		}//endif
		return output;
	}//function

	function parseRHS(self, rhs){
		var parentID;
		var coord;

		if (rhs.startsWith(".")) {
			//EXPECTING List(".") <coordinates>
			var parent = self;
			coord = rhs.substring(1);
			while (coord.startsWith(".")) {
				parent = parent.parent();
				coord = coord.substring(1);
			}//while
			parentID = getID(parent);
		} else {
			//EXPECTING <parentName> "." <coordinates>
			var temp = rhs.split(".");
			parentID = temp[0];

			if (parentID == "page") {
				var body = $("body");
				parentID = body.attr("id");
				if (parentID === undefined) {
					body.attr("id", "page");
					parentID = "page";
				}//endif
			} else if (parentID == "window") {
				var w = $("#window");
				if (w.length == 0) {
					body = $("body");
					body.append('<div id="window" style="position:fixed;top:0;left:0;right:0;bottom:0;"></div>');
					body.css({"position": "relative"});
				}//endif
			}//endif

			coord = temp[1];
		}//endif

		if (coord.indexOf("[") >= 0) {
			temp = coord.split("[");
			if (temp.length == 3) {
				//EXPECTING "[" <h-expression> "]" "[" <v-expression> "]"
				coord = {"v": eval(temp[1].split("]")[0]), "h": eval(temp[2].split("]")[0])};
			} else if (temp[0].length == 0) {
				//EXPECTING "[" <h-expression> "]" <v-position>
				coord = {"v": eval(temp[0].split("]")[0]), "h": canonical_horizontal[temp[1]]};
			} else {
				//EXPECTING <h-position> "[" <v-expression> "]"
				coord = {"v": canonical_vertical[temp[0]], "h": eval(temp[1].split("]")[0])};
			}//endif
		} else {
			//EXPECTING <h-position> <v-position>
			coord = canonical[coord]
		}//endif

		return {"id": parentID, "coord": coord};
	}//function

	function removeRedundancy(formula){
		var output = [];
		for (var i = 0; i < formula.length; i++) {
			var g = formula[i];
			for (var u = 0; u < i; u++) {
				if (g.l.coord == formula[u].l.coord && g.r.coord == formula[u].r.coord) g = undefined;
			}//for
			if (g) output.push(g);
		}//for
		return output;
	}//function


	//RETURN FUNCTION THAT WILL ONLY CALL f AFTER time MS HAS PASSED
	function debounce(f, timeout){
		var pending = undefined;

		function output(){
			var self = this;
			var args = arguments;
			if (pending) clearTimeout(pending);
			var g = function(){
				f.apply(self, args);
				pending = undefined;
			};
			pending = setTimeout(g, timeout);
		}//function
		return output;
	}//function

	function forall(self, func){
		for (var i = 0; i < self.length; i++) {
			func(self[i], i, self);
		}//for
		return self;
	}//function

	function forAllKey(map, func){
		//func MUST ACCEPT key, value PARAMETERS
		var keys=Object.keys(map);
		for(var i=keys.length;i--;){
			var key=keys[i];
			var val=map[key];
			if (val!==undefined) func(key, val);
		}//for
	}//function

	var UID;
	(function(){
		var next=0;

		UID=function(){
			next++;
			return next;
		};//method
	})();

	function map(arr, func){
		var output=[];
		for(var i=0;i<arr.length;i++){
			var v=func(arr[i], i);
			if (v===undefined || v==null) continue;
			output.push(v);
		}//for
		return output;
	}//method

})();
